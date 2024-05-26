use anyhow::anyhow;
use anyhow::Result;

use crate::block_cache::*;
use crate::btree::insert::*;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

enum RemoveResult<V> {
    NotFound,

    // We still return the empty metadata block in case this is the root
    RemoveChild(MetadataBlock, V),
    ReplaceChild(MetadataBlock, V),
}

// Returns Some((new_root, old_value)) if key is present, otherwise None.
fn remove_<V: Serializable>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
) -> Result<RemoveResult<V>> {
    use RemoveResult::*;

    if alloc.is_internal(loc)? {
        let mut node = alloc.shadow::<MetadataBlock>(loc)?;
        let mut idx = node.keys.bsearch(&key);
        if idx < 0 {
            return Ok(NotFound);
        }

        if idx as u32 == node.nr_entries.get() {
            idx -= 1;
        }

        let idx = idx as usize;

        let child = node.values.get(idx);
        match remove_::<V>(alloc, child, key)? {
            NotFound => Ok(NotFound),
            RemoveChild(_, v) => {
                node.remove_at(idx);
                if node.is_empty() {
                    Ok(RemoveChild(node.loc, v))
                } else {
                    Ok(ReplaceChild(node.loc, v))
                }
            }
            ReplaceChild(new, v) => {
                node.values.set(idx, &new);
                Ok(ReplaceChild(node.loc, v))
            }
        }
    } else {
        let mut node = alloc.shadow::<V>(loc)?;
        let idx = node.keys.bsearch(&key);
        if idx < 0 || idx as u32 > node.nr_entries.get() {
            Ok(NotFound)
        } else {
            let idx = idx as usize;
            let v = node.values.get(idx);
            node.remove_at(idx);
            if node.is_empty() {
                Ok(RemoveChild(node.loc, v))
            } else {
                Ok(ReplaceChild(node.loc, v))
            }
        }
    }
}

pub fn remove<V: Serializable>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
) -> Result<Option<(MetadataBlock, V)>> {
    use RemoveResult::*;

    match remove_::<V>(alloc, root, key)? {
        NotFound => Ok(None),
        RemoveChild(new_root, v) => Ok(Some((new_root, v))),
        ReplaceChild(new_root, v) => Ok(Some((new_root, v))),
    }
}

//-------------------------------------------------------------------------

pub struct NodeInfo {
    key_min: Option<u32>,
    loc: MetadataBlock,
}

impl NodeInfo {
    fn new<NV: Serializable>(node: &WNode<NV>) -> Self {
        let key_min = node.keys.first();
        let loc = node.loc;
        NodeInfo { key_min, loc }
    }
}

// Removing a range can turn one entry into two if the range covers the
// middle of an existing entry.  So, like for insert, we have a way of
// returning more than one new block.  If a pair is returned then the
// first one corresponds to the idx of the original block.
pub enum RecurseResult {
    Single(NodeInfo),
    Pair(NodeInfo, NodeInfo),
}

impl RecurseResult {
    pub fn single<NV: Serializable>(node: &WNode<NV>) -> Self {
        RecurseResult::Single(NodeInfo::new(node))
    }

    pub fn pair<NV: Serializable>(n1: &WNode<NV>, n2: &WNode<NV>) -> Self {
        RecurseResult::Pair(NodeInfo::new(n1), NodeInfo::new(n2))
    }
}

// FIXME: common code with insert
pub fn ensure_space<NV: Serializable, M: FnOnce(&mut WNode<NV>, usize)>(
    alloc: &mut NodeAlloc,
    left: &mut WNode<NV>,
    idx: usize,
    mutator: M,
) -> Result<RecurseResult> {
    if left.is_full() {
        let right_block = alloc.new_block()?;
        let mut right = init_node(right_block.clone(), left.is_leaf())?;
        redistribute2(left, &mut right);

        if idx < left.nr_entries() {
            mutator(left, idx);
        } else {
            mutator(&mut right, idx - left.nr_entries());
        }

        Ok(RecurseResult::pair(left, &right))
    } else {
        mutator(left, idx);
        Ok(RecurseResult::single(left))
    }
}
// Call this when recursing back up the spine
fn node_insert_result(
    alloc: &mut NodeAlloc,
    node: &mut WNode<MetadataBlock>,
    idx: usize,
    res: &RecurseResult,
) -> Result<RecurseResult> {
    use RecurseResult::*;

    match res {
        Single(NodeInfo { key_min: None, .. }) => {
            node.keys.remove_at(idx);
            node.values.remove_at(idx);
            Ok(RecurseResult::single(node))
        }
        Single(NodeInfo {
            key_min: Some(new_key),
            loc,
        }) => {
            node.keys.set(idx, new_key);
            node.values.set(idx, loc);
            Ok(RecurseResult::single(node))
        }
        Pair(left, right) => {
            node.keys.set(idx, &left.key_min.unwrap());
            node.values.set(idx, &left.loc);

            ensure_space(alloc, node, idx, |node, idx| {
                node.insert_at(idx + 1, right.key_min.unwrap(), &right.loc)
            })
        }
    }
}

//-------------------------------------------------------------------------

pub type ValFn<'a, V> = Box<dyn Fn(u32, V) -> Option<(u32, V)> + 'a>;

pub fn mk_val_fn<'a, V, F>(f: F) -> ValFn<'a, V>
where
    V: Serializable,
    F: Fn(u32, V) -> Option<(u32, V)> + 'a,
{
    Box::new(f)
}

enum SplitOp {
    Noop,
    SplitAndShift(usize),
    Shift(usize),
}

// This works for both lt_ and geq_, the direction of 'shift' just changes.
fn split_op<V: Serializable>(node: &WNode<V>, key: u32) -> SplitOp {
    use SplitOp::*;

    match node.keys.bsearch(&key) {
        idx if idx < 0 => Noop,
        idx if node.keys.get(idx as usize) == key => Shift(idx as usize),
        idx => SplitAndShift(idx as usize),
    }
}

fn remove_lt_internal<V>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<RecurseResult>
where
    V: Serializable,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<MetadataBlock>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            let res = remove_lt_recurse(alloc, node.values.get(idx), key, split_fn)?;
            node_insert_result(alloc, &mut node, idx, &res)?;

            // remove_lt cannot cause a Pair result, so shift here is safe.
            node.shift_left_no_return(idx);
        }
        Shift(idx) => {
            node.shift_left_no_return(idx);
        }
    }

    Ok(RecurseResult::single(&node))
}

fn remove_lt_leaf<V>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<RecurseResult>
where
    V: Serializable,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<V>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            match split_fn(node.keys.get(idx), node.values.get(idx)) {
                None => {
                    node.keys.remove_at(idx);
                    node.values.remove_at(idx);
                }
                Some((new_key, new_value)) => {
                    node.keys.set(idx, &new_key);
                    node.values.set(idx, &new_value);
                }
            }
            node.shift_left_no_return(idx);
        }
        Shift(idx) => {
            node.shift_left_no_return(idx);
        }
    }

    Ok(RecurseResult::single(&node))
}

pub fn remove_lt_recurse<LeafV>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<LeafV>,
) -> Result<RecurseResult>
where
    LeafV: Serializable,
{
    if alloc.is_internal(loc)? {
        remove_lt_internal(alloc, loc, key, split_fn)
    } else {
        remove_lt_leaf(alloc, loc, key, split_fn)
    }
}

pub fn remove_lt<LeafV>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    split_fn: &ValFn<LeafV>,
) -> Result<MetadataBlock>
where
    LeafV: Serializable,
{
    match remove_lt_recurse(alloc, root, key, split_fn)? {
        RecurseResult::Single(NodeInfo { loc, .. }) => Ok(loc),
        RecurseResult::Pair(_, _) => Err(anyhow!("remove_lt increase nr entries somehow")),
    }
}

//-------------------------------------------------------------------------

fn remove_geq_internal<V>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<RecurseResult>
where
    V: Serializable,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<MetadataBlock>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            let res = remove_geq_recurse(alloc, node.values.get(idx), key, split_fn)?;
            node_insert_result(alloc, &mut node, idx, &res)?;

            // remove_geq() cannot cause a Pair result, so remove_from here is safe.
            node.remove_from(idx + 1);
        }
        Shift(idx) => {
            node.remove_from(idx);
        }
    }

    Ok(RecurseResult::single(&node))
}

fn remove_geq_leaf<V>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<RecurseResult>
where
    V: Serializable,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<V>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            match split_fn(node.keys.get(idx), node.values.get(idx)) {
                None => {
                    node.keys.remove_at(idx);
                    node.values.remove_at(idx);
                }
                Some((new_key, new_value)) => {
                    node.keys.set(idx, &new_key);
                    node.values.set(idx, &new_value);
                }
            }
            node.remove_from(idx + 1);
        }
        Shift(idx) => {
            node.remove_from(idx);
        }
    }

    Ok(RecurseResult::single(&node))
}
fn remove_geq_recurse<LeafV>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<LeafV>,
) -> Result<RecurseResult>
where
    LeafV: Serializable,
{
    if alloc.is_internal(loc)? {
        remove_geq_internal(alloc, loc, key, split_fn)
    } else {
        remove_geq_leaf(alloc, loc, key, split_fn)
    }
}

pub fn remove_geq<LeafV>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    split_fn: &ValFn<LeafV>,
) -> Result<MetadataBlock>
where
    LeafV: Serializable,
{
    match remove_geq_recurse(alloc, root, key, split_fn)? {
        RecurseResult::Single(NodeInfo { loc, .. }) => Ok(loc),
        RecurseResult::Pair(_, _) => Err(anyhow!("remove_geq increased nr of entries")),
    }
}

//-------------------------------------------------------------------------

// All usizes are indexes
// FIXME: Trim ops should hold the key they're trimming against too
enum RangeOp {
    Recurse(usize),
    TrimLt(usize),
    TrimGeq(usize),
    Erase(usize, usize),
}

type RangeProgram = Vec<RangeOp>;

// Categorises where a given key is to be found.  usizes are indexes into the
// key array.
enum KeyLoc {
    Within(usize),
    Exact(usize),
}

// The key must be >= to the first key in the node.
fn key_search<NV: Serializable>(node: &WNode<NV>, k: u32) -> KeyLoc {
    let idx = node.keys.bsearch(&k);

    assert!(idx >= 0);
    let idx = idx as usize;

    if node.keys.get(idx) == k {
        KeyLoc::Exact(idx)
    } else {
        KeyLoc::Within(idx)
    }
}

// All indexes in the program are *before* any operations were executed
fn range_split<NV: Serializable>(node: &WNode<NV>, key_begin: u32, key_end: u32) -> RangeProgram {
    use KeyLoc::*;
    use RangeOp::*;

    if node.is_empty() {
        // no entries
        return vec![];
    }

    if key_end <= node.keys.get(0) {
        // remove range is before this node
        return vec![];
    }

    let b = key_search(node, key_begin);
    let e = key_search(node, key_end);

    match (b, e) {
        // Recurse special cases:
        (Exact(idx1), Within(idx2)) if idx1 == idx2 => {
            vec![Recurse(idx1)]
        }
        (Within(idx1), Within(idx2)) if idx1 == idx2 => {
            vec![Recurse(idx1)]
        }
        (Within(idx1), Exact(idx2)) if (idx2 - idx1) == 1 => {
            vec![Recurse(idx1)]
        }

        // General cases:
        (Exact(idx1), Exact(idx2)) => {
            vec![Erase(idx1, idx2)]
        }
        (Exact(idx1), Within(idx2)) => {
            if idx2 == idx1 {
                vec![TrimGeq(idx1)]
            } else {
                vec![Erase(idx1, idx2), TrimLt(idx2)]
            }
        }
        (Within(idx1), Exact(idx2)) => {
            if idx2 - idx1 == 1 {
                vec![TrimGeq(idx1)]
            } else {
                vec![TrimGeq(idx1), Erase(idx1 + 1, idx2)]
            }
        }
        (Within(idx1), Within(idx2)) => {
            if idx2 - idx1 == 1 {
                vec![TrimGeq(idx1), TrimLt(idx2)]
            } else {
                vec![TrimGeq(idx1), Erase(idx1 + 1, idx2), TrimLt(idx2)]
            }
        }
    }
}

fn remove_range_internal<V>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<RecurseResult>
where
    V: Serializable + Copy,
{
    use RangeOp::*;

    let mut node = alloc.shadow::<MetadataBlock>(loc)?;
    let prog = range_split(&node, key_begin, key_end);
    let prog_len = prog.len();

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(idx) => {
                assert!(prog_len == 1);
                return remove_range_recurse(
                    alloc,
                    node.values.get(idx),
                    key_begin,
                    key_end,
                    split_lt,
                    split_geq,
                );
            }

            // The rest of the ops are guaranteed to return a Single, so we don't need
            // to do anything fancy aggregating them.
            TrimLt(idx) => {
                let idx = idx - delta;

                let res = remove_lt_recurse(alloc, node.values.get(idx), key_end, split_lt)?;
                node_insert_result(alloc, &mut node, idx, &res)?;
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                let res = remove_geq_recurse(alloc, node.values.get(idx), key_begin, split_geq)?;
                node_insert_result(alloc, &mut node, idx, &res)?;
            }
            Erase(idx_b, idx_e) => {
                let idx_b = idx_b - delta;
                let idx_e = idx_e - delta;
                node.erase(idx_b, idx_e);
                delta += idx_e - idx_b;
            }
        }
    }
    Ok(RecurseResult::single(&node))
}

fn remove_range_leaf<V>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<RecurseResult>
where
    V: Serializable + Copy,
{
    use RangeOp::*;

    let mut node = alloc.shadow::<V>(loc)?;
    let prog = range_split(&node, key_begin, key_end);
    let prog_len = prog.len();

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(idx) => {
                assert!(prog_len == 1);

                // This means the range hits the middle of an entry.
                // So we'll have to split it in two.
                let k = node.keys.get(idx);
                let v = node.values.get(idx);
                match (split_geq(k, v), split_lt(k, v)) {
                    (None, None) => {
                        node.remove_at(idx);
                        return Ok(RecurseResult::single(&node));
                    }
                    (Some((k, v)), None) => {
                        node.overwrite_at(idx, k, &v);
                        return Ok(RecurseResult::single(&node));
                    }
                    (None, Some((k, v))) => {
                        node.overwrite_at(idx, k, &v);
                        return Ok(RecurseResult::single(&node));
                    }
                    (Some((k1, v1)), Some((k2, v2))) => {
                        eprintln!("k1 = {:?}, k2 = {:?}", k1, k2);
                        node.overwrite_at(idx, k1, &v1);
                        return ensure_space(alloc, &mut node, idx, |node, idx| {
                            node.insert_at(idx + 1, k2, &v2)
                        });
                    }
                }
            }
            TrimLt(idx) => {
                let idx = idx - delta;
                match split_lt(node.keys.get(idx), node.values.get(idx)) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, v)) => {
                        node.keys.set(idx, &new_key);
                        node.values.set(idx, &v);
                    }
                }
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                match split_geq(node.keys.get(idx), node.values.get(idx)) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, v)) => {
                        node.keys.set(idx, &new_key);
                        node.values.set(idx, &v);
                    }
                }
            }
            Erase(idx_b, idx_e) => {
                let idx_b = idx_b - delta;
                let idx_e = idx_e - delta;
                node.erase(idx_b, idx_e);
                delta += idx_e - idx_b;
            }
        }
    }

    Ok(RecurseResult::single(&node))
}

fn remove_range_recurse<V>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<RecurseResult>
where
    V: Serializable + Copy,
{
    if alloc.is_internal(loc)? {
        remove_range_internal(alloc, loc, key_begin, key_end, split_lt, split_geq)
    } else {
        remove_range_leaf(alloc, loc, key_begin, key_end, split_lt, split_geq)
    }
}

pub fn remove_range<V>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<MetadataBlock>
where
    V: Serializable + Copy,
{
    use RecurseResult::*;

    match remove_range_recurse(alloc, root, key_begin, key_end, split_lt, split_geq)? {
        Single(NodeInfo { loc, .. }) => Ok(loc),
        Pair(left, right) => {
            let mut parent = init_node::<MetadataBlock>(alloc.new_block()?, false)?;
            parent.append_many(
                &[left.key_min.unwrap(), right.key_min.unwrap()],
                &[left.loc, right.loc],
            );
            Ok(parent.loc)
        }
    }
}

//-------------------------------------------------------------------------
