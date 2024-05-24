use anyhow::Result;

use crate::block_cache::*;
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

// Returns the new lowest key (if there is one), and the location of node
fn node_result<NV: Serializable>(node: WNode<NV>) -> (Option<u32>, MetadataBlock) {
    if node.is_empty() {
        (None, node.loc)
    } else {
        (Some(node.keys.get(0)), node.loc)
    }
}

fn remove_lt_internal<V, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<MetadataBlock>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            match remove_lt_recurse(alloc, node.values.get(idx), key, split_fn)? {
                (None, _loc) => {
                    node.remove_at(idx);
                }
                (Some(new_key), loc) => {
                    node.keys.set(idx, &new_key);
                    node.values.set(idx, &loc);
                }
            }

            node.shift_left_no_return(idx);
        }
        Shift(idx) => {
            node.shift_left_no_return(idx);
        }
    }

    Ok(node_result(node))
}

fn remove_lt_leaf<V, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<V>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            match split_fn(node.keys.get(idx), &node.values.get(idx)) {
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

    Ok(node_result(node))
}

pub fn remove_lt_recurse<LeafV, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    LeafV: Serializable,
    SplitFn: Fn(u32, &LeafV) -> Option<(u32, LeafV)>,
{
    if alloc.is_internal(loc)? {
        remove_lt_internal(alloc, loc, key, split_fn)
    } else {
        remove_lt_leaf(alloc, loc, key, split_fn)
    }
}

pub fn remove_lt<LeafV, SplitFn>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<MetadataBlock>
where
    LeafV: Serializable,
    SplitFn: Fn(u32, &LeafV) -> Option<(u32, LeafV)>,
{
    let (_, new_root) = remove_lt_recurse(alloc, root, key, split_fn)?;
    Ok(new_root)
}

//-------------------------------------------------------------------------

fn remove_geq_internal<V, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<MetadataBlock>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            match remove_geq_recurse(alloc, node.values.get(idx), key, split_fn)? {
                (None, _loc) => {
                    node.remove_at(idx);
                }
                (Some(new_key), loc) => {
                    node.keys.set(idx, &new_key);
                    node.values.set(idx, &loc);
                }
            }

            node.remove_from(idx + 1);
        }
        Shift(idx) => {
            node.remove_from(idx);
        }
    }

    Ok(node_result(node))
}

fn remove_geq_leaf<V, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    use SplitOp::*;

    let mut node = alloc.shadow::<V>(loc)?;
    match split_op(&node, key) {
        Noop => {}
        SplitAndShift(idx) => {
            match split_fn(node.keys.get(idx), &node.values.get(idx)) {
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

    Ok(node_result(node))
}
pub fn remove_geq_recurse<LeafV, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    LeafV: Serializable,
    SplitFn: Fn(u32, &LeafV) -> Option<(u32, LeafV)>,
{
    if alloc.is_internal(loc)? {
        remove_geq_internal(alloc, loc, key, split_fn)
    } else {
        remove_geq_leaf(alloc, loc, key, split_fn)
    }
}

pub fn remove_geq<LeafV, SplitFn>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    split_fn: &SplitFn,
) -> Result<MetadataBlock>
where
    LeafV: Serializable,
    SplitFn: Fn(u32, &LeafV) -> Option<(u32, LeafV)>,
{
    let (_, new_root) = remove_geq_recurse(alloc, root, key, split_fn)?;
    Ok(new_root)
}

//-------------------------------------------------------------------------

// All usizes are indexes
enum RangeOp {
    TrimLt(usize),
    TrimGeq(usize),
    Erase(usize, usize),
}

type RangeProgram = Vec<RangeOp>;

// All indexes in the program are *before* any operations were executed
fn range_split<NV: Serializable>(node: &WNode<NV>, key_begin: u32, key_end: u32) -> RangeProgram {
    todo!();
}

fn remove_range_internal<V, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &SplitFn,
    split_geq: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    use RangeOp::*;

    let mut node = alloc.shadow::<MetadataBlock>(loc)?;
    let prog = range_split(&node, key_begin, key_end);

    let mut delta = 0;
    for op in prog {
        match op {
            TrimLt(idx) => {
                let idx = idx - delta;
                match remove_lt_recurse(alloc, node.values.get(idx), key_end, split_lt)? {
                    (None, _loc) => {
                        node.remove_at(idx);
                    }
                    (Some(new_key), loc) => {
                        node.keys.set(idx, &new_key);
                        node.values.set(idx, &loc);
                    }
                }
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                match remove_geq_recurse(alloc, node.values.get(idx), key_begin, split_geq)? {
                    (None, _loc) => {
                        node.remove_at(idx);
                    }
                    (Some(new_key), loc) => {
                        node.keys.set(idx, &new_key);
                        node.values.set(idx, &loc);
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

    Ok(node_result(node))
}

fn remove_range_leaf<V, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &SplitFn,
    split_geq: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    use RangeOp::*;

    let mut node = alloc.shadow::<V>(loc)?;
    let prog = range_split(&node, key_begin, key_end);

    let mut delta = 0;
    for op in prog {
        match op {
            TrimLt(idx) => {
                let idx = idx - delta;
                match split_lt(node.keys.get(idx), &node.values.get(idx)) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, loc)) => {
                        node.keys.set(idx, &new_key);
                        node.values.set(idx, &loc);
                    }
                }
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                match split_geq(node.keys.get(idx), &node.values.get(idx)) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, loc)) => {
                        node.keys.set(idx, &new_key);
                        node.values.set(idx, &loc);
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

    Ok(node_result(node))
}

pub fn remove_range_recurse<V, SplitFn>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &SplitFn,
    split_geq: &SplitFn,
) -> Result<(Option<u32>, MetadataBlock)>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    if alloc.is_internal(loc)? {
        remove_range_internal(alloc, loc, key_begin, key_end, split_lt, split_geq)
    } else {
        remove_range_leaf(alloc, loc, key_begin, key_end, split_lt, split_geq)
    }
}

pub fn remove_range<V, SplitFn>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &SplitFn,
    split_geq: &SplitFn,
) -> Result<MetadataBlock>
where
    V: Serializable,
    SplitFn: Fn(u32, &V) -> Option<(u32, V)>,
{
    let (_, new_root) = remove_range_recurse(alloc, root, key_begin, key_end, split_lt, split_geq)?;
    Ok(new_root)
}

//-------------------------------------------------------------------------
