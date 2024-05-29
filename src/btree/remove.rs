use anyhow::anyhow;
use anyhow::Result;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

fn remove_internal<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<MetadataBlock, INode>(loc)?;

    let mut idx = node.lower_bound(key);
    if idx < 0 {
        return Ok(NodeResult::single(&node));
    }

    if idx as usize == node.nr_entries() {
        idx -= 1;
    }

    let idx = idx as usize;

    let child = node.get_value(idx).unwrap();
    let res = remove_recurse::<V, INode, LNode>(alloc, child, key)?;
    node_insert_result(alloc, &mut node, idx, &res)
}

fn remove_leaf<V: Serializable, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<V, LNode>(loc)?;

    let idx = node.lower_bound(key);
    if (idx >= 0) && ((idx as usize) < node.nr_entries()) {
        let idx = idx as usize;
        if node.get_key(idx).unwrap() == key {
            node.remove_at(idx);
        }
    }
    Ok(NodeResult::single(&node))
}

fn remove_recurse<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
) -> Result<NodeResult> {
    if alloc.is_internal(loc)? {
        remove_internal::<V, INode, LNode>(alloc, loc, key)
    } else {
        remove_leaf::<V, LNode>(alloc, loc, key)
    }
}

pub fn remove<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
) -> Result<MetadataBlock> {
    use NodeResult::*;

    match remove_recurse::<V, INode, LNode>(alloc, root, key)? {
        Single(NodeInfo { loc, .. }) => Ok(loc),
        Pair(left, right) => {
            let block = alloc.new_block()?;
            INode::init(block.loc(), block.clone(), false)?;
            let mut parent = INode::open(block.loc(), block.clone())?;
            parent.append(
                &[left.key_min.unwrap(), right.key_min.unwrap()],
                &[left.loc, right.loc],
            );
            Ok(parent.loc())
        }
    }
}

//-------------------------------------------------------------------------

pub type ValFn<'a, V> = Box<dyn Fn(u32, V) -> Option<(u32, V)> + 'a>;

#[allow(dead_code)]
pub fn mk_val_fn<'a, V, F>(f: F) -> ValFn<'a, V>
where
    V: Serializable,
    F: Fn(u32, V) -> Option<(u32, V)> + 'a,
{
    Box::new(f)
}

// All usizes are indexes
// FIXME: Trim ops should hold the key they're trimming against too
enum NodeOp {
    Recurse(usize),
    TrimLt(usize),
    TrimGeq(usize),
    Erase(usize, usize),
}

type NodeProgram = Vec<NodeOp>;

fn lt_prog<V: Serializable, N: NodeW<V, WriteProxy>>(node: &N, key: u32) -> NodeProgram {
    use NodeOp::*;

    if node.nr_entries() == 0 {
        return vec![];
    }

    match node.lower_bound(key) {
        idx if idx < 0 => {
            vec![]
        }
        idx if node.get_key(idx as usize).unwrap() == key => {
            vec![Erase(0, idx as usize)]
        }
        idx => {
            vec![Erase(0, idx as usize), TrimLt(idx as usize)]
        }
    }
}

fn remove_lt_internal<V, INode: NodeW<MetadataBlock, WriteProxy>, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = alloc.shadow::<MetadataBlock, INode>(loc)?;
    let prog = lt_prog(&node, key);

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(_) => {
                panic!("unexpected recurse");
            }
            TrimLt(idx) => {
                let idx = idx - delta;
                let res = remove_lt_recurse::<V, INode, LNode>(
                    alloc,
                    node.get_value(idx).unwrap(),
                    key,
                    split_fn,
                )?;

                // remove_lt cannot cause a Pair result, so we don't need to preserve the result
                node_insert_result(alloc, &mut node, idx, &res)?;
            }
            TrimGeq(_) => {
                panic!("unexpected trim geq");
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }

    Ok(NodeResult::single(&node))
}

fn remove_lt_leaf<V, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = alloc.shadow::<V, LNode>(loc)?;
    let prog = lt_prog(&node, key);

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(_) => {
                panic!("unexpected recurse");
            }
            TrimLt(idx) => match split_fn(node.get_key(idx).unwrap(), node.get_value(idx).unwrap())
            {
                None => {
                    node.remove_at(idx);
                }
                Some((new_key, new_value)) => {
                    node.overwrite(idx, new_key, &new_value);
                }
            },
            TrimGeq(_) => {
                panic!("unexpected trim geq");
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }

    Ok(NodeResult::single(&node))
}

pub fn remove_lt_recurse<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult> {
    if alloc.is_internal(loc)? {
        remove_lt_internal::<V, INode, LNode>(alloc, loc, key, split_fn)
    } else {
        remove_lt_leaf::<V, LNode>(alloc, loc, key, split_fn)
    }
}

pub fn remove_lt<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<MetadataBlock> {
    match remove_lt_recurse::<V, INode, LNode>(alloc, root, key, split_fn)? {
        NodeResult::Single(NodeInfo { loc, .. }) => Ok(loc),
        NodeResult::Pair(_, _) => Err(anyhow!("remove_lt increase nr entries somehow")),
    }
}

//-------------------------------------------------------------------------

fn geq_prog<V: Serializable, N: NodeW<V, WriteProxy>>(node: &N, key: u32) -> NodeProgram {
    use NodeOp::*;

    let nr_entries = node.nr_entries();
    if nr_entries == 0 {
        return vec![];
    }

    match node.lower_bound(key) {
        idx if idx < 0 => {
            vec![Erase(0, node.nr_entries())]
        }
        idx if node.get_key(idx as usize).unwrap() == key => {
            vec![Erase(idx as usize, nr_entries)]
        }
        idx => {
            let idx = idx as usize;
            if idx + 1 < nr_entries {
                vec![TrimGeq(idx), Erase(idx + 1, nr_entries)]
            } else {
                vec![TrimGeq(idx)]
            }
        }
    }
}

fn remove_geq_internal<V, INode: NodeW<MetadataBlock, WriteProxy>, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = alloc.shadow::<MetadataBlock, INode>(loc)?;
    let prog = geq_prog(&node, key);

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(_) => {
                panic!("unexpected recurse");
            }
            TrimLt(_) => {
                panic!("unexpected thin lt");
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                let res = remove_geq_recurse::<V, INode, LNode>(
                    alloc,
                    node.get_value(idx).unwrap(),
                    key,
                    split_fn,
                )?;

                // remove_geq cannot cause a Pair result, so this can't split node.
                node_insert_result(alloc, &mut node, idx, &res)?;
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }

    Ok(NodeResult::single(&node))
}

fn remove_geq_leaf<V, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = alloc.shadow::<V, LNode>(loc)?;
    let prog = geq_prog(&node, key);

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(_) => {
                panic!("unexpected recurse");
            }
            TrimLt(_) => {
                panic!("unexpected trim lt");
            }
            TrimGeq(idx) => {
                match split_fn(node.get_key(idx).unwrap(), node.get_value(idx).unwrap()) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, new_value)) => {
                        node.overwrite(idx, new_key, &new_value);
                    }
                }
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }

    Ok(NodeResult::single(&node))
}

fn remove_geq_recurse<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult> {
    if alloc.is_internal(loc)? {
        remove_geq_internal::<V, INode, LNode>(alloc, loc, key, split_fn)
    } else {
        remove_geq_leaf::<V, LNode>(alloc, loc, key, split_fn)
    }
}

pub fn remove_geq<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<MetadataBlock> {
    match remove_geq_recurse::<V, INode, LNode>(alloc, root, key, split_fn)? {
        NodeResult::Single(NodeInfo { loc, .. }) => Ok(loc),
        NodeResult::Pair(_, _) => Err(anyhow!("remove_geq increased nr of entries")),
    }
}

//-------------------------------------------------------------------------

// Categorises where a given key is to be found.  usizes are indexes into the
// key array.
enum KeyLoc {
    Within(usize),
    Exact(usize),
}

// The key must be >= to the first key in the node.
fn key_search<V: Serializable, N: NodeW<V, WriteProxy>>(node: &N, k: u32) -> KeyLoc {
    let idx = node.lower_bound(k);

    assert!(idx >= 0);
    let idx = idx as usize;

    if node.get_key(idx).unwrap() == k {
        KeyLoc::Exact(idx)
    } else {
        KeyLoc::Within(idx)
    }
}

// All indexes in the program are *before* any operations were executed
fn range_split<V: Serializable, N: NodeW<V, WriteProxy>>(
    node: &N,
    key_begin: u32,
    key_end: u32,
) -> NodeProgram {
    use KeyLoc::*;
    use NodeOp::*;

    if node.is_empty() {
        // no entries
        return vec![];
    }

    if key_end <= node.get_key(0).unwrap() {
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

fn remove_range_internal<V, INode: NodeW<MetadataBlock, WriteProxy>, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable + Copy,
{
    use NodeOp::*;

    let mut node = alloc.shadow::<MetadataBlock, INode>(loc)?;
    let prog = range_split(&node, key_begin, key_end);
    let prog_len = prog.len();

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(idx) => {
                assert!(prog_len == 1);
                return remove_range_recurse::<V, INode, LNode>(
                    alloc,
                    node.get_value(idx).unwrap(),
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

                let res = remove_lt_recurse::<V, INode, LNode>(
                    alloc,
                    node.get_value(idx).unwrap(),
                    key_end,
                    split_lt,
                )?;
                node_insert_result(alloc, &mut node, idx, &res)?;
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                let res = remove_geq_recurse::<V, INode, LNode>(
                    alloc,
                    node.get_value(idx).unwrap(),
                    key_begin,
                    split_geq,
                )?;
                node_insert_result(alloc, &mut node, idx, &res)?;
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }
    Ok(NodeResult::single(&node))
}

fn remove_range_leaf<V: Serializable + Copy, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<NodeResult> {
    use NodeOp::*;

    let mut node = alloc.shadow::<V, LNode>(loc)?;
    let prog = range_split(&node, key_begin, key_end);
    let prog_len = prog.len();

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(idx) => {
                assert!(prog_len == 1);

                // This means the range hits the middle of an entry.
                // So we'll have to split it in two.
                let k = node.get_key(idx).unwrap();
                let v = node.get_value(idx).unwrap();
                match (split_geq(k, v), split_lt(k, v)) {
                    (None, None) => {
                        node.remove_at(idx);
                        return Ok(NodeResult::single(&node));
                    }
                    (Some((k, v)), None) => {
                        node.overwrite(idx, k, &v);
                        return Ok(NodeResult::single(&node));
                    }
                    (None, Some((k, v))) => {
                        node.overwrite(idx, k, &v);
                        return Ok(NodeResult::single(&node));
                    }
                    (Some((k1, v1)), Some((k2, v2))) => {
                        eprintln!("k1 = {:?}, k2 = {:?}", k1, k2);
                        node.overwrite(idx, k1, &v1);
                        return ensure_space(alloc, &mut node, idx, |node, idx| {
                            node.insert(idx + 1, k2, &v2)
                        });
                    }
                }
            }
            TrimLt(idx) => {
                let idx = idx - delta;
                match split_lt(node.get_key(idx).unwrap(), node.get_value(idx).unwrap()) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, v)) => {
                        node.overwrite(idx, new_key, &v);
                    }
                }
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                match split_geq(node.get_key(idx).unwrap(), node.get_value(idx).unwrap()) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, v)) => {
                        node.overwrite(idx, new_key, &v);
                    }
                }
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }

    Ok(NodeResult::single(&node))
}

fn remove_range_recurse<
    V: Serializable + Copy,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<NodeResult> {
    if alloc.is_internal(loc)? {
        remove_range_internal::<V, INode, LNode>(
            alloc, loc, key_begin, key_end, split_lt, split_geq,
        )
    } else {
        remove_range_leaf::<V, LNode>(alloc, loc, key_begin, key_end, split_lt, split_geq)
    }
}

pub fn remove_range<
    V: Serializable + Copy,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<MetadataBlock> {
    use NodeResult::*;

    match remove_range_recurse::<V, INode, LNode>(
        alloc, root, key_begin, key_end, split_lt, split_geq,
    )? {
        Single(NodeInfo { loc, .. }) => Ok(loc),
        Pair(left, right) => {
            let proxy = alloc.new_block()?;
            INode::init(proxy.loc(), proxy.clone(), false)?;
            let mut parent = INode::open(proxy.loc(), proxy)?;
            parent.append(
                &[left.key_min.unwrap(), right.key_min.unwrap()],
                &[left.loc, right.loc],
            );
            Ok(parent.loc())
        }
    }
}

//-------------------------------------------------------------------------
