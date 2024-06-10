use anyhow::anyhow;
use anyhow::Result;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

fn remove_internal<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
) -> Result<NodeResult> {
    let mut node = cache.shadow::<NodePtr, INode>(n_ptr)?;

    let mut idx = node.lower_bound(key);
    if idx < 0 {
        return Ok(NodeResult::single(&node));
    }

    if idx as usize == node.nr_entries() {
        idx -= 1;
    }

    let idx = idx as usize;

    let child = node.get_value(idx);
    let res = remove_recurse::<V, INode, LNode>(cache, child, key)?;
    node_insert_result(cache, &mut node, idx, &res)
}

fn remove_leaf<V: Serializable, LNode: NodeW<V, ExclusiveProxy>>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
) -> Result<NodeResult> {
    let mut node = cache.shadow::<V, LNode>(n_ptr)?;

    let idx = node.lower_bound(key);
    if (idx >= 0) && ((idx as usize) < node.nr_entries()) {
        let idx = idx as usize;
        if node.get_key(idx) == key {
            node.remove_at(idx);
        }
    }
    Ok(NodeResult::single(&node))
}

fn remove_recurse<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
) -> Result<NodeResult> {
    if cache.is_internal(n_ptr)? {
        remove_internal::<V, INode, LNode>(cache, n_ptr, key)
    } else {
        remove_leaf::<V, LNode>(cache, n_ptr, key)
    }
}

pub fn remove<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    root: NodePtr,
    key: u32,
) -> Result<NodePtr> {
    use NodeResult::*;

    match remove_recurse::<V, INode, LNode>(cache, root, key)? {
        Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
        Pair(left, right) => {
            let mut parent: INode = cache.new_node(false)?;
            parent.append(
                &[left.key_min.unwrap(), right.key_min.unwrap()],
                &[left.n_ptr, right.n_ptr],
            );
            Ok(parent.n_ptr())
        }
    }
}

//-------------------------------------------------------------------------

// All usizes are indexes
// FIXME: Trim ops should hold the key they're trimming against too
enum NodeOp {
    Recurse(usize),
    TrimLt(usize),
    TrimGeq(usize),
    Erase(usize, usize),
}

type NodeProgram = Vec<NodeOp>;

fn lt_prog<V: Serializable, N: NodeW<V, ExclusiveProxy>>(node: &N, key: u32) -> NodeProgram {
    use NodeOp::*;

    if node.nr_entries() == 0 {
        return vec![];
    }

    match node.lower_bound(key) {
        idx if idx < 0 => {
            vec![]
        }
        idx if node.get_key(idx as usize) == key => {
            vec![Erase(0, idx as usize)]
        }
        idx => {
            vec![Erase(0, idx as usize), TrimLt(idx as usize)]
        }
    }
}

fn remove_lt_internal<V, INode: NodeW<NodePtr, ExclusiveProxy>, LNode: NodeW<V, ExclusiveProxy>>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = cache.shadow::<NodePtr, INode>(n_ptr)?;
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
                    cache,
                    node.get_value(idx),
                    key,
                    split_fn,
                )?;

                // remove_lt cannot cause a Pair result, so we don't need to preserve the result
                node_insert_result(cache, &mut node, idx, &res)?;
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

fn remove_lt_leaf<V, LNode: NodeW<V, ExclusiveProxy>>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = cache.shadow::<V, LNode>(n_ptr)?;
    let prog = lt_prog(&node, key);

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(_) => {
                panic!("unexpected recurse");
            }
            TrimLt(idx) => match split_fn(node.get_key(idx), node.get_value(idx)) {
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
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult> {
    if cache.is_internal(n_ptr)? {
        remove_lt_internal::<V, INode, LNode>(cache, n_ptr, key, split_fn)
    } else {
        remove_lt_leaf::<V, LNode>(cache, n_ptr, key, split_fn)
    }
}

pub fn remove_lt<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    root: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodePtr> {
    match remove_lt_recurse::<V, INode, LNode>(cache, root, key, split_fn)? {
        NodeResult::Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
        NodeResult::Pair(_, _) => Err(anyhow!("remove_lt increase nr entries somehow")),
    }
}

//-------------------------------------------------------------------------

fn geq_prog<V: Serializable, N: NodeW<V, ExclusiveProxy>>(node: &N, key: u32) -> NodeProgram {
    use NodeOp::*;

    let nr_entries = node.nr_entries();
    if nr_entries == 0 {
        return vec![];
    }

    match node.lower_bound(key) {
        idx if idx < 0 => {
            vec![Erase(0, node.nr_entries())]
        }
        idx if node.get_key(idx as usize) == key => {
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

fn remove_geq_internal<V, INode: NodeW<NodePtr, ExclusiveProxy>, LNode: NodeW<V, ExclusiveProxy>>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = cache.shadow::<NodePtr, INode>(n_ptr)?;
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
                    cache,
                    node.get_value(idx),
                    key,
                    split_fn,
                )?;

                // remove_geq cannot cause a Pair result, so this can't split node.
                node_insert_result(cache, &mut node, idx, &res)?;
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }

    Ok(NodeResult::single(&node))
}

fn remove_geq_leaf<V, LNode: NodeW<V, ExclusiveProxy>>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable,
{
    use NodeOp::*;

    let mut node = cache.shadow::<V, LNode>(n_ptr)?;
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
            TrimGeq(idx) => match split_fn(node.get_key(idx), node.get_value(idx)) {
                None => {
                    node.remove_at(idx);
                }
                Some((new_key, new_value)) => {
                    node.overwrite(idx, new_key, &new_value);
                }
            },
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
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodeResult> {
    if cache.is_internal(n_ptr)? {
        remove_geq_internal::<V, INode, LNode>(cache, n_ptr, key, split_fn)
    } else {
        remove_geq_leaf::<V, LNode>(cache, n_ptr, key, split_fn)
    }
}

pub fn remove_geq<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    root: NodePtr,
    key: u32,
    split_fn: &ValFn<V>,
) -> Result<NodePtr> {
    match remove_geq_recurse::<V, INode, LNode>(cache, root, key, split_fn)? {
        NodeResult::Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
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
fn key_search<V: Serializable, N: NodeW<V, ExclusiveProxy>>(node: &N, k: u32) -> KeyLoc {
    let idx = node.lower_bound(k);

    assert!(idx >= 0);
    let idx = idx as usize;

    if node.get_key(idx) == k {
        KeyLoc::Exact(idx)
    } else {
        KeyLoc::Within(idx)
    }
}

// All indexes in the program are *before* any operations were executed
fn range_split<V: Serializable, N: NodeW<V, ExclusiveProxy>>(
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

    if key_end <= node.get_key(0) {
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

fn remove_range_internal<
    V,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<NodeResult>
where
    V: Serializable + Copy,
{
    use NodeOp::*;

    let mut node = cache.shadow::<NodePtr, INode>(n_ptr)?;
    let prog = range_split(&node, key_begin, key_end);
    let prog_len = prog.len();

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(idx) => {
                assert!(prog_len == 1);
                return remove_range_recurse::<V, INode, LNode>(
                    cache,
                    node.get_value(idx),
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
                    cache,
                    node.get_value(idx),
                    key_end,
                    split_lt,
                )?;
                node_insert_result(cache, &mut node, idx, &res)?;
            }
            TrimGeq(idx) => {
                let idx = idx - delta;
                let res = remove_geq_recurse::<V, INode, LNode>(
                    cache,
                    node.get_value(idx),
                    key_begin,
                    split_geq,
                )?;
                node_insert_result(cache, &mut node, idx, &res)?;
            }
            Erase(idx_b, idx_e) => {
                node.erase(idx_b - delta, idx_e - delta);
                delta += idx_e - idx_b;
            }
        }
    }
    Ok(NodeResult::single(&node))
}

fn remove_range_leaf<V: Serializable + Copy, LNode: NodeW<V, ExclusiveProxy>>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<NodeResult> {
    use NodeOp::*;

    let mut node = cache.shadow::<V, LNode>(n_ptr)?;
    let prog = range_split(&node, key_begin, key_end);
    let prog_len = prog.len();

    let mut delta = 0;
    for op in prog {
        match op {
            Recurse(idx) => {
                assert!(prog_len == 1);

                // This means the range hits the middle of an entry.
                // So we'll have to split it in two.
                let k = node.get_key(idx);
                let v = node.get_value(idx);
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
                        return ensure_space(cache, &mut node, idx, |node, idx| {
                            node.insert(idx + 1, k2, &v2)
                        });
                    }
                }
            }
            TrimLt(idx) => {
                let idx = idx - delta;
                match split_lt(node.get_key(idx), node.get_value(idx)) {
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
                match split_geq(node.get_key(idx), node.get_value(idx)) {
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
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    n_ptr: NodePtr,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<NodeResult> {
    if cache.is_internal(n_ptr)? {
        remove_range_internal::<V, INode, LNode>(
            cache, n_ptr, key_begin, key_end, split_lt, split_geq,
        )
    } else {
        remove_range_leaf::<V, LNode>(cache, n_ptr, key_begin, key_end, split_lt, split_geq)
    }
}

pub fn remove_range<
    V: Serializable + Copy,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    root: NodePtr,
    key_begin: u32,
    key_end: u32,
    split_lt: &ValFn<V>,
    split_geq: &ValFn<V>,
) -> Result<NodePtr> {
    use NodeResult::*;

    match remove_range_recurse::<V, INode, LNode>(
        cache, root, key_begin, key_end, split_lt, split_geq,
    )? {
        Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
        Pair(left, right) => {
            let mut parent: INode = cache.new_node(false)?;
            parent.append(
                &[left.key_min.unwrap(), right.key_min.unwrap()],
                &[left.n_ptr, right.n_ptr],
            );
            Ok(parent.n_ptr())
        }
    }
}

//-------------------------------------------------------------------------
