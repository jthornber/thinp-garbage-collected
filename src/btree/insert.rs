use anyhow::Result;
use std::slice;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

fn insert_into_internal<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    alloc: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<NodePtr, INode>(n_ptr)?;

    let mut idx = node.lower_bound(key);
    if idx < 0 {
        idx = 0
    }

    if idx > 0 && idx == node.nr_entries() as isize {
        idx -= 1;
    }

    let idx = idx as usize;
    let child_loc = node.get_value(idx);
    let res = insert_recursive::<V, INode, LNode>(alloc, child_loc, key, value)?;
    node_insert_result(alloc, &mut node, idx, &res)
}

fn insert_into_leaf<V: Serializable, LNode: NodeW<V, ExclusiveProxy>>(
    alloc: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<V, LNode>(n_ptr)?;
    let idx = node.lower_bound(key);

    if idx < 0 {
        ensure_space(alloc, &mut node, idx as usize, |node, _idx| {
            node.prepend(slice::from_ref(&key), slice::from_ref(value))
        })
    } else if idx as usize >= node.nr_entries() {
        ensure_space(alloc, &mut node, idx as usize, |node, _idx| {
            node.append(slice::from_ref(&key), slice::from_ref(value))
        })
    } else if node.get_key(idx as usize) == key {
        // overwrite
        ensure_space(alloc, &mut node, idx as usize, |node, idx| {
            node.overwrite(idx, key, value)
        })
    } else {
        ensure_space(alloc, &mut node, idx as usize, |node, idx| {
            node.insert(idx + 1, key, value)
        })
    }
}

fn insert_recursive<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    alloc: &NodeCache,
    n_ptr: NodePtr,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    if alloc.is_internal(n_ptr)? {
        insert_into_internal::<V, INode, LNode>(alloc, n_ptr, key, value)
    } else {
        insert_into_leaf::<V, LNode>(alloc, n_ptr, key, value)
    }
}

// Returns the new root
pub fn insert<
    V: Serializable,
    INode: NodeW<NodePtr, ExclusiveProxy>,
    LNode: NodeW<V, ExclusiveProxy>,
>(
    cache: &NodeCache,
    root: NodePtr,
    key: u32,
    value: &V,
) -> Result<NodePtr> {
    use NodeResult::*;

    match insert_recursive::<V, INode, LNode>(cache, root, key, value)? {
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

//-------------------------------------------------
