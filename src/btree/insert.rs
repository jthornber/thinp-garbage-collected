use anyhow::Result;
use std::slice;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

fn insert_into_internal<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<MetadataBlock, INode>(loc)?;

    let mut idx = node.lower_bound(key);
    if idx < 0 {
        idx = 0
    }

    if idx > 0 && idx == node.nr_entries() as isize {
        idx -= 1;
    }

    let idx = idx as usize;
    let child_loc = node.get_value(idx).unwrap();
    let res = insert_recursive::<V, INode, LNode>(alloc, child_loc, key, value)?;
    node_insert_result(alloc, &mut node, idx, &res)
}

fn insert_into_leaf<V: Serializable, LNode: NodeW<V, WriteProxy>>(
    alloc: &mut NodeAlloc,
    block: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<V, LNode>(block)?;
    let idx = node.lower_bound(key);

    if idx < 0 {
        ensure_space(alloc, &mut node, idx as usize, |node, _idx| {
            node.prepend(slice::from_ref(&key), slice::from_ref(value))
        })
    } else if idx as usize >= node.nr_entries() {
        ensure_space(alloc, &mut node, idx as usize, |node, _idx| {
            node.append(slice::from_ref(&key), slice::from_ref(value))
        })
    } else if node.get_key(idx as usize).unwrap() == key {
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
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    block: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    if alloc.is_internal(block)? {
        insert_into_internal::<V, INode, LNode>(alloc, block, key, value)
    } else {
        insert_into_leaf::<V, LNode>(alloc, block, key, value)
    }
}

// Returns the new root
pub fn insert<
    V: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<MetadataBlock> {
    use NodeResult::*;

    match insert_recursive::<V, INode, LNode>(alloc, root, key, value)? {
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

//-------------------------------------------------
