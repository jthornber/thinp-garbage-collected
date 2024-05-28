use anyhow::Result;
use std::slice;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::btree::simple_node::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

fn insert_into_internal<V: Serializable>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<MetadataBlock>(loc)?;
    let mut idx = node.keys.bsearch(&key);
    if idx < 0 {
        idx = 0
    }

    if idx > 0 && idx == node.nr_entries.get() as isize {
        idx -= 1;
    }

    let idx = idx as usize;

    let child_loc = node.values.get(idx);
    if key < node.keys.get(idx) {
        node.keys.set(idx, &key);
    }

    let res = insert_recursive::<V>(alloc, child_loc, key, value)?;
    node_insert_result(alloc, &mut node, idx, &res)
}

fn insert_into_leaf<V: Serializable>(
    alloc: &mut NodeAlloc,
    block: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    let mut node = alloc.shadow::<V>(block)?;
    let idx = node.keys.bsearch(&key);

    if idx < 0 {
        ensure_space(alloc, &mut node, idx as usize, |node, _idx| {
            node.prepend(slice::from_ref(&key), slice::from_ref(value))
        })
    } else if idx as usize >= node.keys.len() {
        ensure_space(alloc, &mut node, idx as usize, |node, _idx| {
            node.append(slice::from_ref(&key), slice::from_ref(value))
        })
    } else if node.keys.get(idx as usize) == key {
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

fn insert_recursive<V: Serializable>(
    alloc: &mut NodeAlloc,
    block: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<NodeResult> {
    if alloc.is_internal(block)? {
        insert_into_internal::<V>(alloc, block, key, value)
    } else {
        insert_into_leaf::<V>(alloc, block, key, value)
    }
}

// Returns the new root
pub fn insert<V: Serializable>(
    alloc: &mut NodeAlloc,
    root: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<MetadataBlock> {
    use NodeResult::*;

    match insert_recursive(alloc, root, key, value)? {
        Single(NodeInfo { loc, .. }) => Ok(loc),
        Pair(left, right) => {
            let mut parent = init_node::<MetadataBlock>(alloc.new_block()?, false)?;
            parent.append(
                &[left.key_min.unwrap(), right.key_min.unwrap()],
                &[left.loc, right.loc],
            );
            Ok(parent.loc)
        }
    }
}

//-------------------------------------------------
