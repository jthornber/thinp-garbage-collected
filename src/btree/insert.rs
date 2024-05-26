use anyhow::Result;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

fn has_space_for_insert<NV: Serializable, Data: Readable>(node: &Node<NV, Data>) -> bool {
    node.nr_entries.get() < Node::<NV, Data>::max_entries() as u32
}

fn min_key(alloc: &mut NodeAlloc, loc: MetadataBlock) -> Result<u32> {
    // It's safe to alway assume this is an internal node, since we only access
    // the keys.
    let node = alloc.read::<MetadataBlock>(loc)?;
    Ok(node.keys.get(0))
}

fn split_into_two<NV: Serializable>(
    alloc: &mut NodeAlloc,
    mut left: WNode<NV>,
) -> Result<(WNode<NV>, WNode<NV>)> {
    let right_block = alloc.new_block()?;
    let mut right = init_node(right_block.clone(), left.is_leaf())?;
    redistribute2(&mut left, &mut right);

    Ok((left, right))
}

enum InsertResult {
    Single(MetadataBlock),
    Pair(MetadataBlock, MetadataBlock),
}

fn ensure_space<NV: Serializable, M: FnOnce(&mut WNode<NV>, usize)>(
    alloc: &mut NodeAlloc,
    mut node: WNode<NV>,
    idx: usize,
    mutator: M,
) -> Result<InsertResult> {
    if !has_space_for_insert(&node) {
        let (mut left, mut right) = split_into_two::<NV>(alloc, node)?;

        if idx < left.nr_entries() {
            mutator(&mut left, idx);
        } else {
            mutator(&mut right, idx - left.nr_entries());
        }

        Ok(InsertResult::Pair(left.loc, right.loc))
    } else {
        mutator(&mut node, idx);
        Ok(InsertResult::Single(node.loc))
    }
}

fn insert_into_internal<V: Serializable>(
    alloc: &mut NodeAlloc,
    loc: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<InsertResult> {
    use InsertResult::*;

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
    drop(node);

    // recurse
    match insert_recursive::<V>(alloc, child_loc, key, value)? {
        Single(new_loc) => {
            let mut node = alloc.shadow(loc)?;
            node.values.set(idx, &new_loc);
            Ok(Single(node.loc))
        }
        Pair(left, right) => {
            eprintln!("pair");
            let mut node = alloc.shadow(loc)?;
            node.values.set(idx, &left);
            let right_key = min_key(alloc, right)?;
            ensure_space(alloc, node, idx, |node, idx| {
                node.insert_at(idx + 1, right_key, &right)
            })
        }
    }
}

fn insert_into_leaf<V: Serializable>(
    alloc: &mut NodeAlloc,
    block: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<InsertResult> {
    let mut node = alloc.shadow::<V>(block)?;
    let idx = node.keys.bsearch(&key);

    if idx < 0 {
        eprintln!("prepend, key = {}", key);
        ensure_space(alloc, node, idx as usize, |node, _idx| {
            node.prepend(key, value)
        })
    } else if idx as usize >= node.keys.len() {
        eprintln!("append");
        ensure_space(alloc, node, idx as usize, |node, _idx| {
            node.append(key, value)
        })
    } else if node.keys.get(idx as usize) == key {
        // overwrite
        eprintln!("overwrite");
        node.values.set(idx as usize, value);
        Ok(InsertResult::Single(node.loc))
    } else {
        eprintln!("insert");
        ensure_space(alloc, node, idx as usize, |node, idx| {
            node.insert_at(idx + 1, key, value)
        })
    }
}

fn insert_recursive<V: Serializable>(
    alloc: &mut NodeAlloc,
    block: MetadataBlock,
    key: u32,
    value: &V,
) -> Result<InsertResult> {
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
    use InsertResult::*;

    match insert_recursive(alloc, root, key, value)? {
        Single(loc) => Ok(loc),
        Pair(left, right) => {
            let mut parent = init_node::<MetadataBlock>(alloc.new_block()?, false)?;
            eprintln!(
                "min_key(left) = {}, min_key(right) = {}",
                min_key(alloc, left)?,
                min_key(alloc, right)?
            );
            parent.append_many(
                &[min_key(alloc, left)?, min_key(alloc, right)?],
                &[left, right],
            );
            Ok(parent.loc)
        }
    }
}

//-------------------------------------------------
