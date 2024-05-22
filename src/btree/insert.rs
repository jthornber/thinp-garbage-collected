use anyhow::Result;
use std::sync::Arc;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub struct AllocContext {
    tm: Arc<TransactionManager>,
    context: ReferenceContext,
}

impl AllocContext {
    pub fn new(tm: Arc<TransactionManager>, context: ReferenceContext) -> Self {
        Self { tm, context }
    }

    fn new_block(&mut self) -> Result<WriteProxy> {
        self.tm.new_block(self.context, &BNODE_KIND)
    }

    fn is_internal(&mut self, loc: MetadataBlock) -> Result<bool> {
        let b = self.tm.read(loc, &BNODE_KIND)?;
        Ok(read_flags(b.r())? == BTreeFlags::Internal)
    }

    fn shadow<NV: Serializable>(&mut self, loc: MetadataBlock) -> Result<WNode<NV>> {
        Ok(w_node(self.tm.shadow(self.context, loc, &BNODE_KIND)?))
    }

    fn read<NV: Serializable>(&mut self, loc: MetadataBlock) -> Result<RNode<NV>> {
        Ok(r_node(self.tm.read(loc, &BNODE_KIND)?))
    }
}

fn has_space_for_insert<NV: Serializable, Data: Readable>(node: &Node<NV, Data>) -> bool {
    node.nr_entries.get() < Node::<NV, Data>::max_entries() as u32
}

fn min_key(alloc: &mut AllocContext, loc: MetadataBlock) -> Result<u32> {
    // It's safe to alway assume this is an internal node, since we only access
    // the keys.
    let node = alloc.read::<MetadataBlock>(loc)?;
    Ok(node.keys.get(0))
}

fn redistribute2<NV: Serializable>(left: &mut WNode<NV>, right: &mut WNode<NV>) {
    let nr_left = left.nr_entries.get() as usize;
    let nr_right = right.nr_entries.get() as usize;
    let total = nr_left + nr_right;
    let target_left = total / 2;

    match nr_left.cmp(&target_left) {
        std::cmp::Ordering::Less => {
            // Move entries from right to left
            let nr_move = target_left - nr_left;
            let (keys, values) = right.shift_left(nr_move);
            left.append_many(&keys, &values);
        }
        std::cmp::Ordering::Greater => {
            // Move entries from left to right
            let nr_move = nr_left - target_left;
            let (keys, values) = left.remove_right(nr_move);
            right.prepend_many(&keys, &values);
        }
        std::cmp::Ordering::Equal => { /* do nothing */ }
    }
}

fn split_into_two<NV: Serializable>(
    alloc: &mut AllocContext,
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
    alloc: &mut AllocContext,
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
    alloc: &mut AllocContext,
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
    alloc: &mut AllocContext,
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
    alloc: &mut AllocContext,
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
    alloc: &mut AllocContext,
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

enum RemoveResult<V> {
    NotFound,

    // We still return the empty metadata block in case this is the root
    RemoveChild(MetadataBlock, V),
    ReplaceChild(MetadataBlock, V),
}

// Returns Some((new_root, old_value)) if key is present, otherwise None.
fn remove_<V: Serializable>(
    alloc: &mut AllocContext,
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
    alloc: &mut AllocContext,
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
