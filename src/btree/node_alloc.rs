use anyhow::Result;
use std::sync::Arc;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub struct NodeAlloc {
    tm: Arc<TransactionManager>,
    context: ReferenceContext,
}

impl NodeAlloc {
    pub fn new(tm: Arc<TransactionManager>, context: ReferenceContext) -> Self {
        Self { tm, context }
    }

    pub fn new_block(&mut self) -> Result<WriteProxy> {
        self.tm.new_block(self.context, &BNODE_KIND)
    }

    pub fn is_internal(&mut self, loc: MetadataBlock) -> Result<bool> {
        let b = self.tm.read(loc, &BNODE_KIND)?;
        Ok(read_flags(b.r())? == BTreeFlags::Internal)
    }

    pub fn shadow<NV: Serializable>(&mut self, loc: MetadataBlock) -> Result<WNode<NV>> {
        Ok(w_node(self.tm.shadow(self.context, loc, &BNODE_KIND)?))
    }
}

//-------------------------------------------------------------------------

pub fn redistribute2<NV: Serializable>(left: &mut WNode<NV>, right: &mut WNode<NV>) {
    let nr_left = left.nr_entries.get() as usize;
    let nr_right = right.nr_entries.get() as usize;
    let total = nr_left + nr_right;
    let target_left = total / 2;

    match nr_left.cmp(&target_left) {
        std::cmp::Ordering::Less => {
            // Move entries from right to left
            let nr_move = target_left - nr_left;
            let (keys, values) = right.shift_left(nr_move);
            left.append(&keys, &values);
        }
        std::cmp::Ordering::Greater => {
            // Move entries from left to right
            let nr_move = nr_left - target_left;
            let (keys, values) = left.remove_right(nr_move);
            right.prepend(&keys, &values);
        }
        std::cmp::Ordering::Equal => { /* do nothing */ }
    }
}

// FIXME: common code with insert
pub fn ensure_space<NV: Serializable, M: FnOnce(&mut WNode<NV>, usize)>(
    alloc: &mut NodeAlloc,
    left: &mut WNode<NV>,
    idx: usize,
    mutator: M,
) -> Result<NodeResult> {
    if left.is_full() {
        let right_block = alloc.new_block()?;
        let mut right = init_node(right_block.clone(), left.is_leaf())?;
        redistribute2(left, &mut right);

        if idx < left.nr_entries() {
            mutator(left, idx);
        } else {
            mutator(&mut right, idx - left.nr_entries());
        }

        Ok(NodeResult::pair(left, &right))
    } else {
        mutator(left, idx);
        Ok(NodeResult::single(left))
    }
}

// Call this when recursing back up the spine
pub fn node_insert_result(
    alloc: &mut NodeAlloc,
    node: &mut WNode<MetadataBlock>,
    idx: usize,
    res: &NodeResult,
) -> Result<NodeResult> {
    use NodeResult::*;

    match res {
        Single(NodeInfo { key_min: None, .. }) => {
            node.keys.remove_at(idx);
            node.values.remove_at(idx);
            Ok(NodeResult::single(node))
        }
        Single(NodeInfo {
            key_min: Some(new_key),
            loc,
        }) => {
            node.keys.set(idx, new_key);
            node.values.set(idx, loc);
            Ok(NodeResult::single(node))
        }
        Pair(left, right) => {
            node.keys.set(idx, &left.key_min.unwrap());
            node.values.set(idx, &left.loc);

            ensure_space(alloc, node, idx, |node, idx| {
                node.insert(idx + 1, right.key_min.unwrap(), &right.loc)
            })
        }
    }
}

//-------------------------------------------------------------------------
