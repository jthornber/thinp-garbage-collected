use anyhow::Result;
use std::sync::Arc;

use crate::block_cache::*;
use crate::btree::node::*;
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
        self.tm.new_block(self.context)
    }

    pub fn is_internal(&mut self, n_ptr: NodePtr) -> Result<bool> {
        let b = self.tm.read(n_ptr.loc)?;
        Ok(read_flags(&b)? == BTreeFlags::Internal)
    }

    pub fn shadow<V: Serializable, Node: NodeW<V, WriteProxy>>(
        &mut self,
        n_ptr: NodePtr,
    ) -> Result<Node> {
        let w_proxy = self.tm.shadow(self.context, n_ptr.loc)?;
        Node::open(w_proxy.loc(), w_proxy)
    }
}

//-------------------------------------------------------------------------

pub fn redistribute2<V: Serializable, Node: NodeW<V, WriteProxy>>(
    left: &mut Node,
    right: &mut Node,
) {
    let nr_left = left.nr_entries();
    let nr_right = right.nr_entries();
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
pub fn ensure_space<
    V: Serializable,
    Node: NodeW<V, WriteProxy>,
    M: Fn(&mut Node, usize) -> NodeInsertOutcome,
>(
    alloc: &mut NodeAlloc,
    left: &mut Node,
    idx: usize,
    mutator: M,
) -> Result<NodeResult> {
    use NodeInsertOutcome::*;

    match mutator(left, idx) {
        Success => Ok(NodeResult::single(left)),
        NoSpace => {
            let right_block = alloc.new_block()?;
            Node::init(right_block.loc(), right_block.clone(), left.is_leaf())?;
            let mut right = Node::open(right_block.loc(), right_block.clone())?;
            redistribute2(left, &mut right);

            if idx < left.nr_entries() {
                mutator(left, idx);
            } else {
                mutator(&mut right, idx - left.nr_entries());
            }

            Ok(NodeResult::pair(left, &right))
        }
    }
}

// Call this when recursing back up the spine
pub fn node_insert_result<Node: NodeW<NodePtr, WriteProxy>>(
    alloc: &mut NodeAlloc,
    node: &mut Node,
    idx: usize,
    res: &NodeResult,
) -> Result<NodeResult> {
    use NodeResult::*;

    match res {
        Single(NodeInfo { key_min: None, .. }) => {
            node.remove_at(idx);
            Ok(NodeResult::single(node))
        }
        Single(NodeInfo {
            key_min: Some(new_key),
            n_ptr,
        }) => {
            node.overwrite(idx, *new_key, n_ptr);
            Ok(NodeResult::single(node))
        }
        Pair(left, right) => {
            node.overwrite(idx, left.key_min.unwrap(), &left.n_ptr);
            ensure_space(alloc, node, idx, |node, idx| {
                node.insert(idx + 1, right.key_min.unwrap(), &right.n_ptr)
            })
        }
    }
}

//-------------------------------------------------------------------------
