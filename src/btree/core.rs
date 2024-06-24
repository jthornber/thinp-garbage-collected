use anyhow::Result;
use std::sync::Arc;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::nodes::journal::*;
use crate::btree::transaction_manager::*;
use crate::packed_array::*;

use crate::btree::BTree;

//-------------------------------------------------------------------------

impl<
        V: Serializable + Copy,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    pub fn open_tree(tm: Arc<TransactionManager>, root: NodePtr) -> Self {
        Self {
            tm,
            root,
            snap_time: 0,
            phantom_v: std::marker::PhantomData,
            phantom_inode_r: std::marker::PhantomData,
            phantom_inode_w: std::marker::PhantomData,
            phantom_lnode_r: std::marker::PhantomData,
            phantom_lnode_w: std::marker::PhantomData,
        }
    }

    pub fn empty_tree(tm: Arc<TransactionManager>) -> Result<Self> {
        let node = tm.new_node::<V, LNodeW>(true)?;
        let root = node.n_ptr();

        Ok(Self {
            tm,
            root,
            snap_time: 0,
            phantom_v: std::marker::PhantomData,
            phantom_inode_r: std::marker::PhantomData,
            phantom_inode_w: std::marker::PhantomData,
            phantom_lnode_r: std::marker::PhantomData,
            phantom_lnode_w: std::marker::PhantomData,
        })
    }

    pub fn snap(&mut self, snap_time: u32) -> Self {
        self.snap_time = snap_time;

        Self {
            tm: self.tm.clone(),
            root: self.root,
            snap_time,
            phantom_v: std::marker::PhantomData,
            phantom_inode_r: std::marker::PhantomData,
            phantom_inode_w: std::marker::PhantomData,
            phantom_lnode_r: std::marker::PhantomData,
            phantom_lnode_w: std::marker::PhantomData,
        }
    }

    pub fn root(&self) -> NodePtr {
        self.root
    }

    //-------------------------------

    // Call this when recursing back up the spine
    pub fn node_insert_result<N: NodeW<NodePtr, ExclusiveProxy>>(
        &mut self,
        node: &mut JournalNode<N, NodePtr, ExclusiveProxy>,
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
                // This check is worth it to save journal entries.
                if node.get_key(idx) != *new_key || node.get_value(idx) != *n_ptr {
                    node.overwrite(idx, *new_key, n_ptr);
                }
                Ok(NodeResult::single(node))
            }
            Pair(left, right) => {
                node.overwrite(idx, left.key_min.unwrap(), &left.n_ptr);
                ensure_space(self.tm.as_ref(), node, idx, |node, idx| {
                    node.insert(idx + 1, right.key_min.unwrap(), &right.n_ptr)
                })
            }
        }
    }
}

/*
pub fn btree_refs(r_proxy: &SharedProxy, queue: &mut VecDeque<BlockRef>) {
    let flags = read_flags(&r_proxy).expect("couldn't read node");

    match flags {
        BTreeFlags::Internal => {
            // FIXME: hard coded for now.  No point fixing this until we've switched
            // to log based transactions.
            let node = crate::btree::simple_node::SimpleNode::<NodePtr, SharedProxy>::open(
                r_proxy.loc(),
                r_proxy.clone(),
            )
            .unwrap();
            for i in 0..node.nr_entries.get() {
                queue.push_back(BlockRef::Metadata(node.values.get(i as usize).loc));
            }
        }
        BTreeFlags::Leaf => {
            // FIXME: values should be refs, except in the btree unit tests
        }
    }
}
*/

//-------------------------------------------------------------------------
