use anyhow::Result;
use std::slice;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::btree::nodes::journal::*;
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
    fn insert_into_internal(&mut self, n_ptr: NodePtr, key: Key, value: &V) -> Result<NodeResult> {
        let mut node = self.cache.shadow::<NodePtr, INodeW>(n_ptr, 0)?;

        let mut idx = node.lower_bound(key);
        if idx < 0 {
            idx = 0
        }

        if idx > 0 && idx == node.nr_entries() as isize {
            idx -= 1;
        }

        let idx = idx as usize;
        let child_loc = node.get_value(idx);
        let res = self.insert_recursive(child_loc, key, value)?;
        self.node_insert_result(&mut node, idx, &res)
    }

    fn insert_into_leaf(&mut self, n_ptr: NodePtr, key: Key, value: &V) -> Result<NodeResult> {
        let mut node = self.cache.shadow::<V, LNodeW>(n_ptr, 0)?;
        let idx = node.lower_bound(key);

        if idx < 0 {
            ensure_space(
                self.cache.as_ref(),
                &mut node,
                idx as usize,
                |node, _idx| node.prepend(slice::from_ref(&key), slice::from_ref(value)),
            )
        } else if idx as usize >= node.nr_entries() {
            ensure_space(
                self.cache.as_ref(),
                &mut node,
                idx as usize,
                |node, _idx| node.append(slice::from_ref(&key), slice::from_ref(value)),
            )
        } else if node.get_key(idx as usize) == key {
            // overwrite
            ensure_space(self.cache.as_ref(), &mut node, idx as usize, |node, idx| {
                node.overwrite(idx, key, value)
            })
        } else {
            ensure_space(self.cache.as_ref(), &mut node, idx as usize, |node, idx| {
                node.insert(idx + 1, key, value)
            })
        }
    }

    fn insert_recursive(&mut self, n_ptr: NodePtr, key: Key, value: &V) -> Result<NodeResult> {
        if self.cache.is_internal(n_ptr)? {
            self.insert_into_internal(n_ptr, key, value)
        } else {
            self.insert_into_leaf(n_ptr, key, value)
        }
    }

    // Returns the new root
    pub fn insert_(&mut self, root: NodePtr, key: Key, value: &V) -> Result<NodePtr> {
        use NodeResult::*;

        match self.insert_recursive(root, key, value)? {
            Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
            Pair(left, right) => {
                let mut parent: JournalNode<INodeW, NodePtr, ExclusiveProxy> =
                    self.cache.new_node(false)?;
                parent.append(
                    &[left.key_min.unwrap(), right.key_min.unwrap()],
                    &[left.n_ptr, right.n_ptr],
                );
                Ok(parent.n_ptr())
            }
        }
    }

    // FIXME: merge with insert_
    pub fn insert(&mut self, key: Key, value: &V) -> Result<()> {
        self.root = self.insert_(self.root, key, value)?;
        Ok(())
    }
}

//-------------------------------------------------
