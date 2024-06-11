use anyhow::{ensure, Result};
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_cache::*;
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
    fn check_keys_<NV: Serializable, Node: NodeR<NV, SharedProxy>>(
        node: &Node,
        key_min: u32,
        key_max: Option<u32>,
    ) -> Result<()> {
        // check the keys
        let mut last = None;
        for i in 0..node.nr_entries() {
            let k = node.get_key(i);
            ensure!(k >= key_min);

            if let Some(key_max) = key_max {
                ensure!(k < key_max);
            }

            if let Some(last) = last {
                if k <= last {
                    eprintln!("keys out of order: {}, {}", last, k);
                    ensure!(k > last);
                }
            }
            last = Some(k);
        }
        Ok(())
    }

    fn check_(
        &self,
        n_ptr: NodePtr,
        key_min: u32,
        key_max: Option<u32>,
        seen: &mut BTreeSet<u32>,
    ) -> Result<u32> {
        let mut total = 0;

        ensure!(!seen.contains(&n_ptr.loc));
        seen.insert(n_ptr.loc);

        if self.cache.is_internal(n_ptr)? {
            let node: INodeR = self.cache.read(n_ptr)?;

            Self::check_keys_(&node, key_min, key_max)?;

            for i in 0..node.nr_entries() {
                let kmin = node.get_key(i);
                // FIXME: redundant if, get_key_safe will handle it
                let kmax = if i == node.nr_entries() - 1 {
                    None
                } else {
                    node.get_key_safe(i + 1)
                };
                let loc = node.get_value(i);
                total += self.check_(loc, kmin, kmax, seen)?;
            }
        } else {
            let node: LNodeR = self.cache.read(n_ptr)?;
            Self::check_keys_(&node, key_min, key_max)?;
            total += node.nr_entries() as u32;
        }

        Ok(total)
    }

    /// Checks the btree is well formed and returns the number of entries
    /// in the tree.
    pub fn check(&self) -> Result<u32> {
        let mut seen = BTreeSet::new();
        self.check_(self.root, 0, None, &mut seen)
    }
}

//-------------------------------------------------------------------------
