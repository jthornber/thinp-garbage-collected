use anyhow::{anyhow, Result};
use std::sync::{Arc, Mutex};

use crate::block_cache::*;
use crate::btree::node::*;
use crate::buddy_alloc::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

// FIXME: make thread safe
pub struct NodeCacheInner {
    alloc: BuddyAllocator,
    cache: Arc<BlockCache>,
    snap_time: u32,
}

impl NodeCacheInner {
    pub fn new(cache: Arc<BlockCache>) -> Self {
        // FIXME: hard coded metadata size
        let alloc = BuddyAllocator::new(12); // 16m of metadata
        Self {
            alloc,
            cache,
            snap_time: 0,
        }
    }

    pub fn is_internal(&mut self, n_ptr: NodePtr) -> Result<bool> {
        let b = self.cache.shared_lock(n_ptr.loc)?;
        Ok(read_flags(&b)? == BTreeFlags::Internal)
    }

    pub fn read<V: Serializable, Node: NodeR<V, SharedProxy>>(
        &mut self,
        n_ptr: NodePtr,
    ) -> Result<Node> {
        // FIXME: check seq_nr and replay journal if necc.
        let b = self.cache.shared_lock(n_ptr.loc)?;
        Node::open(n_ptr.loc, b)
    }

    pub fn new_node<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &mut self,
        is_leaf: bool,
    ) -> Result<Node> {
        if let Some(loc) = self.alloc.alloc(1) {
            let new = self.cache.zero_lock(loc as u32)?;
            Node::init(loc as u32, new.clone(), is_leaf)?;
            Node::open(loc as u32, new)
        } else {
            // FIXME: resize the node file
            Err(anyhow!("couldn't allocate new block"))
        }
    }

    pub fn shadow<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &mut self,
        n_ptr: NodePtr,
    ) -> Result<Node> {
        let old = self.cache.exclusive_lock(n_ptr.loc)?;
        let hdr = read_node_header(&mut old.r())?;

        if self.snap_time > hdr.snap_time {
            // copy needed
            if let Some(loc) = self.alloc.alloc(1) {
                let mut new = self.cache.zero_lock(loc as u32)?;
                new.rw()[0..].copy_from_slice(&old.r()[0..]);
                Node::open(loc as u32, new)
            } else {
                Err(anyhow::anyhow!("out of metadata blocks"))
            }
        } else {
            Node::open(n_ptr.loc, old)
        }
    }
}

//-------------------------------------------------------------------------

pub struct NodeCache {
    inner: Arc<Mutex<NodeCacheInner>>,
}

impl NodeCache {
    pub fn new(cache: Arc<BlockCache>) -> Self {
        let inner = Arc::new(Mutex::new(NodeCacheInner::new(cache)));
        Self { inner }
    }

    pub fn is_internal(&self, n_ptr: NodePtr) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        inner.is_internal(n_ptr)
    }

    pub fn read<V: Serializable, Node: NodeR<V, SharedProxy>>(
        &self,
        n_ptr: NodePtr,
    ) -> Result<Node> {
        let mut inner = self.inner.lock().unwrap();
        inner.read(n_ptr)
    }

    pub fn new_node<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &self,
        is_leaf: bool,
    ) -> Result<Node> {
        let mut inner = self.inner.lock().unwrap();
        inner.new_node(is_leaf)
    }

    pub fn shadow<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &self,
        n_ptr: NodePtr,
    ) -> Result<Node> {
        let mut inner = self.inner.lock().unwrap();
        inner.shadow(n_ptr)
    }
}

//-------------------------------------------------------------------------

pub fn redistribute2<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
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

pub fn ensure_space<
    V: Serializable,
    Node: NodeW<V, ExclusiveProxy>,
    M: Fn(&mut Node, usize) -> NodeInsertOutcome,
>(
    cache: &NodeCache,
    left: &mut Node,
    idx: usize,
    mutator: M,
) -> Result<NodeResult> {
    use NodeInsertOutcome::*;

    match mutator(left, idx) {
        Success => Ok(NodeResult::single(left)),
        NoSpace => {
            let mut right = cache.new_node(left.is_leaf())?;
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
pub fn node_insert_result<Node: NodeW<NodePtr, ExclusiveProxy>>(
    cache: &NodeCache,
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
            ensure_space(cache, node, idx, |node, idx| {
                node.insert(idx + 1, right.key_min.unwrap(), &right.n_ptr)
            })
        }
    }
}

//-------------------------------------------------------------------------
