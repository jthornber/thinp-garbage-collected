use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::allocators::journal::*;
use crate::allocators::*;
use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::nodes::journal::*;
use crate::byte_types::*;
use crate::journal::BatchCompletion;
use crate::packed_array::*;

//-------------------------------------------------------------------------

// FIXME: make thread safe
pub struct NodeCacheInner {
    alloc: JournalAlloc<BuddyAllocator>,
    cache: Arc<BlockCache>,
}

impl NodeCacheInner {
    pub fn new(cache: Arc<BlockCache>, alloc: BuddyAllocator) -> Self {
        Self {
            alloc: JournalAlloc::new(alloc, AllocKind::Metadata),
            cache,
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

    fn wrap_node<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &mut self,
        loc: u32,
        data: ExclusiveProxy,
    ) -> Result<JournalNode<Node, V, ExclusiveProxy>> {
        let node = Node::open(loc, data)?;
        Ok(JournalNode::new(node))
    }

    pub fn new_node<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &mut self,
        is_leaf: bool,
    ) -> Result<JournalNode<Node, V, ExclusiveProxy>> {
        match self.alloc.alloc(1) {
            Ok(loc) => {
                let new = self.cache.zero_lock(loc as u32)?;
                Node::init(loc as u32, new.clone(), is_leaf)?;
                self.wrap_node(loc as u32, new)
            }
            Err(MemErr::OutOfSpace) => {
                // FIXME: resize the node file and kick off the gc
                panic!("out of nodes");
            }
            Err(e) => Err(anyhow::Error::from(e)),
        }
    }

    pub fn shadow<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &mut self,
        n_ptr: NodePtr,
        snap_time: u32,
    ) -> Result<JournalNode<Node, V, ExclusiveProxy>> {
        let old = self.cache.exclusive_lock(n_ptr.loc)?;
        let hdr = read_node_header(&mut old.r())?;

        if snap_time > hdr.snap_time {
            // copy needed
            if let Ok(loc) = self.alloc.alloc(1) {
                let mut new = self.cache.zero_lock(loc as u32)?;
                new.rw()[0..].copy_from_slice(&old.r()[0..]);
                self.wrap_node(loc as u32, new)
            } else {
                Err(anyhow::anyhow!("out of metadata blocks"))
            }
        } else {
            self.wrap_node(n_ptr.loc, old)
        }
    }
}

//-------------------------------------------------------------------------

type BatchId = u64;

pub struct NodeCache {
    inner: Arc<Mutex<NodeCacheInner>>,
}

impl NodeCache {
    pub fn new(cache: Arc<BlockCache>, alloc: BuddyAllocator) -> Self {
        let inner = Arc::new(Mutex::new(NodeCacheInner::new(cache, alloc)));
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
    ) -> Result<JournalNode<Node, V, ExclusiveProxy>> {
        let mut inner = self.inner.lock().unwrap();
        inner.new_node(is_leaf)
    }

    pub fn shadow<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &self,
        n_ptr: NodePtr,
        snap_time: u32,
    ) -> Result<JournalNode<Node, V, ExclusiveProxy>> {
        let mut inner = self.inner.lock().unwrap();
        inner.shadow(n_ptr, snap_time)
    }

    pub fn get_batch_id(&self) -> BatchId {
        // FIXME: finish once the block cache has been rewritten
        0
    }

    pub fn unpin_batch(&self, id: BatchId) {
        // FIXME: finish once the block cache has been rewritten
    }
}

//-------------------------------------------------------------------------

pub struct CacheCompletion {
    cache: Arc<NodeCache>,
    id: BatchId,
}

impl CacheCompletion {
    pub fn new(cache: Arc<NodeCache>) -> Self {
        let id = cache.get_batch_id();
        Self { cache, id }
    }
}

impl BatchCompletion for CacheCompletion {
    fn complete(&self) {
        self.cache.unpin_batch(self.id);
    }
}

//-------------------------------------------------------------------------

pub fn redistribute2<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
    left: &mut JournalNode<Node, V, ExclusiveProxy>,
    right: &mut JournalNode<Node, V, ExclusiveProxy>,
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

// FIXME: do we want to move this into BTree? and redistribute2?
pub fn ensure_space<
    V: Serializable,
    Node: NodeW<V, ExclusiveProxy>,
    M: Fn(&mut JournalNode<Node, V, ExclusiveProxy>, usize) -> NodeInsertOutcome,
>(
    cache: &NodeCache,
    left: &mut JournalNode<Node, V, ExclusiveProxy>,
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

//-------------------------------------------------------------------------
