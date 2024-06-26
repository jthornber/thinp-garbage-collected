use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::allocators::journal::*;
use crate::allocators::{self, *};
use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::nodes::journal::*;
use crate::byte_types::*;
use crate::journal::entry::*;
use crate::journal::BatchCompletion;
use crate::journal::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

// FIXME: is NodeCache the new transaction manager?  Should we rename?
pub struct TransactionManagerInner {
    journal: Arc<Mutex<Journal>>,
    metadata_alloc: Arc<Mutex<dyn Allocator>>,
    data_alloc: Arc<Mutex<dyn Allocator>>,
    cache: Arc<BlockCache>,
}

impl TransactionManagerInner {
    pub fn new(
        journal: Arc<Mutex<Journal>>,
        cache: Arc<BlockCache>,
        metadata_alloc: BuddyAllocator,
        data_alloc: BuddyAllocator,
    ) -> Self {
        let metadata_alloc = Arc::new(Mutex::new(JournalAlloc::new(
            metadata_alloc,
            AllocKind::Metadata,
        )));
        let data_alloc = Arc::new(Mutex::new(JournalAlloc::new(data_alloc, AllocKind::Data)));

        Self {
            journal,
            metadata_alloc,
            data_alloc,
            cache,
        }
    }

    pub fn alloc_data(&mut self, len: u64) -> allocators::Result<(u64, Vec<(u64, u64)>)> {
        let mut alloc = self.data_alloc.lock().unwrap();
        alloc.alloc_many(len, 0)
    }

    pub fn free_data(&mut self, b: u64, len: u64) -> allocators::Result<()> {
        let mut alloc = self.data_alloc.lock().unwrap();
        alloc.free(b, len)
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

    fn new_metadata_block(&mut self) -> allocators::Result<MetadataBlock> {
        let mut alloc = self.metadata_alloc.lock().unwrap();
        let b = alloc.alloc(1)?;
        Ok(b as MetadataBlock)
    }

    pub fn new_node<V: Serializable, Node: NodeW<V, ExclusiveProxy>>(
        &mut self,
        is_leaf: bool,
    ) -> Result<JournalNode<Node, V, ExclusiveProxy>> {
        match self.new_metadata_block() {
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
            if let Ok(loc) = self.new_metadata_block() {
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

    fn replay_node(&mut self, loc: MetadataBlock) -> Result<Box<dyn ReplayableNode>> {
        todo!();
    }

    pub fn replay_entry(&mut self, entry: &Entry) -> Result<()> {
        use Entry::*;

        match entry {
            AllocMetadata(b, e) => {
                // FIXME: we need to add alloc_at to the Allocator trait
                todo!()
            }
            FreeMetadata(b, e) => {
                todo!()
            }
            GrowMetadata(delta) => {
                todo!()
            }

            AllocData(b, e) => {
                todo!()
            }
            FreeData(b, e) => {
                todo!()
            }
            GrowData(delta) => {
                todo!()
            }

            UpdateInfoRoot(root) => {
                todo!()
            }

            SetSeq(loc, seq_nr) => {
                todo!()
            }
            Zero(loc, b, e) => {
                todo!()
            }

            Literal(loc, offset, data) => {
                todo!();
            }

            Shadow(loc, dest) => {
                todo!()
            }

            Overwrite(loc, idx, key, value) => {
                let mut n = self.replay_node(*loc)?;
                n.apply_overwrite(*idx, *key, &value)?;
            }
            Insert(loc, idx, k, v) => {
                let mut n = self.replay_node(*loc)?;
                n.apply_insert(*idx, *k, v)?;
            }
            Prepend(loc, ks, vs) => {
                let mut n = self.replay_node(*loc)?;
                n.apply_prepend(ks, &vs)?;
            }
            Append(loc, ks, vs) => {
                let mut n = self.replay_node(*loc)?;
                n.apply_append(ks, &vs)?;
            }
            Erase(loc, idx_b, idx_e) => {
                let mut n = self.replay_node(*loc)?;
                n.apply_erase(*idx_b, *idx_e)?;
            }
        }

        Ok(())
    }

    pub fn replay_entries(&mut self, entries: &[Entry]) -> Result<()> {
        for e in entries {
            self.replay_entry(e)?;
        }

        Ok(())
    }
}

//-------------------------------------------------------------------------

type BatchId = u64;

pub struct TransactionManager {
    inner: Arc<Mutex<TransactionManagerInner>>,
}

impl TransactionManager {
    pub fn new(
        journal: Arc<Mutex<Journal>>,
        cache: Arc<BlockCache>,
        metadata_alloc: BuddyAllocator,
        data_alloc: BuddyAllocator,
    ) -> Self {
        let inner = Arc::new(Mutex::new(TransactionManagerInner::new(
            journal,
            cache,
            metadata_alloc,
            data_alloc,
        )));
        Self { inner }
    }

    pub fn get_metadata_alloc(&self) -> Arc<Mutex<dyn Allocator>> {
        let mut inner = self.inner.lock().unwrap();
        inner.metadata_alloc.clone()
    }

    pub fn get_data_alloc(&self) -> Arc<Mutex<dyn Allocator>> {
        let mut inner = self.inner.lock().unwrap();
        inner.data_alloc.clone()
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
    cache: Arc<TransactionManager>,
    id: BatchId,
}

impl CacheCompletion {
    pub fn new(cache: Arc<TransactionManager>) -> Self {
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
    cache: &TransactionManager,
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
