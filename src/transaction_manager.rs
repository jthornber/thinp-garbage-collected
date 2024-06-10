use anyhow::Result;
use std::collections::BTreeSet;

use crate::block_allocator::*;
use crate::block_cache::*;
use crate::byte_types::*;
use crate::scope_id::*;

use std::sync::{Arc, Mutex};

//------------------------------------------------------------------------------

/// We never share blocks within a single data structure (btree, mtree, etc).
/// However, we do share blocks between different data structures.  We use this
/// context type to distinguish between data structs to force shadowing once
/// per struct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReferenceContext {
    DevTree,     // There is a single dev tree per pool.
    ThinId(u32), // mtrees or btrees are associated with a thin id.
    Scoped(u32), // A temprorary context that lives for a code scope.
}

//------------------------------------------------------------------------------

struct TransactionManager_ {
    allocator: Arc<Mutex<BlockAllocator>>,
    cache: Arc<MetadataCache>,
    pub scopes: Arc<Mutex<ScopeRegister>>,
    shadows: BTreeSet<(ReferenceContext, MetadataBlock)>,
}

impl TransactionManager_ {
    fn new(allocator: Arc<Mutex<BlockAllocator>>, cache: Arc<MetadataCache>) -> Self {
        Self {
            allocator,
            cache,
            scopes: Arc::new(Mutex::new(ScopeRegister::default())),
            shadows: BTreeSet::new(),
        }
    }

    fn commit(&mut self, _roots: &[MetadataBlock]) -> Result<()> {
        todo!();

        /*
                {
                    let mut allocator = self.allocator.lock().unwrap();

                    // quiesce the gc
                    allocator.gc_quiesce();
                    allocator.set_roots(roots);
                }

                // FIXME: check that only the superblock is held
                self.cache.flush()?;

                // writeback the superblock
                self.superblock = None;
                self.cache.flush()?;

                // set new roots ready for next gc
                // FIXME: finish

                // get superblock for next transaction
                self.superblock = Some(self.cache.write_lock(SUPERBLOCK_LOC, &SUPERBLOCK_KIND)?);

                // clear shadows
                self.shadows.clear();

                // resume the gc
                self.allocator.lock().unwrap().gc_resume();

                Ok(())
        */
    }

    fn read(&self, loc: MetadataBlock) -> Result<SharedProxy> {
        let b = self.cache.shared_lock(loc)?;
        Ok(b)
    }

    fn new_block(&mut self, context: ReferenceContext) -> Result<ExclusiveProxy> {
        if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata()? {
            let b = self.cache.zero_lock(loc)?;
            self.shadows.insert((context, loc));
            Ok(b)
        } else {
            // FIXME: I think we need our own error type to distinguish
            // between io error and out of metadata blocks.
            Err(anyhow::anyhow!("out of metadata blocks"))
        }
    }

    /// A shadow is a copy of a metadata block.  To minimise copying we
    /// try and only copy a block only once within each transaction.
    ///
    /// There is a corner case we need to be careful of though; if a
    /// shadowed block has the number of times it is referenced increased, since
    /// is was shadowed, but within this transaction, then we need to force another
    /// copy to be made.  But we don't track the reference counts, so we make the
    /// call on whether to copy based on both the parent and the block to be copied.
    /// If None is passed for the old_parent then we always copy.
    ///
    /// Note: I initially thought we could have a 'inc_ref()' method that just removes
    /// a block from the shadow set.  But this won't work because we need to start
    /// calling inc_ref() for children blocks if we ever shadow that block again.
    ///
    fn shadow(
        &mut self,
        context: ReferenceContext,
        old_loc: MetadataBlock,
    ) -> Result<ExclusiveProxy> {
        if self.shadows.contains(&(context, old_loc)) {
            Ok(self.cache.exclusive_lock(old_loc)?)
        } else if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata()? {
            eprintln!("shadowing {}", old_loc);
            let old = self.cache.shared_lock(old_loc)?;
            let mut new = self.cache.zero_lock(loc)?;
            self.shadows.insert((context, loc));

            // We're careful not to touch the block header
            // FIXME: I don't think we need the subscripts?
            new.rw()[0..].copy_from_slice(&old.r()[0..]);
            Ok(new)
        } else {
            Err(anyhow::anyhow!("out of metadata blocks"))
        }
    }
}

//------------------------------------------------------------------------------

pub struct TransactionManager {
    inner: Mutex<TransactionManager_>,
}

impl TransactionManager {
    pub fn new(allocator: Arc<Mutex<BlockAllocator>>, cache: Arc<MetadataCache>) -> Self {
        Self {
            inner: Mutex::new(TransactionManager_::new(allocator, cache)),
        }
    }

    pub fn scopes(&self) -> Arc<Mutex<ScopeRegister>> {
        use std::ops::DerefMut;
        let mut inner = self.inner.lock().unwrap();
        inner.deref_mut().scopes.clone()
    }

    pub fn commit(&self, roots: &[MetadataBlock]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.commit(roots)
    }

    pub fn read(&self, loc: MetadataBlock) -> Result<SharedProxy> {
        let inner = self.inner.lock().unwrap();
        inner.read(loc)
    }

    pub fn new_block(&self, context: ReferenceContext) -> Result<ExclusiveProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.new_block(context)
    }

    pub fn shadow(&self, context: ReferenceContext, loc: MetadataBlock) -> Result<ExclusiveProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.shadow(context, loc)
    }
}

//------------------------------------------------------------------------------
