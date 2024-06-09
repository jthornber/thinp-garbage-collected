use anyhow::Result;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::block_cache::*;
use crate::buddy_alloc::*;
use crate::transaction_manager::TransactionManager;

//-------------------------------------------------------------------------

// We use a 4k block size for both virtual and physical blocks
type VBlock = u64;
type PBlock = u64;

type ThinID = u64;

struct ThinInfo {
    size: VBlock,
    root: MetadataBlock,
}

#[allow(dead_code)]
struct Pool {
    tm: Arc<TransactionManager>,
    devs: BTreeMap<ThinID, ThinInfo>,

    meta_alloc: BuddyAllocator,
    data_alloc: BuddyAllocator,
}

struct Map {
    data_begin: PBlock,
    len: PBlock,
}

enum LookupResult {
    Unmapped(PBlock), // len
    Mapped(Map),
}

#[allow(dead_code)]
impl Pool {
    pub fn create<P: AsRef<Path>>(_dir: P) -> Self {
        todo!();
    }

    pub fn open<P: AsRef<Path>>(_dir: P) -> Self {
        todo!();
    }

    pub fn close(self) -> Result<()> {
        todo!()
    }

    pub fn create_thin(&mut self, _size: VBlock) -> Result<ThinID> {
        todo!();
    }

    pub fn create_thick(&mut self, _size: VBlock) -> Result<ThinID> {
        todo!();
    }

    pub fn create_snap(&mut self, _origin: ThinID) -> Result<ThinID> {
        todo!();
    }

    pub fn delete_thin(&mut self, _dev: ThinID) -> Result<()> {
        todo!();
    }

    pub fn nr_free_data_blocks(&self) -> Result<u64> {
        todo!();
    }

    pub fn nr_free_metadata_blocks(&self) -> Result<u64> {
        todo!();
    }

    pub fn metadata_dev_size(&self) -> Result<u64> {
        todo!();
    }

    pub fn data_dev_size(&self) -> Result<u64> {
        todo!();
    }

    // FIXME: can we return impl Iterator?
    pub fn get_read_mapping(
        &self,
        _dev: ThinID,
        _key_begin: VBlock,
        _key_end: VBlock,
    ) -> Result<VecDeque<LookupResult>> {
        todo!();
    }

    pub fn get_write_mapping(
        &self,
        _dev: ThinID,
        _key_begin: VBlock,
        _key_end: VBlock,
    ) -> Result<VecDeque<LookupResult>> {
        todo!();
    }

    pub fn discard(&mut self, _dev: ThinID, _key_begin: VBlock, _key_end: VBlock) -> Result<()> {
        todo!();
    }
}

//-------------------------------------------------------------------------
