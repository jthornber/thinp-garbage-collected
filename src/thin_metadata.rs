use anyhow::Result;
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::Arc;

use crate::block_cache::*;
use crate::buddy_alloc::*;
use crate::journal::*;
use crate::types::*;

//-------------------------------------------------------------------------

struct ThinInfo {
    size: VBlock,
    root: MetadataBlock,
}

#[allow(dead_code)]
struct Pool {
    journal: Journal,
    devs: BTreeMap<ThinID, ThinInfo>,

    meta_alloc: BuddyAllocator,
    data_alloc: BuddyAllocator,
}

struct Map {
    data_begin: PBlock,
    len: PBlock,
}

enum Run {
    Unmapped(VBlock), // len
    Mapped(Map),
}

#[allow(dead_code)]
impl Pool {
    pub fn create<P: AsRef<Path>>(
        dir: P,
        metadata_order: usize,
        data_order: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref();

        // Create directory, failing if it already exists.
        if dir.exists() {
            return Err(anyhow::anyhow!("Directory already exists"));
        }
        fs::create_dir_all(dir)?;

        // Create the node file in dir, this should have size 4k * 2^metadata_order
        let node_file_path = dir.join("node_file");
        let node_file_size = 4096 * (1 << metadata_order);
        let node_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(node_file_path)?;
        node_file.set_len(node_file_size as u64)?;

        // Create journal in dir
        let journal_file_path = dir.join("journal");
        let journal = Journal::create(journal_file_path)?;

        // Initialize the buddy allocators
        let meta_alloc = BuddyAllocator::new(metadata_order);
        let data_alloc = BuddyAllocator::new(data_order);
        Ok(Pool {
            journal,
            devs: BTreeMap::new(),
            meta_alloc,
            data_alloc,
        })
    }

    pub fn open<P: AsRef<Path>>(_dir: P) -> Self {
        todo!();
    }

    pub fn close(self) -> Result<()> {
        todo!()
    }

    pub fn create_thin(&mut self, _size: VBlock) -> Result<ThinID> {
        // create new btree
        // Add thin_info to btree
        // add journal entry
        // sync journal
        todo!()
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
    ) -> Result<VecDeque<Run>> {
        todo!();
    }

    pub fn get_write_mapping(
        &self,
        _dev: ThinID,
        _key_begin: VBlock,
        _key_end: VBlock,
    ) -> Result<VecDeque<Run>> {
        todo!();
    }

    pub fn discard(&mut self, _dev: ThinID, _key_begin: VBlock, _key_end: VBlock) -> Result<()> {
        todo!();
    }
}

//-------------------------------------------------------------------------
