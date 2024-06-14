use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use thinp::io_engine::*;

use crate::allocators::*;
use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::btree::nodes::simple::*;
use crate::btree::BTree;
use crate::btree::*;
use crate::core::*;
use crate::journal::*;
use crate::packed_array::*;
use crate::types::*;

//-------------------------------------------------------------------------

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
struct ThinInfo {
    size: VBlock,
    root: NodePtr,
}

impl Serializable for ThinInfo {
    fn packed_len() -> usize {
        8 + NodePtr::packed_len()
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.size)?;
        self.root.pack(w)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let size = r.read_u64::<LittleEndian>()?;
        let root = NodePtr::unpack(r)?;
        Ok(Self { size, root })
    }
}

type InfoTree = BTree<
    ThinInfo,
    SimpleNode<NodePtr, SharedProxy>,
    SimpleNode<NodePtr, ExclusiveProxy>,
    SimpleNode<ThinInfo, SharedProxy>,
    SimpleNode<ThinInfo, ExclusiveProxy>,
>;

//-------------------------------------------------------------------------

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
struct Mapping {
    b: PBlock,
    e: PBlock,
    snap_time: u32,
}

impl Serializable for Mapping {
    fn packed_len() -> usize {
        8 + 8 + 4
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.b)?;
        w.write_u64::<LittleEndian>(self.e)?;
        w.write_u32::<LittleEndian>(self.snap_time)?;
        Ok(())
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let b = r.read_u64::<LittleEndian>()?;
        let e = r.read_u64::<LittleEndian>()?;
        let snap_time = r.read_u32::<LittleEndian>()?;

        Ok(Self { b, e, snap_time })
    }
}

type MappingTree = BTree<
    Mapping,
    SimpleNode<NodePtr, SharedProxy>,
    SimpleNode<NodePtr, ExclusiveProxy>,
    SimpleNode<Mapping, SharedProxy>,
    SimpleNode<Mapping, ExclusiveProxy>,
>;

//-------------------------------------------------------------------------

#[allow(dead_code)]
struct Pool {
    engine: Arc<dyn IoEngine>,
    journal: Arc<Mutex<Journal>>,
    cache: Arc<NodeCache>,
    data_alloc: BuddyAllocator,

    infos: InfoTree,
    active_devs: BTreeMap<ThinID, MappingTree>,

    snap_time: u32,
    next_thin_id: ThinID,
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
        nr_metadata_blocks: u64,
        nr_data_blocks: u64,
    ) -> Result<Self> {
        /*
                let dir = dir.as_ref();

                // Create directory, failing if it already exists.
                if dir.exists() {
                    return Err(anyhow::anyhow!("Directory already exists"));
                }
                fs::create_dir_all(dir)?;

                // Create the node file in dir, this should have size 4k * 2^metadata_order
                let node_file_path = dir.join("node_file");
                let node_file_size = 4096 * nr_metadata_blocks;
                let node_file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(node_file_path)?;
                node_file.set_len(node_file_size)?;

                // Create journal in dir
                let journal_file_path = dir.join("journal");
                let journal = Journal::create(journal_file_path)?;

                // Initialize the buddy allocators
                let meta_alloc = BuddyAllocator::new(nr_metadata_blocks);
                let data_alloc = BuddyAllocator::new(nr_data_blocks);
                Ok(Pool {
                    journal,
                    devs: BTreeMap::new(),
                    meta_alloc,
                    data_alloc,
                })
        */
        todo!()
    }

    pub fn open<P: AsRef<Path>>(_dir: P) -> Self {
        todo!();
    }

    pub fn close(self) -> Result<()> {
        todo!()
    }

    fn new_thin_id(&mut self) -> ThinID {
        let id = self.next_thin_id;
        self.next_thin_id += 1;
        id
    }

    fn update_info_root(&mut self) -> Result<()> {
        let mut journal = self.journal.lock().unwrap();
        journal.add_op(Entry::UpdateInfoRoot(self.infos.root()))?;
        Ok(())
    }

    pub fn create_thin_(&mut self, size: VBlock) -> Result<(ThinID, MappingTree)> {
        // Choose a new id
        let id = self.new_thin_id();

        // create new btree
        let mappings = MappingTree::empty_tree(self.cache.clone())?;

        Ok((id, mappings))
    }

    pub fn create_thin(&mut self, size: VBlock) -> Result<ThinID> {
        let (id, mappings) = self.create_thin_(size)?;
        // Add thin_info to btree
        let info = ThinInfo {
            size,
            root: mappings.root(),
        };
        // FIXME: ThinID is 64bit, need 64bit keys
        self.infos.insert(id as u32, &info)?;
        self.update_info_root()?;
        Ok(id)
    }

    pub fn create_thick(&mut self, size: VBlock) -> Result<ThinID> {
        // Create a new thin
        let (id, mut mappings) = self.create_thin_(size)?;

        // Allocate enough data space to completely map it
        let (total, runs) = self.data_alloc.alloc_many(size, 0)?;

        if total != size {
            // not enough space, free off that data and return error
            for (b, e) in runs {
                self.data_alloc.free(b, e - b)?;
            }

            return Err(anyhow!("Could not allocate enough data space"));
        }

        // Insert mappings
        let mut virt_block = 0;
        for (b, e) in runs {
            // FIXME: need 64bit keys
            mappings.insert(
                virt_block as u32,
                &Mapping {
                    b,
                    e,
                    snap_time: self.snap_time,
                },
            )?;
        }

        // Add thin_info to btree
        let info = ThinInfo {
            size,
            root: mappings.root(),
        };
        // FIXME: ThinID is 64bit, need 64bit keys
        self.infos.insert(id as u32, &info)?;
        self.update_info_root()?;

        Ok(id)
    }

    pub fn create_snap(&mut self, _origin: ThinID) -> Result<ThinID> {
        todo!();
    }

    pub fn delete_thin(&mut self, dev: ThinID) -> Result<()> {
        // FIXME: need 64bit keys
        self.infos.remove(dev as u32);
        self.update_info_root()?;
        Ok(())
    }

    pub fn nr_free_data_blocks(&self) -> Result<u64> {
        self.data_alloc.nr_free()
    }

    pub fn nr_free_metadata_blocks(&self) -> Result<u64> {
        self.cache.nr_free()
    }

    pub fn metadata_dev_size(&self) -> Result<u64> {
        self.cache.nr_metadata_blocks()
    }

    pub fn data_dev_size(&self) -> Result<u64> {
        self.data_alloc.nr_blocks()
    }

    fn get_mapping_tree(&self, dev: ThinID) -> Result<MappingTree> {
        todo!();
    }

    // FIXME: can we return impl Iterator?
    pub fn get_read_mapping(
        &self,
        dev: ThinID,
        key_begin: VBlock,
        key_end: VBlock,
    ) -> Result<Vec<(u64, Mapping)>> {
        let mappings = self.get_mapping_tree(dev)?;

        // selects the part of a mapping that is above key_begin
        let select_above = move |k: u32, m: Mapping| {
            let k = k as u64; // FIXME: 64bit keys

            let len = m.e - m.b;
            if k + len > key_begin {
                let delta = key_begin - k;
                Some((
                    key_begin as u32,
                    Mapping {
                        b: m.b + delta,
                        e: m.e,
                        snap_time: m.snap_time,
                    },
                ))
            } else {
                None
            }
        };

        // selects the part of a mapping that is below key_end
        let select_below = move |k: u32, m: Mapping| {
            let k = k as u64; // FIXME: 64bit keys

            if k < key_end {
                Some((
                    k as u32,
                    Mapping {
                        b: m.b,
                        e: m.e.min(key_end),
                        snap_time: m.snap_time,
                    },
                ))
            } else {
                None
            }
        };

        // FIXME: need 64bit keys
        let ms = mappings.lookup_range(
            key_begin as u32,
            key_end as u32,
            &mk_val_fn(select_above),
            &mk_val_fn(select_below),
        )?;

        // FIXME 64bit keys
        Ok(ms.iter().map(|(k, m)| (*k as u64, m.clone())).collect())
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
