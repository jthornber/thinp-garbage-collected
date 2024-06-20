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
use crate::journal::batch;
use crate::journal::entry::*;
use crate::journal::*;
use crate::packed_array::*;
use crate::types::*;

//-------------------------------------------------------------------------

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
struct ThinInfo {
    size: VBlock,
    snap_time: u32,
    root: NodePtr,
}

impl Serializable for ThinInfo {
    fn packed_len() -> usize {
        8 + 4 + NodePtr::packed_len()
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.size)?;
        w.write_u32::<LittleEndian>(self.snap_time)?;
        self.root.pack(w)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let size = r.read_u64::<LittleEndian>()?;
        let snap_time = r.read_u32::<LittleEndian>()?;
        let root = NodePtr::unpack(r)?;
        Ok(Self {
            size,
            snap_time,
            root,
        })
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

struct Journaller {
    journal: Arc<Mutex<Journal>>,
    cache: Arc<NodeCache>,
}

impl Journaller {
    fn new(journal: Arc<Mutex<Journal>>, cache: Arc<NodeCache>) -> Self {
        Journaller { journal, cache }
    }

    fn batch<T, F: FnOnce() -> Result<T>>(&self, action: F) -> Result<T> {
        let batch_id = self.cache.get_batch_id();
        batch::begin_batch();
        let r = action();

        // We need to write the batch to the journal regardless since the node will
        // have been updated.
        let completion: Option<Box<dyn BatchCompletion>> =
            Some(Box::new(CacheCompletion::new(self.cache.clone())));
        let b = Batch {
            ops: batch::end_batch()?,
            completion,
        };
        self.journal.lock().unwrap().add_batch(b);

        r
    }
}

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
        batch::add_entry(Entry::UpdateInfoRoot(self.infos.root()))?;
        Ok(())
    }

    fn journalled<T, F: FnOnce() -> Result<T>>(&self, action: F) -> Result<T> {
        let journaller = Journaller::new(self.journal.clone(), self.cache.clone());
        journaller.batch(action)
    }

    fn journaller(&self) -> Journaller {
        Journaller::new(self.journal.clone(), self.cache.clone())
    }

    pub fn create_thin_(&mut self, size: VBlock) -> Result<(ThinID, MappingTree)> {
        let j = self.journaller();
        j.batch(|| {
            // Choose a new id
            let id = self.new_thin_id();

            // create new btree
            let mappings = MappingTree::empty_tree(self.cache.clone())?;

            Ok((id, mappings))
        })
    }

    pub fn create_thin(&mut self, size: VBlock) -> Result<ThinID> {
        let j = self.journaller();
        j.batch(|| {
            let (id, mappings) = self.create_thin_(size)?;
            // Add thin_info to btree
            let info = ThinInfo {
                size,
                snap_time: self.snap_time,
                root: mappings.root(),
            };
            self.infos.insert(id, &info)?;
            self.update_info_root()?;
            Ok(id)
        })
    }

    pub fn create_thick(&mut self, size: VBlock) -> Result<ThinID> {
        let j = self.journaller();
        j.batch(|| {
            // Create a new thin
            let (id, mut mappings) = self.create_thin_(size)?;

            // Provision the entire range
            self.provision(&mut mappings, 0, size)?;

            // Add thin_info to btree
            let info = ThinInfo {
                size,
                snap_time: self.snap_time,
                root: mappings.root(),
            };
            self.infos.insert(id, &info)?;
            self.update_info_root()?;

            Ok(id)
        })
    }

    pub fn create_snap(&mut self, origin: ThinID) -> Result<ThinID> {
        let j = self.journaller();
        j.batch(|| {
            // Get the mapping tree and info for the origin thin device
            let (mut origin_info, mut origin_mappings) = self.get_mapping_tree(origin)?;

            // Create a snapshot of the origin mapping tree
            let snap_mappings = origin_mappings.snap(self.snap_time);

            // Choose a new id for the snapshot
            let snap_id = self.new_thin_id();

            // Create a new ThinInfo for the snapshot
            let snap_info = ThinInfo {
                size: origin_info.size,
                snap_time: self.snap_time,
                root: snap_mappings.root(),
            };

            // Insert the new ThinInfo for the snapshot into the infos BTree
            self.infos.insert(snap_id, &snap_info)?;

            // Update the snap_time in the ThinInfo for the origin thin device
            origin_info.snap_time = self.snap_time;
            self.snap_time += 1;
            self.infos.insert(origin, &origin_info)?;

            // Update the info root
            self.update_info_root()?;
            Ok(snap_id)
        })
    }

    pub fn delete_thin(&mut self, dev: ThinID) -> Result<()> {
        let j = self.journaller();
        j.batch(|| {
            self.infos.remove(dev);
            self.update_info_root()?;
            Ok(())
        })
    }

    /*
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
    */

    fn get_mapping_tree(&self, dev: ThinID) -> Result<(ThinInfo, MappingTree)> {
        todo!();
    }

    // selects the part of a mapping that is above key_begin
    fn select_above(key_begin: Key, k: Key, m: Mapping) -> Option<(Key, Mapping)> {
        let len = m.e - m.b;
        if k + len > key_begin {
            let delta = key_begin - k;
            Some((
                key_begin,
                Mapping {
                    b: m.b + delta,
                    e: m.e,
                    snap_time: m.snap_time,
                },
            ))
        } else {
            None
        }
    }

    // selects the part of a mapping that is below key_end
    fn select_below(key_end: Key, k: Key, m: Mapping) -> Option<(Key, Mapping)> {
        if k < key_end {
            Some((
                k,
                Mapping {
                    b: m.b,
                    e: m.e.min(key_end),
                    snap_time: m.snap_time,
                },
            ))
        } else {
            None
        }
    }

    pub fn get_read_mapping(
        &self,
        id: ThinID,
        thin_begin: VBlock,
        thin_end: VBlock,
    ) -> Result<Vec<(u64, Mapping)>> {
        let (_, mappings) = self.get_mapping_tree(id)?;

        let select_above =
            mk_val_fn(move |k: Key, m: Mapping| Self::select_above(thin_begin, k, m));
        let select_below = mk_val_fn(move |k: Key, m: Mapping| Self::select_below(thin_end, k, m));

        mappings.lookup_range(thin_begin, thin_end, &select_above, &select_below)
    }

    fn should_break_sharing(info: &ThinInfo, m: &Mapping) -> bool {
        // Don't fill this in, I'll do it.
        todo!();
    }

    fn break_sharing(
        &mut self,
        mappings: &mut MappingTree,
        vbegin: VBlock,
        m: Mapping,
    ) -> Result<Mapping> {
        // Don't fill this in, I'll do it.
        todo!();
    }

    fn update_mappings_root(
        &mut self,
        id: ThinID,
        info: &mut ThinInfo,
        mappings: &MappingTree,
    ) -> Result<()> {
        info.root = mappings.root();
        self.infos.insert(id, info)?;
        self.update_info_root()
    }

    fn provision(
        &mut self,
        mappings: &mut MappingTree,
        mut current: VBlock,
        end: VBlock,
    ) -> Result<Vec<(VBlock, Mapping)>> {
        let size = end - current;
        let (total, runs) = self.data_alloc.alloc_many(size, 0)?;
        if total != size {
            // Not enough space, free the allocated data and return an error
            for (b, e) in runs {
                self.data_alloc.free(b, e - b)?;
            }
            return Err(anyhow!("Could not allocate enough data space"));
        }

        let mut provisioned = Vec::new();
        for (b, e) in runs {
            let mapping = Mapping {
                b,
                e,
                snap_time: self.snap_time,
            };
            mappings.insert(current, &mapping)?;
            provisioned.push((current, mapping));
            current += e - b;
        }
        Ok(provisioned)
    }

    pub fn get_write_mapping(
        &mut self,
        id: ThinID,
        thin_begin: VBlock,
        thin_end: VBlock,
    ) -> Result<Vec<(VBlock, Mapping)>> {
        let j = self.journaller();
        j.batch(|| {
            let (mut info, mut mappings) = self.get_mapping_tree(id)?;
            let select_above =
                mk_val_fn(move |k: Key, m: Mapping| Self::select_above(thin_begin, k, m));
            let select_below =
                mk_val_fn(move |k: Key, m: Mapping| Self::select_below(thin_end, k, m));
            let ms = mappings.lookup_range(thin_begin, thin_end, &select_above, &select_below)?;

            // Provision any gaps
            let mut current = thin_begin;
            let mut provisioned: Vec<(VBlock, Mapping)> = Vec::new();
            for (k, m) in ms.iter() {
                if current < *k {
                    // Provision new mappings for the gap
                    provisioned.extend(self.provision(&mut mappings, current, *k)?);
                }
                provisioned.push((*k, *m));
                current = m.e;
            }

            if current < thin_end {
                // Provision new mappings for the remaining gap
                provisioned.extend(self.provision(&mut mappings, current, thin_end)?);
            }

            // Break sharing if needed
            for (k, m) in provisioned.iter_mut() {
                if Self::should_break_sharing(&info.clone(), m) {
                    *m = self.break_sharing(&mut mappings, *k as VBlock, *m)?;
                }
            }

            self.update_mappings_root(id, &mut info, &mappings)?;
            Ok(provisioned)
        })
    }

    pub fn discard(&mut self, id: ThinID, thin_begin: VBlock, thin_end: VBlock) -> Result<()> {
        let j = self.journaller();
        j.batch(|| {
            let (mut info, mut mappings) = self.get_mapping_tree(id)?;

            let select_above =
                mk_val_fn(move |k: Key, m: Mapping| Self::select_above(thin_begin, k, m));
            let select_below =
                mk_val_fn(move |k: Key, m: Mapping| Self::select_below(thin_end, k, m));

            mappings.remove_range(thin_begin, thin_end, &select_below, &select_above)?;
            self.update_mappings_root(id, &mut info, &mappings)
        })
    }
}

//-------------------------------------------------------------------------
