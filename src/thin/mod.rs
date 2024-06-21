use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rio::{Completion, Rio};
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
use crate::copier::fake::*;
use crate::copier::*;
use crate::core::*;
use crate::journal::batch;
use crate::journal::entry::*;
use crate::journal::*;
use crate::packed_array::*;
use crate::types::*;

//-------------------------------------------------------------------------

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct ThinInfo {
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

pub type InfoTree = BTree<
    ThinInfo,
    SimpleNode<NodePtr, SharedProxy>,
    SimpleNode<NodePtr, ExclusiveProxy>,
    SimpleNode<ThinInfo, SharedProxy>,
    SimpleNode<ThinInfo, ExclusiveProxy>,
>;

//-------------------------------------------------------------------------

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct Mapping {
    pub b: PBlock,
    pub e: PBlock,
    pub snap_time: u32,
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

pub type MappingTree = BTree<
    Mapping,
    SimpleNode<NodePtr, SharedProxy>,
    SimpleNode<NodePtr, ExclusiveProxy>,
    SimpleNode<Mapping, SharedProxy>,
    SimpleNode<Mapping, ExclusiveProxy>,
>;

//-------------------------------------------------------------------------

// FIXME: still needed?
type Mappings = BTreeMap<VBlock, Mapping>;

/// Converts from the vec that comes back from btree lookup to the more useful
/// Mappings data structure (which is easier to adjust).
fn build_mappings(ms: &[(VBlock, Mapping)]) -> Mappings {
    let mut result = BTreeMap::new();
    for (vbegin, m) in ms {
        result.insert(*vbegin, *m);
    }
    result
}

//-------------------------------------------------------------------------

struct MetadataOps {
    removes: Vec<(VBlock, VBlock)>,
    inserts: Vec<(VBlock, Mapping)>,
}

impl MetadataOps {
    fn new() -> Self {
        Self {
            removes: Vec::new(),
            inserts: Vec::new(),
        }
    }

    fn push_insert(&mut self, vb: VBlock, m: &Mapping) {
        self.inserts.push((vb, m.clone()));
    }

    fn push_remove(&mut self, b: VBlock, e: VBlock) {
        if let Some((last_b, last_e)) = self.removes.last_mut() {
            if *last_e == b {
                // Merge ranges
                *last_e = e;
                return;
            }
        }

        self.removes.push((b, e));
    }

    fn removes(&self) -> &[(VBlock, VBlock)] {
        &self.removes
    }

    fn inserts(&self) -> &[(VBlock, Mapping)] {
        &self.inserts
    }
}

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
pub struct Pool {
    copier: Arc<dyn Copier>,
    journal: Arc<Mutex<Journal>>,
    cache: Arc<NodeCache>,
    data_alloc: BuddyAllocator,

    infos: InfoTree,
    active_devs: BTreeMap<ThinID, MappingTree>,

    snap_time: u32,
    next_thin_id: ThinID,
}

pub struct Map {
    data_begin: PBlock,
    len: PBlock,
}

pub enum Run {
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
        let dir = dir.as_ref();
        // Create directory, failing if it already exists.
        if dir.exists() {
            return Err(anyhow::anyhow!("Directory already exists"));
        }
        fs::create_dir_all(dir)?;
        // Create the node file in dir, this should have size 4k * nr_metadata_blocks
        let node_file_path = dir.join("node_file");
        let node_file_size = 4096 * nr_metadata_blocks;
        let node_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&node_file_path)?;
        node_file.set_len(node_file_size)?;

        let copier: Arc<dyn Copier> = Arc::new(FakeCopier::new());

        // Initialize the IoEngine
        let engine = Arc::new(SyncIoEngine::new(&node_file_path, true)?);

        // Initialize the BlockCache
        let block_cache = Arc::new(BlockCache::new(engine.clone(), 16)?);

        // Initialize the BuddyAllocator for metadata and data
        let meta_alloc = BuddyAllocator::new(nr_metadata_blocks);
        let data_alloc = BuddyAllocator::new(nr_data_blocks);

        // Initialize the NodeCache
        let node_cache = Arc::new(NodeCache::new(block_cache, meta_alloc));

        // Create journal in dir
        let journal_file_path = dir.join("journal");
        let journal = Arc::new(Mutex::new(Journal::create(journal_file_path)?));

        // Create an empty InfoTree
        let infos = BTree::empty_tree(node_cache.clone())?;

        // Initialize the active devices map
        let active_devs = BTreeMap::new();

        // Initialize the snap time and next thin ID
        let snap_time = 0;
        let next_thin_id = 0;

        // Initialize the Rio instance
        // let rio = Rio::new()?;
        Ok(Pool {
            copier,
            journal,
            cache: node_cache,
            data_alloc,
            infos,
            active_devs,
            snap_time,
            next_thin_id,
            // rio,
        })
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
            let provisioned_mappings = self.provision(0, size)?;
            self.insert_mappings(&mut mappings, provisioned_mappings)?;

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

    // FIXME: we should cache the infos so we don't have to keep reading them
    fn get_mapping_tree(&self, dev: ThinID) -> Result<(ThinInfo, MappingTree)> {
        let info = self
            .infos
            .lookup(dev)?
            .ok_or_else(|| anyhow!("ThinID not found"))?;
        let mappings = MappingTree::open_tree(self.cache.clone(), info.root);

        Ok((info, mappings))
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

    //---------------------

    fn should_break_sharing(info: &ThinInfo, m: &Mapping) -> bool {
        // Was a snapshot taken since this mapping was created?
        info.snap_time > m.snap_time
    }

    // I don't think we need this
    fn break_sharing(&mut self, vbegin: VBlock, m: Mapping) -> Result<Vec<(VBlock, Mapping)>> {
        // Calculate the size of the range to be duplicated
        let size = m.e - m.b;

        // Provision a new range of physical blocks for the duplicate range
        let new_mappings = self.provision(vbegin, vbegin + size)?;
        Ok(new_mappings)
    }

    // Asynchronous method for copying data using rio
    fn copy_data(&self, src: PBlock, dst: PBlock, size: PBlock) -> Completion<()> {
        todo!();

        /*
                let src_file = todo!();
                let dst_file = todo!();
                let rio = &self.rio;
                rio.read_at(&src_file, src as u64 * 4096, size as usize * 4096)
                    .and_then(move |buf| rio.write_at(&dst_file, dst as u64 * 4096, &buf))
        */
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

    fn provision(&mut self, mut current: VBlock, end: VBlock) -> Result<Vec<(VBlock, Mapping)>> {
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
            provisioned.push((current, mapping));
            current += e - b;
        }
        Ok(provisioned)
    }

    fn insert_mappings(
        &mut self,
        mappings: &mut MappingTree,
        provisioned: Vec<(VBlock, Mapping)>,
    ) -> Result<()> {
        for (virt_block, mapping) in provisioned {
            mappings.insert(virt_block, &mapping)?;
        }
        Ok(())
    }

    // FIXME: what happens if we fail part way through?
    pub fn update_metadata(&mut self, mappings: &mut MappingTree, ops: &MetadataOps) -> Result<()> {
        for (b, e) in ops.removes() {
            self.discard_(mappings, *b, *e)?;
        }

        for (vbegin, m) in ops.inserts() {
            mappings.insert(*vbegin, m)?;
        }

        Ok(())
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

            let mut data_ops = Vec::new();
            let mut metadata_ops = MetadataOps::new();

            // Run through the mappings, adding any new provisioned areas, or breaking sharing
            let mut current = thin_begin;
            let mut provisioned = Vec::new();
            for (vbegin, m) in &ms {
                if current < *vbegin {
                    // Provision new mappings for the gap
                    let new_mappings = self.provision(current, *vbegin)?;
                    provisioned.extend(&new_mappings);

                    for (v, m) in &new_mappings {
                        data_ops.push(DataOp::Zero(ZeroOp {
                            begin: m.b,
                            end: m.e,
                        }));
                        metadata_ops.push_insert(*v, m);
                    }
                }
                provisioned.push((*vbegin, *m));
                current = m.e;
            }

            // There may be an unprovisioned area between the final mapping and the end of the
            // range.
            // FIXME: factor out common code
            if current < thin_end {
                // Provision new mappings for the remaining gap
                let new_mappings = self.provision(current, thin_end)?;
                provisioned.extend(&new_mappings);

                for (v, m) in &new_mappings {
                    data_ops.push(DataOp::Zero(ZeroOp {
                        begin: m.b,
                        end: m.e,
                    }));
                    metadata_ops.push_insert(*v, m);
                }
            }

            // Break sharing if needed
            let mut final_provisioned: Vec<(VBlock, Mapping)> = Vec::new();
            for (vbegin, m) in provisioned.iter_mut() {
                if Self::should_break_sharing(&info.clone(), m) {
                    let m_len = m.e - m.b;
                    let broken_mappings = self.provision(*vbegin, *vbegin + m_len)?;
                    metadata_ops.push_remove(*vbegin, *vbegin + m_len);
                    for (vbegin, new_m) in broken_mappings {
                        final_provisioned.push((vbegin, new_m));

                        data_ops.push(DataOp::Copy(CopyOp {
                            src_begin: vbegin,
                            src_end: vbegin + m_len,
                            dst_begin: new_m.b,
                        }));
                    }
                } else {
                    final_provisioned.push((*vbegin, *m));
                }
            }

            // Any required data ops will be completed before we start updating the metadata.  That
            // way if there's a crash there will be nothing to unroll, other than allocations which
            // can be left to the garbage collector.
            self.copier.exec(&data_ops)?;
            self.update_metadata(&mut mappings, &metadata_ops)?;
            self.update_mappings_root(id, &mut info, &mappings)?;

            Ok(final_provisioned)
        })
    }

    //---------------------

    fn discard_(
        &mut self,
        mappings: &mut MappingTree,
        thin_begin: VBlock,
        thin_end: VBlock,
    ) -> Result<()> {
        let select_above =
            mk_val_fn(move |k: Key, m: Mapping| Self::select_above(thin_begin, k, m));
        let select_below = mk_val_fn(move |k: Key, m: Mapping| Self::select_below(thin_end, k, m));

        mappings.remove_range(thin_begin, thin_end, &select_below, &select_above)?;
        Ok(())
    }

    pub fn discard(&mut self, id: ThinID, thin_begin: VBlock, thin_end: VBlock) -> Result<()> {
        let j = self.journaller();
        j.batch(|| {
            let (mut info, mut mappings) = self.get_mapping_tree(id)?;
            self.discard_(&mut mappings, thin_begin, thin_end)?;
            self.update_mappings_root(id, &mut info, &mappings)
        })
    }
}

//-------------------------------------------------------------------------
