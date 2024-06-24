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
use crate::btree::nodes::simple::*;
use crate::btree::transaction_manager::*;
use crate::btree::BTree;
use crate::btree::*;
use crate::copier::fake::*;
use crate::copier::*;
use crate::core::*;
use crate::journal::batch;
use crate::journal::entry::*;
use crate::journal::*;
use crate::packed_array::*;
use crate::thin::mapping::*;
use crate::types::*;

pub mod mapping;
mod tests;

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

#[derive(Default)]
struct Ops {
    zeroes: Vec<(PBlock, PBlock)>,
    copies: Vec<(PBlock, PBlock, PBlock)>,
    removes: Vec<(VBlock, VBlock)>,
    inserts: Vec<(VBlock, Mapping)>,
}

impl Ops {
    fn push_zero(&mut self, b: PBlock, e: PBlock) {
        self.zeroes.push((b, e));
    }

    fn push_copy(&mut self, src_b: PBlock, src_e: PBlock, dst_b: PBlock) {
        self.copies.push((src_b, src_e, dst_b));
    }

    fn push_insert(&mut self, vbegin: VBlock, m: &Mapping) {
        self.inserts.push((vbegin, *m));
        if let Some((vbegin, last_m)) = self.inserts.last_mut() {
            if last_m.snap_time == m.snap_time && m.b == last_m.e {
                // Merge mappings
                last_m.e = m.e;
                return;
            }
        }

        self.inserts.push((vbegin, *m));
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

    fn zeroes(&self) -> &[(PBlock, PBlock)] {
        &self.zeroes
    }

    fn copies(&self) -> &[(PBlock, PBlock, PBlock)] {
        &self.copies
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
    tm: Arc<TransactionManager>,
}

impl Journaller {
    fn new(journal: Arc<Mutex<Journal>>, tm: Arc<TransactionManager>) -> Self {
        Journaller { journal, tm }
    }

    fn batch<T, F: FnOnce() -> Result<T>>(&self, action: F) -> Result<T> {
        let batch_id = self.tm.get_batch_id();
        batch::begin_batch();
        let r = action();

        // We need to write the batch to the journal regardless since the node will
        // have been updated.
        let completion: Option<Box<dyn BatchCompletion>> =
            Some(Box::new(CacheCompletion::new(self.tm.clone())));
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
    tm: Arc<TransactionManager>,
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
        if !dir.exists() {
            return Err(anyhow::anyhow!("Directory does not exist"));
        }
        let node_file_path = Self::create_node_file(dir, nr_metadata_blocks)?;

        let copier = Arc::new(FakeCopier::new());
        let engine = Arc::new(SyncIoEngine::new(&node_file_path, true)?);
        let block_cache = Arc::new(BlockCache::new(engine, 16)?);

        let meta_alloc = BuddyAllocator::new(nr_metadata_blocks);
        let data_alloc = BuddyAllocator::new(nr_data_blocks);

        let journal = Self::create_journal(dir)?;
        let tm = Arc::new(TransactionManager::new(
            journal.clone(),
            block_cache,
            meta_alloc,
        ));
        let journaller = Journaller::new(journal.clone(), tm.clone());

        let infos = journaller.batch(|| BTree::empty_tree(tm.clone()))?;

        Ok(Pool {
            copier,
            journal,
            tm,
            data_alloc,
            infos,
            active_devs: BTreeMap::new(),
            snap_time: 0,
            next_thin_id: 0,
        })
    }

    fn create_node_file(dir: &Path, nr_metadata_blocks: u64) -> Result<PathBuf> {
        let node_file_path = dir.join("node_file");
        let node_file_size = 4096 * nr_metadata_blocks;
        let node_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&node_file_path)?;
        node_file.set_len(node_file_size)?;
        Ok(node_file_path)
    }

    fn create_journal(dir: &Path) -> Result<Arc<Mutex<Journal>>> {
        let journal_file_path = dir.join("journal");
        Ok(Arc::new(Mutex::new(Journal::create(journal_file_path)?)))
    }

    //----------------------

    pub fn open<P: AsRef<Path>>(_dir: P) -> Self {
        todo!();
    }

    //----------------------

    pub fn close(self) -> Result<()> {
        todo!()
    }

    fn new_thin_id(&mut self) -> ThinID {
        let id = self.next_thin_id;
        self.next_thin_id += 1;
        id
    }

    //----------------------

    fn update_info_root(&mut self) -> Result<()> {
        batch::add_entry(Entry::UpdateInfoRoot(self.infos.root()))?;
        Ok(())
    }

    fn journalled<T, F: FnOnce() -> Result<T>>(&self, action: F) -> Result<T> {
        let journaller = Journaller::new(self.journal.clone(), self.tm.clone());
        journaller.batch(action)
    }

    fn journaller(&self) -> Journaller {
        Journaller::new(self.journal.clone(), self.tm.clone())
    }

    pub fn create_thin_(&mut self, size: VBlock) -> Result<(ThinID, MappingTree)> {
        // Choose a new id
        let id = self.new_thin_id();

        // create new btree
        let mappings = MappingTree::empty_tree(self.tm.clone())?;

        Ok((id, mappings))
    }

    pub fn create_thin(&mut self, size: VBlock) -> Result<ThinID> {
        self.journaller().batch(|| {
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
        self.journaller().batch(|| {
            // Create a new thin
            let (id, mut mappings) = self.create_thin_(size)?;
            let mut ops = Ops::default();

            // Provision the entire range
            let _ = self.provision(0, size, &mut ops)?;

            // Add thin_info to btree
            let info = ThinInfo {
                size,
                snap_time: self.snap_time,
                root: mappings.root(),
            };
            self.exec_ops(&mut mappings, &ops)?;
            self.infos.insert(id, &info)?;
            self.update_info_root()?;

            Ok(id)
        })
    }

    pub fn create_snap(&mut self, origin: ThinID) -> Result<ThinID> {
        self.journaller().batch(|| {
            let (mut origin_info, mut origin_mappings) = self.get_mapping_tree(origin)?;
            let snap_mappings = origin_mappings.snap(self.snap_time);

            let snap_id = self.new_thin_id();
            let snap_info = ThinInfo {
                size: origin_info.size,
                snap_time: self.snap_time,
                root: snap_mappings.root(),
            };
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
        self.journaller().batch(|| {
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

    //---------------------

    // FIXME: we should cache the infos so we don't have to keep reading them
    fn get_mapping_tree(&self, dev: ThinID) -> Result<(ThinInfo, MappingTree)> {
        let info = self
            .infos
            .lookup(dev)?
            .ok_or_else(|| anyhow!("ThinID not found"))?;
        let mappings = MappingTree::open_tree(self.tm.clone(), info.root);

        Ok((info, mappings))
    }

    /*
    fn lookup_range(
        mappings: &MappingTree,
        thin_begin: VBlock,
        thin_end: VBlock,
    ) -> Result<Vec<(VBlock, Mapping)>> {
        mappings.lookup_range(thin_begin, thin_end)
    }
    */

    pub fn get_read_mapping(
        &self,
        id: ThinID,
        thin_begin: VBlock,
        thin_end: VBlock,
    ) -> Result<Vec<(VBlock, Mapping)>> {
        let (_, mappings) = self.get_mapping_tree(id)?;
        mappings.lookup_range(thin_begin, thin_end)
    }

    //---------------------

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
        begin: VBlock,
        end: VBlock,
        ops: &mut Ops,
    ) -> Result<Vec<(VBlock, Mapping)>> {
        let len = end - begin;

        let (total, runs) = self.data_alloc.alloc_many(len, 0)?;
        if total != len {
            // Not enough space, free the allocated data and return an error
            for (b, e) in runs {
                self.data_alloc.free(b, e - b)?;
            }
            return Err(anyhow!("Could not allocate enough data space"));
        }

        let mut result = Vec::new();
        let mut current = begin;
        for (b, e) in runs {
            ops.push_zero(b, e);

            let mapping = Mapping {
                b,
                e,
                snap_time: self.snap_time,
            };
            result.push((current, mapping));
            ops.push_insert(current, &mapping);
            current += e - b;
        }

        Ok(result)
    }

    fn should_break_sharing(info: &ThinInfo, m: &Mapping) -> bool {
        // Was a snapshot taken since this mapping was created?
        info.snap_time > m.snap_time
    }

    fn break_sharing(
        &mut self,
        begin: VBlock,
        end: VBlock,
        ops: &mut Ops,
    ) -> Result<Vec<(VBlock, Mapping)>> {
        ops.push_remove(begin, end);

        let len = end - begin;
        let (total, runs) = self.data_alloc.alloc_many(len, 0)?;
        if total != len {
            // Not enough space, free the allocated data and return an error
            for (b, e) in runs {
                self.data_alloc.free(b, e - b)?;
            }
            return Err(anyhow!("Could not allocate enough data space"));
        }

        let mut result = Vec::new();
        let mut current = begin;
        for (b, e) in runs {
            ops.push_copy(current, current + (e - b), b);

            let mapping = Mapping {
                b,
                e,
                snap_time: self.snap_time,
            };
            result.push((current, mapping));
            ops.push_insert(current, &mapping);
            current += e - b;
        }

        Ok(result)
    }

    // Any required data ops will be completed before we start updating the metadata.  That
    // way if there's a crash there will be nothing to unroll, other than allocations which
    // can be left to the garbage collector.
    fn exec_ops(&mut self, mappings: &mut MappingTree, ops: &Ops) -> Result<()> {
        let mut data_ops = Vec::new();

        // build zero ops
        for (b, e) in ops.zeroes() {
            data_ops.push(DataOp::Zero(ZeroOp { begin: *b, end: *e }));
        }

        // build copy ops
        for (src_begin, src_end, dst_begin) in ops.copies() {
            data_ops.push(DataOp::Copy(CopyOp {
                src_begin: *src_begin,
                src_end: *src_end,
                dst_begin: *dst_begin,
            }));
        }
        self.copier.exec(&data_ops)?;

        for (b, e) in ops.removes() {
            mappings.remove_range(*b, *e)?;
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
        let (mut info, mut mappings) = self.get_mapping_tree(id)?;
        let mappings_in_range = mappings.lookup_range(thin_begin, thin_end)?;

        self.journaller().batch(|| {
            let mut ops = Ops::default();
            let mut current = thin_begin;
            let mut result = Vec::new();

            // Closure to process mappings and gaps
            let mut process_mapping = |vbegin: VBlock, m: Option<&Mapping>| -> Result<()> {
                if current < vbegin {
                    result.extend(self.provision(current, vbegin, &mut ops)?);
                }

                if let Some(m) = m {
                    if Self::should_break_sharing(&info, m) {
                        let len = m.e - m.b;
                        result.extend(self.break_sharing(vbegin, vbegin + len, &mut ops)?);
                    } else {
                        result.push((vbegin, *m));
                    }
                }

                current = vbegin;
                Ok(())
            };

            // Process all mappings in the range
            for (vbegin, m) in &mappings_in_range {
                process_mapping(*vbegin, Some(m))?;
            }

            // Handle any trailing gap
            process_mapping(thin_end, None)?;

            // Finalize operations
            self.exec_ops(&mut mappings, &ops)?;
            self.update_mappings_root(id, &mut info, &mappings)?;

            Ok(result)
        })
    }

    //---------------------

    pub fn discard(&mut self, id: ThinID, thin_begin: VBlock, thin_end: VBlock) -> Result<()> {
        self.journaller().batch(|| {
            let (mut info, mut mappings) = self.get_mapping_tree(id)?;
            mappings.remove_range(thin_begin, thin_end)?;
            self.update_mappings_root(id, &mut info, &mappings)
        })
    }

    //---------------------

    fn flush(&mut self, id: ThinID) -> Result<()> {
        // find the latest cache pinning id and wait for it to hit the disk
        todo!();
    }
}

//-------------------------------------------------------------------------
