pub mod entry;
mod format;
mod pack;

//-------------------------------------------------------------------------

use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num_enum::TryFromPrimitive;
use std::collections::{BTreeMap, VecDeque};
use std::convert::TryFrom;
use std::io::{Read, Write};
use std::path::Path;

use crate::block_cache::*;
use crate::btree::node::Key;
use crate::btree::*;
use crate::journal::entry::*;
use crate::journal::format::*;
use crate::journal::pack::*;
use crate::slab::*;
use crate::types::*;

//-------------------------------------------------------------------------

/// Call backs made when a batch of entries have all hit the disk.
// This could be just a FnOnce, but I suspect we'll add other methods in here.
pub trait BatchCompletion {
    fn complete(&self);
}

pub struct Batch {
    pub ops: Vec<Entry>,
    pub completion: Option<Box<dyn BatchCompletion>>,
}

pub struct Journal {
    slab: SlabFile,
    batches: Vec<Batch>,
    seqs: BTreeMap<MetadataBlock, SequenceNr>,
}

impl Drop for Journal {
    fn drop(&mut self) {
        self.sync().unwrap();
        self.slab.close();
    }
}

impl Journal {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let slab = SlabFileBuilder::create(path)
            .read(true)
            .write(true)
            .compressed(true)
            .cache_nr_entries(16)
            .queue_depth(4)
            .build()?;

        Ok(Self {
            slab,
            batches: Vec::new(),
            seqs: BTreeMap::new(),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, write: bool) -> Result<Self> {
        let slab = SlabFileBuilder::open(path)
            .read(true)
            .write(write)
            .cache_nr_entries(16)
            .queue_depth(4)
            .build()?;

        Ok(Self {
            slab,
            batches: Vec::new(),
            seqs: BTreeMap::new(),
        })
    }

    pub fn add_batch(&mut self, batch: Batch) {
        self.batches.push(batch)
    }

    pub fn sync(&mut self) -> Result<()> {
        // hack
        if self.batches.is_empty() {
            return Ok(());
        }

        let mut batches: Vec<Batch> = Vec::new();

        std::mem::swap(&mut batches, &mut self.batches);

        let mut w: Vec<u8> = Vec::new();
        for b in &batches {
            pack_ops(&mut w, &b.ops)?;
        }

        // FIXME: use rio
        self.slab.write_slab(&w)?;

        for b in batches {
            if let Some(completion) = b.completion {
                completion.complete();
            }
        }

        Ok(())
    }

    pub fn up_to_date(&mut self, n: &NodePtr) -> Result<bool> {
        if let Some(seq) = self.seqs.get(&n.loc) {
            if n.seq_nr == *seq {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(anyhow!("no sequence nr for {}", n.loc))
        }
    }

    pub fn get_ops(
        &mut self,
        _loc: MetadataBlock,
        _seq_old: SequenceNr,
        _seq_new: SequenceNr,
    ) -> Result<Vec<Entry>> {
        todo!()
    }

    pub fn dump<W: Write>(&mut self, out: &mut W) -> Result<()> {
        for s in 0..self.slab.get_nr_slabs() {
            let mut bytes = self.slab.read(s as u32)?;
            let mut r = std::io::Cursor::new(bytes.as_ref());
            let ops = unpack_ops(&mut r)?;
            for op in &ops {
                writeln!(out, "    {}", format_op(op))?;
            }
        }
        Ok(())
    }
}

//-------------------------------------------------------------------------
