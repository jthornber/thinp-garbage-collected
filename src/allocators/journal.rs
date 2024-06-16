use std::sync::{Arc, Mutex};

use crate::allocators::bits::*;
use crate::allocators::*;
use crate::journal::{self, Entry, Journal};

//-------------------------------------

pub enum AllocKind {
    Metadata,
    Data,
}

pub struct JournalAlloc<A: Allocator> {
    kind: AllocKind,
    journal: Arc<Mutex<Journal>>,
    inner: A,
}

impl<A: Allocator> JournalAlloc<A> {
    pub fn new(inner: A, journal: Arc<Mutex<Journal>>, kind: AllocKind) -> Self {
        Self {
            kind,
            journal,
            inner,
        }
    }

    fn add_op(&self, entry: Entry) {
        let batch = journal::Batch {
            ops: vec![entry],
            completion: None,
        };

        let mut journal = self.journal.lock().unwrap();
        journal.add_batch(batch);
    }

    fn add_ops(&self, entries: Vec<Entry>) {
        let batch = journal::Batch {
            ops: entries,
            completion: None,
        };

        let mut journal = self.journal.lock().unwrap();
        journal.add_batch(batch);
    }
}

use AllocKind::*;

impl<A: Allocator> Allocator for JournalAlloc<A> {
    fn alloc_many(&mut self, nr_blocks: u64, min_order: usize) -> Result<(u64, Vec<AllocRun>)> {
        let (total, runs) = self.inner.alloc_many(nr_blocks, min_order)?;

        let mut entries = Vec::new();
        for (b, e) in &runs {
            let entry = match self.kind {
                Metadata => Entry::AllocMetadata(*b as u32, *e as u32),
                Data => Entry::AllocData(*b, *e),
            };
            entries.push(entry);
        }

        self.add_ops(entries);
        Ok((total, runs))
    }

    fn alloc(&mut self, nr_blocks: u64) -> Result<u64> {
        let b = self.inner.alloc(nr_blocks)?;

        let op = match self.kind {
            Metadata => Entry::AllocMetadata(b as u32, (b + nr_blocks) as u32),
            Data => Entry::AllocData(b, b + nr_blocks),
        };
        self.add_op(op);

        Ok(b)
    }

    fn free(&mut self, block: u64, nr_blocks: u64) -> Result<()> {
        self.inner.free(block, nr_blocks)?;

        let op = match self.kind {
            Metadata => Entry::FreeMetadata(block as u32, (block + nr_blocks) as u32),
            Data => Entry::FreeData(block, block + nr_blocks),
        };
        self.add_op(op);

        Ok(())
    }

    fn grow(&mut self, nr_extra_blocks: u64) -> Result<()> {
        self.inner.grow(nr_extra_blocks)?;

        let op = match self.kind {
            Metadata => Entry::GrowMetadata(nr_extra_blocks as u32),
            Data => Entry::GrowData(nr_extra_blocks),
        };
        self.add_op(op);

        Ok(())
    }
}

//-------------------------------------
