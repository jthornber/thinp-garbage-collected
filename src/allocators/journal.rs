use std::sync::{Arc, Mutex};

use crate::allocators::bits::*;
use crate::allocators::*;
use crate::journal::batch;
use crate::journal::entry::*;

//-------------------------------------

pub enum AllocKind {
    Metadata,
    Data,
}

pub struct JournalAlloc<A: Allocator> {
    kind: AllocKind,
    inner: A,
}

impl<A: Allocator> JournalAlloc<A> {
    pub fn new(inner: A, kind: AllocKind) -> Self {
        Self { kind, inner }
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

        batch::add_entries(&entries);
        Ok((total, runs))
    }

    fn alloc(&mut self, nr_blocks: u64) -> Result<u64> {
        let b = self.inner.alloc(nr_blocks)?;

        let op = match self.kind {
            Metadata => Entry::AllocMetadata(b as u32, (b + nr_blocks) as u32),
            Data => Entry::AllocData(b, b + nr_blocks),
        };
        batch::add_entry(op);

        Ok(b)
    }

    fn free(&mut self, block: u64, nr_blocks: u64) -> Result<()> {
        self.inner.free(block, nr_blocks)?;

        let op = match self.kind {
            Metadata => Entry::FreeMetadata(block as u32, (block + nr_blocks) as u32),
            Data => Entry::FreeData(block, block + nr_blocks),
        };
        batch::add_entry(op);

        Ok(())
    }

    fn grow(&mut self, nr_extra_blocks: u64) -> Result<()> {
        self.inner.grow(nr_extra_blocks)?;

        let op = match self.kind {
            Metadata => Entry::GrowMetadata(nr_extra_blocks as u32),
            Data => Entry::GrowData(nr_extra_blocks),
        };
        batch::add_entry(op);

        Ok(())
    }
}

//-------------------------------------
