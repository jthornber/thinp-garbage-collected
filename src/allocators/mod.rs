mod bits;
mod bitset;
mod buddy_alloc;
pub mod data_alloc;
pub mod journal;
pub mod metadata_alloc;

//-------------------------------------

pub use crate::allocators::buddy_alloc::BuddyAllocator;

use std::result;
use thiserror::Error;

/// Indicates memory errors such as referencing unallocated memory.  Or bad permissions.
#[derive(Error, Clone, Debug)]
pub enum MemErr {
    #[error("Bad params {0:?}")]
    BadParams(String),

    #[error("Unable to allocate enough space")]
    OutOfSpace,

    #[error("Bad free requested {0:?}")]
    BadFree(u64),

    #[error("internal error {0:?}")]
    Internal(String),
}

pub type Result<T> = result::Result<T, MemErr>;
pub type AllocRun = (u64, u64);

pub trait Allocator {
    fn alloc_many(&mut self, nr_blocks: u64, min_order: usize) -> Result<(u64, Vec<AllocRun>)>;
    fn alloc(&mut self, nr_blocks: u64) -> Result<u64>;
    fn free(&mut self, block: u64, nr_blocks: u64) -> Result<()>;
    fn grow(&mut self, nr_extra_blocks: u64) -> Result<()>;
}

//-------------------------------------
