mod bits;
mod buddy_alloc;
pub mod data_alloc;
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

//-------------------------------------
