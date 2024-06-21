use std::result;
use std::sync::Arc;
use thiserror::Error;

//-------------------------------------

pub type Block = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CopyOp {
    pub src_begin: Block,
    pub src_end: Block,
    pub dst_begin: Block,
}

impl CopyOp {
    pub fn len(&self) -> Block {
        self.src_end - self.src_begin
    }
}

//-------------------------------------

pub struct ZeroOp {
    pub begin: Block,
    pub end: Block,
}

//-------------------------------------

#[derive(Error, Clone, Debug)]
pub enum CopyErr {
    #[error("Read errors {0:?}")]
    BadRead(Vec<Block>),

    #[error("Write errors {0:?}")]
    BadWrite(Vec<Block>),
}

//-------------------------------------

pub type Result<T> = result::Result<T, CopyErr>;

// The constructor for the instance should be passed the src and dst
// paths and the block size.
pub trait Copier {
    /// This copies the blocks in roughly the order given, so sort ops before
    /// submitting.
    fn copy(&mut self, ops: &[CopyOp]) -> Result<()>;
    fn zero(&mut self, ops: &[ZeroOp]) -> Result<()>;

    // FIXME: do we want to combine ops and have a single submit fn that takes a mix?
}

//-------------------------------------
