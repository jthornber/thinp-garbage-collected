use std::fmt;
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

#[derive(Copy, Clone, Debug)]
pub struct ZeroOp {
    pub begin: Block,
    pub end: Block,
}

//-------------------------------------

#[derive(Copy, Clone, Debug)]
pub enum DataOp {
    Copy(CopyOp),
    Zero(ZeroOp),
}

//-------------------------------------

#[derive(Error, Clone, Debug)]
pub enum IoDir {
    Read,
    Write,
}

impl fmt::Display for IoDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IoDir::Read => write!(f, "Read"),
            IoDir::Write => write!(f, "Write"),
        }
    }
}

#[derive(Error, Clone, Debug)]
pub enum CopyErr {
    #[error("errors {0:?}")]
    BadIo(Vec<(IoDir, Block)>),
}

pub type Result<T> = result::Result<T, CopyErr>;

//-------------------------------------

// The constructor for the instance should be passed the src and dst
// paths and the block size.
pub trait Copier {
    fn exec(&self, ops: &[DataOp]) -> Result<()>;
}

//-------------------------------------
