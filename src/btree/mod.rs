use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};
use std::sync::Arc;

use crate::block_cache::MetadataBlock;
use crate::btree::node_cache::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

pub type SequenceNr = u32;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct NodePtr {
    pub loc: MetadataBlock,
    pub seq_nr: SequenceNr,
}

impl Serializable for NodePtr {
    fn packed_len() -> usize {
        8
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u32::<LittleEndian>(self.loc)?;
        w.write_u32::<LittleEndian>(self.seq_nr)?;
        Ok(())
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let loc = r.read_u32::<LittleEndian>()?;
        let seq_nr = r.read_u32::<LittleEndian>()?;
        Ok(NodePtr { loc, seq_nr })
    }
}

//-------------------------------------------------------------------------

pub struct BTree<V: Serializable + Copy, INodeR, INodeW, LNodeR, LNodeW> {
    cache: Arc<NodeCache>,
    root: NodePtr,
    snap_time: u32,

    phantom_v: std::marker::PhantomData<V>,
    phantom_inode_r: std::marker::PhantomData<INodeR>,
    phantom_inode_w: std::marker::PhantomData<INodeW>,
    phantom_lnode_r: std::marker::PhantomData<LNodeR>,
    phantom_lnode_w: std::marker::PhantomData<LNodeW>,
}

mod check;
mod core;
mod insert;
mod lookup;
pub mod node;
pub mod node_cache;
pub mod nodes;
mod remove;
mod tests;

//-------------------------------------------------------------------------
