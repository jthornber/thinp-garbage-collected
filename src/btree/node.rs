use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

use crate::block_cache::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

#[derive(Eq, PartialEq, Clone, Copy)]
pub enum BTreeFlags {
    Internal = 0,
    Leaf = 1,
}

impl From<u16> for BTreeFlags {
    fn from(value: u16) -> Self {
        match value {
            0 => BTreeFlags::Internal,
            1 => BTreeFlags::Leaf,
            _ => panic!("Invalid value for BTreeFlags: {}", value),
        }
    }
}

//-------------------------------------------------------------------------

pub const NODE_SIZE: usize = 4096;
pub const NODE_HEADER_SIZE: usize = 16;

// We have a standard node header that is the same for all
// implementations.
pub struct NodeHeader {
    pub seq_nr: u32,

    // Shadow op will trigger COW if the current time is higher than this
    pub snap_time: u32,

    pub flags: BTreeFlags,
    pub kind: u16, // eg, SimpleNode, BlockTimeNode
    pub nr_entries: u32,
}

pub fn write_node_header<W: Write>(w: &mut W, hdr: &NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.seq_nr)?;
    w.write_u32::<LittleEndian>(hdr.snap_time)?;
    w.write_u16::<LittleEndian>(hdr.flags as u16)?;
    w.write_u16::<LittleEndian>(hdr.kind)?;
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;

    Ok(())
}

pub fn read_node_header<R: Read>(r: &mut R) -> Result<NodeHeader> {
    let seq_nr = r.read_u32::<LittleEndian>()?;
    let snap_time = r.read_u32::<LittleEndian>()?;
    let flags = BTreeFlags::from(r.read_u16::<LittleEndian>()?);
    let kind = r.read_u16::<LittleEndian>()?;
    let nr_entries = r.read_u32::<LittleEndian>()?;

    Ok(NodeHeader {
        seq_nr,
        snap_time,
        flags,
        kind,
        nr_entries,
    })
}

pub fn read_flags(r_proxy: &SharedProxy) -> Result<BTreeFlags> {
    let hdr = read_node_header(&mut r_proxy.r())?;
    Ok(hdr.flags)
}

//-------------------------------------------------------------------------

pub type SequenceNr = u32;

#[derive(Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
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

pub struct NodeInfo {
    pub key_min: Option<u32>,
    pub n_ptr: NodePtr,
}

impl NodeInfo {
    pub fn new<V: Serializable, Data: Readable, N: NodeR<V, Data>>(node: &N) -> Self {
        let key_min = node.get_key_safe(0);
        let n_ptr = node.n_ptr();
        NodeInfo { key_min, n_ptr }
    }
}

// Removing a range can turn one entry into two if the range covers the
// middle of an existing entry.  So, like for insert, we have a way of
// returning more than one new block.  If a pair is returned then the
// first one corresponds to the idx of the original block.
pub enum NodeResult {
    Single(NodeInfo),
    Pair(NodeInfo, NodeInfo),
}

impl NodeResult {
    pub fn single<V: Serializable, Data: Readable, N: NodeR<V, Data>>(node: &N) -> Self {
        NodeResult::Single(NodeInfo::new(node))
    }

    pub fn pair<V: Serializable, Data: Readable, N: NodeR<V, Data>>(n1: &N, n2: &N) -> Self {
        NodeResult::Pair(NodeInfo::new(n1), NodeInfo::new(n2))
    }
}

//-------------------------------------------------------------------------

pub trait NodeR<V: Serializable, Data: Readable>: Sized {
    fn open(loc: MetadataBlock, data: Data) -> Result<Self>;

    fn n_ptr(&self) -> NodePtr;
    fn nr_entries(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn get_key(&self, idx: usize) -> u32;
    fn get_key_safe(&self, idx: usize) -> Option<u32>;
    fn get_value(&self, idx: usize) -> V;
    fn get_value_safe(&self, idx: usize) -> Option<V>;
    fn lower_bound(&self, key: u32) -> isize;

    // FIXME: make return type Option
    fn get_entries(&self, b_idx: usize, e_idx: usize) -> (Vec<u32>, Vec<V>);
    fn get_flags(&self) -> BTreeFlags;

    fn is_internal(&self) -> bool {
        self.get_flags() == BTreeFlags::Internal
    }

    fn is_leaf(&self) -> bool {
        self.get_flags() == BTreeFlags::Leaf
    }
}

// FIXME: rename SpaceOutcome?
pub enum NodeInsertOutcome {
    Success,
    NoSpace,
}

pub trait NodeW<V: Serializable, Data: Writeable>: NodeR<V, Data> {
    /// Initialises a fresh, empty node.
    fn init(loc: MetadataBlock, data: Data, is_leaf: bool) -> Result<()>;

    fn overwrite(&mut self, idx: usize, k: u32, value: &V) -> NodeInsertOutcome;
    fn insert(&mut self, idx: usize, k: u32, value: &V) -> NodeInsertOutcome;
    fn prepend(&mut self, keys: &[u32], values: &[V]) -> NodeInsertOutcome;
    fn append(&mut self, keys: &[u32], values: &[V]) -> NodeInsertOutcome;
    fn erase(&mut self, b_idx: usize, e_idx: usize);

    // fn redistribute2(&mut self, rhs: &mut Self);

    // FIXME: inconsistent naming in the next two
    fn shift_left(&mut self, count: usize) -> (Vec<u32>, Vec<V>) {
        let r = self.get_entries(0, count);
        self.erase(0, count);
        r
    }

    fn remove_right(&mut self, count: usize) -> (Vec<u32>, Vec<V>) {
        let e_idx = self.nr_entries();
        let b_idx = e_idx - count;
        let r = self.get_entries(b_idx, e_idx);
        self.erase(b_idx, e_idx);
        r
    }

    // FIXME: rename to remove()
    fn remove_at(&mut self, idx: usize) {
        self.erase(idx, idx + 1);
    }
}

//-------------------------------------------------------------------------

pub type ValFn<'a, V> = Box<dyn Fn(u32, V) -> Option<(u32, V)> + 'a>;

#[allow(dead_code)]
pub fn mk_val_fn<'a, V, F>(f: F) -> ValFn<'a, V>
where
    V: Serializable,
    F: Fn(u32, V) -> Option<(u32, V)> + 'a,
{
    Box::new(f)
}

//-------------------------------------------------------------------------
