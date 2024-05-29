use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt};

use crate::block_cache::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

#[derive(Eq, PartialEq)]
pub enum BTreeFlags {
    Internal = 0,
    Leaf = 1,
}

impl From<u32> for BTreeFlags {
    fn from(value: u32) -> Self {
        match value {
            0 => BTreeFlags::Internal,
            1 => BTreeFlags::Leaf,
            _ => panic!("Invalid value for BTreeFlags: {}", value),
        }
    }
}

// Every node implementation must start with a u32 containing the flags.
// This lets us discover if it's a leaf node or internal and instance
// the appropriate type.
pub fn read_flags(r: &[u8]) -> Result<BTreeFlags> {
    let mut r = &r[BLOCK_HEADER_SIZE..];
    let flags = r.read_u32::<LittleEndian>()?;
    Ok(BTreeFlags::from(flags))
}

//-------------------------------------------------------------------------

pub struct NodeInfo {
    pub key_min: Option<u32>,
    pub loc: MetadataBlock,
}

impl NodeInfo {
    pub fn new<V: Serializable, Data: Readable, N: NodeR<V, Data>>(node: &N) -> Self {
        let key_min = node.get_key(0);
        let loc = node.loc();
        NodeInfo { key_min, loc }
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

    fn loc(&self) -> MetadataBlock;
    fn nr_entries(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn get_key(&self, idx: usize) -> Option<u32>;
    fn get_value(&self, idx: usize) -> Option<V>;
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
