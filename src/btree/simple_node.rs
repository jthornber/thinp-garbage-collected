use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Write;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

pub const NODE_HEADER_SIZE: usize = 16;

// FIXME: we can pack this more
pub struct NodeHeader {
    flags: u32,
    nr_entries: u32,
    value_size: u16,
}

/// Writes the header of a node to a writer.
pub fn write_node_header<W: Write>(w: &mut W, hdr: NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.flags)?;
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;
    w.write_u16::<LittleEndian>(hdr.value_size)?;

    // Pad out to a 64bit boundary
    w.write_u16::<LittleEndian>(0)?;
    w.write_u32::<LittleEndian>(0)?;

    Ok(())
}

//-------------------------------------------------------------------------

#[allow(dead_code)]
pub struct SimpleNode<V: Serializable, Data: Readable> {
    // We cache a copy of the loc because the underlying proxy isn't available.
    // This doesn't get written to disk.
    pub loc: u32,

    pub flags: U32<Data>,
    pub nr_entries: U32<Data>,
    pub value_size: U16<Data>,

    pub keys: PArray<u32, Data>,
    pub values: PArray<V, Data>,
}

impl<V: Serializable, Data: Readable> SimpleNode<V, Data> {
    pub fn new(loc: u32, data: Data) -> Self {
        let (_, data) = data.split_at(BLOCK_HEADER_SIZE);
        let (flags, data) = data.split_at(4);
        let (nr_entries, data) = data.split_at(4);
        let (value_size, data) = data.split_at(2);
        let (_padding, data) = data.split_at(6);
        let (keys, values) = data.split_at(Self::max_entries() * std::mem::size_of::<u32>());

        let flags = U32::new(flags);
        let nr_entries = U32::new(nr_entries);
        let value_size = U16::new(value_size);
        let keys = PArray::new(keys, nr_entries.get() as usize);
        let values = PArray::new(values, nr_entries.get() as usize);

        Self {
            loc,
            flags,
            nr_entries,
            value_size,
            keys,
            values,
        }
    }

    fn max_entries() -> usize {
        (BLOCK_PAYLOAD_SIZE - NODE_HEADER_SIZE)
            / (std::mem::size_of::<u32>() + std::mem::size_of::<V>())
    }

    pub fn has_space(&self, count: usize) -> bool {
        self.nr_entries.get() as usize + count <= Self::max_entries()
    }
}

impl<V: Serializable, Data: Readable> NodeR<V> for SimpleNode<V, Data> {
    fn loc(&self) -> MetadataBlock {
        self.loc
    }

    fn nr_entries(&self) -> usize {
        self.nr_entries.get() as usize
    }

    fn is_empty(&self) -> bool {
        self.nr_entries() == 0
    }

    fn get_key(&self, idx: usize) -> Option<u32> {
        self.keys.get_checked(idx)
    }

    fn get_value(&self, idx: usize) -> Option<V> {
        self.values.get_checked(idx)
    }

    fn get_entries(&self, b_idx: usize, e_idx: usize) -> (Vec<u32>, Vec<V>) {
        (
            self.keys.get_many(b_idx, e_idx),
            self.values.get_many(b_idx, e_idx),
        )
    }

    fn get_flags(&self) -> BTreeFlags {
        BTreeFlags::from(self.flags.get())
    }
}

impl<V: Serializable, Data: Writeable> NodeW<V> for SimpleNode<V, Data> {
    fn overwrite(&mut self, idx: usize, k: u32, value: &V) -> NodeInsertOutcome {
        self.keys.set(idx, &k);
        self.values.set(idx, value);
        NodeInsertOutcome::Success
    }

    fn insert(&mut self, idx: usize, key: u32, value: &V) -> NodeInsertOutcome {
        if self.has_space(1) {
            self.keys.insert_at(idx, &key);
            self.values.insert_at(idx, value);
            self.nr_entries.inc(1);
            NodeInsertOutcome::Success
        } else {
            NodeInsertOutcome::NoSpace
        }
    }

    fn prepend(&mut self, keys: &[u32], values: &[V]) -> NodeInsertOutcome {
        if self.has_space(keys.len()) {
            self.keys.prepend_many(keys);
            self.values.prepend_many(values);
            self.nr_entries.inc(keys.len() as u32);
            NodeInsertOutcome::Success
        } else {
            NodeInsertOutcome::NoSpace
        }
    }

    fn append(&mut self, keys: &[u32], values: &[V]) -> NodeInsertOutcome {
        if self.has_space(keys.len()) {
            self.keys.append_many(keys);
            self.values.append_many(values);
            self.nr_entries.inc(keys.len() as u32);
            NodeInsertOutcome::Success
        } else {
            NodeInsertOutcome::NoSpace
        }
    }

    fn erase(&mut self, idx_b: usize, idx_e: usize) {
        self.keys.erase(idx_b, idx_e);
        self.values.erase(idx_b, idx_e);
        self.nr_entries.dec((idx_e - idx_b) as u32);
    }
}

//-------------------------------------------------------------------------

// FIXME: remove these, I don't think they add much now it's parameterised by V
// FIXME: replace with a Cow like type that defers shadowing until we really
// modify the node.
pub type RNode<V> = SimpleNode<V, ReadProxy>;
pub type WNode<V> = SimpleNode<V, WriteProxy>;

pub fn w_node<V: Serializable>(block: WriteProxy) -> WNode<V> {
    SimpleNode::new(block.loc(), block)
}

pub fn r_node<V: Serializable>(block: ReadProxy) -> RNode<V> {
    SimpleNode::new(block.loc(), block)
}

pub fn init_node<V: Serializable>(mut block: WriteProxy, is_leaf: bool) -> Result<WNode<V>> {
    let loc = block.loc();

    // initialise the block
    let mut w = std::io::Cursor::new(block.rw());
    let hdr = BlockHeader {
        loc,
        kind: BNODE_KIND,
        sum: 0,
    };
    write_block_header(&mut w, &hdr)?;

    write_node_header(
        &mut w,
        NodeHeader {
            flags: if is_leaf {
                BTreeFlags::Leaf
            } else {
                BTreeFlags::Internal
            } as u32,
            nr_entries: 0,
            value_size: V::packed_len() as u16,
        },
    )?;

    Ok(w_node(block))
}

//-------------------------------------------------------------------------
