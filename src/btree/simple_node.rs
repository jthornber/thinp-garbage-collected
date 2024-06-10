use anyhow::Result;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

pub const SIMPLE_NODE_KIND: u32 = 0;

#[allow(dead_code)]
pub struct SimpleNode<V: Serializable, Data: Readable> {
    // We cache a copy of the loc because the underlying proxy isn't available.
    // This doesn't get written to disk.
    pub loc: u32,

    pub kind: U32<Data>,
    pub seq_nr: U32<Data>,
    pub flags: U32<Data>,
    pub nr_entries: U32<Data>,

    pub keys: PArray<u32, Data>,
    pub values: PArray<V, Data>,
}

impl<V: Serializable, Data: Readable> SimpleNode<V, Data> {
    pub fn new(loc: u32, data: Data) -> Self {
        let (kind, data) = data.split_at(4);
        let (seq_nr, data) = data.split_at(4);
        let (flags, data) = data.split_at(4);
        let (nr_entries, data) = data.split_at(4);
        let (keys, values) = data.split_at(Self::max_entries() * std::mem::size_of::<u32>());

        let kind = U32::new(kind);
        let seq_nr = U32::new(seq_nr);
        let flags = U32::new(flags);
        let nr_entries = U32::new(nr_entries);
        let keys = PArray::new(keys, nr_entries.get() as usize);
        let values = PArray::new(values, nr_entries.get() as usize);

        Self {
            loc,
            kind,
            seq_nr,
            flags,
            nr_entries,
            keys,
            values,
        }
    }

    fn max_entries() -> usize {
        (NODE_SIZE - NODE_HEADER_SIZE) / (std::mem::size_of::<u32>() + std::mem::size_of::<V>())
    }

    pub fn has_space(&self, count: usize) -> bool {
        self.nr_entries.get() as usize + count <= Self::max_entries()
    }
}

impl<V: Serializable, Data: Readable> NodeR<V, Data> for SimpleNode<V, Data> {
    fn open(loc: MetadataBlock, data: Data) -> Result<Self> {
        Ok(Self::new(loc, data))
    }

    fn n_ptr(&self) -> NodePtr {
        NodePtr {
            loc: self.loc,
            seq_nr: self.seq_nr.get(),
        }
    }

    fn nr_entries(&self) -> usize {
        self.nr_entries.get() as usize
    }

    fn is_empty(&self) -> bool {
        self.nr_entries() == 0
    }

    fn get_key(&self, idx: usize) -> u32 {
        self.keys.get(idx)
    }

    fn get_key_safe(&self, idx: usize) -> Option<u32> {
        self.keys.get_checked(idx)
    }

    fn get_value(&self, idx: usize) -> V {
        self.values.get(idx)
    }

    fn get_value_safe(&self, idx: usize) -> Option<V> {
        self.values.get_checked(idx)
    }

    fn lower_bound(&self, key: u32) -> isize {
        self.keys.bsearch(&key)
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

impl<V: Serializable, Data: Writeable> NodeW<V, Data> for SimpleNode<V, Data> {
    fn init(loc: MetadataBlock, mut data: Data, is_leaf: bool) -> Result<()> {
        // initialise the block
        let mut w = std::io::Cursor::new(data.rw());
        let hdr = NodeHeader {
            kind: SIMPLE_NODE_KIND,
            seq_nr: 0,
            flags: if is_leaf {
                BTreeFlags::Leaf
            } else {
                BTreeFlags::Internal
            },
            nr_entries: 0,
        };

        write_node_header(&mut w, &hdr)?;

        Ok(())
    }

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
