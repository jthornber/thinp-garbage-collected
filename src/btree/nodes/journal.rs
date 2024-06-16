use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::block_cache::MetadataBlock;
use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::byte_types::*;
use crate::journal::Entry;
use crate::packed_array::*;

//-------------------------------------------------------------------------

fn to_bytes<V: Serializable>(v: &V) -> Bytes {
    let mut w = Vec::new();
    v.pack(&mut w).unwrap();
    w
}

fn to_bytes_many<V: Serializable>(values: &[V]) -> Bytes {
    let mut w = Vec::new();
    for v in values {
        v.pack(&mut w).unwrap();
    }
    w
}

//-------------------------------------------------------------------------

pub struct JournalNode<N, V, Data> {
    cache: Arc<Mutex<NodeCache>>,
    node: N,
    phantom_v: std::marker::PhantomData<V>,
    phantom_data: std::marker::PhantomData<Data>,
}

impl<N, V, Data> JournalNode<N, V, Data> {
    pub fn new(cache: Arc<Mutex<NodeCache>>, node: N) -> Self {
        Self {
            cache,
            node,
            phantom_v: std::marker::PhantomData,
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<N, V, Data> JournalNode<N, V, Data>
where
    N: NodeR<V, Data>,
    V: Serializable,
    Data: Readable,
{
    // FIXME: I'd like to return  Result<()> from here, but most of the node ops
    // are assumed to be unable to fail.  Revisit.
    pub fn add_op(&mut self, op: Entry) {
        self.cache.lock().unwrap().add_journal_op(op).unwrap()
    }
}

impl<N, V, Data> NodeR<V, Data> for JournalNode<N, V, Data>
where
    N: NodeR<V, Data>,
    V: Serializable,
    Data: Readable,
{
    fn open(loc: MetadataBlock, data: Data) -> Result<Self> {
        unreachable!();
    }

    fn n_ptr(&self) -> NodePtr {
        self.node.n_ptr()
    }

    fn nr_entries(&self) -> usize {
        self.node.nr_entries()
    }

    fn is_empty(&self) -> bool {
        self.node.is_empty()
    }

    fn get_key(&self, idx: usize) -> Key {
        self.node.get_key(idx)
    }

    fn get_key_safe(&self, idx: usize) -> Option<Key> {
        self.node.get_key_safe(idx)
    }

    fn get_value(&self, idx: usize) -> V {
        self.node.get_value(idx)
    }

    fn get_value_safe(&self, idx: usize) -> Option<V> {
        self.node.get_value_safe(idx)
    }

    fn lower_bound(&self, key: Key) -> isize {
        self.node.lower_bound(key)
    }

    fn get_entries(&self, b_idx: usize, e_idx: usize) -> (Vec<Key>, Vec<V>) {
        self.node.get_entries(b_idx, e_idx)
    }

    fn get_flags(&self) -> BTreeFlags {
        self.node.get_flags()
    }
}

impl<N, V, Data> NodeW<V, Data> for JournalNode<N, V, Data>
where
    N: NodeW<V, Data>,
    V: Serializable,
    Data: Writeable,
{
    fn init(loc: MetadataBlock, data: Data, is_leaf: bool) -> Result<()> {
        // FIXME: write to journal
        N::init(loc, data, is_leaf)
    }

    fn overwrite(&mut self, idx: usize, k: Key, value: &V) -> NodeInsertOutcome {
        let loc = self.node.n_ptr().loc;
        let op = Entry::Overwrite(loc, idx as u32, k, to_bytes(value));
        self.add_op(op);
        self.node.overwrite(idx, k, value)
    }

    fn insert(&mut self, idx: usize, k: Key, value: &V) -> NodeInsertOutcome {
        let loc = self.node.n_ptr().loc;
        let op = Entry::Insert(loc, idx as u32, k, to_bytes(value));
        self.add_op(op);
        self.node.insert(idx, k, value)
    }

    fn prepend(&mut self, keys: &[Key], values: &[V]) -> NodeInsertOutcome {
        let loc = self.node.n_ptr().loc;
        let serialized_values = values.iter().map(|v| to_bytes(v)).collect();
        let op = Entry::Prepend(loc, keys.to_vec(), serialized_values);
        self.add_op(op);
        self.node.prepend(keys, values)
    }

    fn append(&mut self, keys: &[Key], values: &[V]) -> NodeInsertOutcome {
        let loc = self.node.n_ptr().loc;
        let serialized_values = values.iter().map(|v| to_bytes(v)).collect();
        let op = Entry::Append(loc, keys.to_vec(), serialized_values);
        self.add_op(op);
        self.node.append(keys, values)
    }

    fn erase(&mut self, b_idx: usize, e_idx: usize) {
        let loc = self.node.n_ptr().loc;
        let op = Entry::Erase(loc, b_idx as u32, e_idx as u32);
        self.add_op(op);
        self.node.erase(b_idx, e_idx)
    }
}

//-------------------------------------------------------------------------
