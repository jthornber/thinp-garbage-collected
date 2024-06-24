use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::allocators::journal::*;
use crate::allocators::*;
use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::nodes::journal::*;
use crate::byte_types::*;
use crate::journal::BatchCompletion;
use crate::packed_array::*;

//----------------------------------------------------------------

trait NodeFactory {
    fn create(&self, data: ExclusiveProxy) -> Result<Box<dyn ReplayableNode>>;
}

struct NodeRegistry {
    factories: BTreeMap<u16, Box<dyn NodeFactory>>,
}

impl NodeRegistry {
    fn register(&mut self, kind: u16, factory: Box<dyn NodeFactory>) {
        self.factories.insert(kind, factory);
    }

    fn open_node(&self, kind: u16, data: ExclusiveProxy) -> Result<Box<dyn ReplayableNode>> {
        self.factories[&kind].create(data)
    }
}

//----------------------------------------------------------------
