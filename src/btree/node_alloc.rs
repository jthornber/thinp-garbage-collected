use anyhow::Result;
use std::sync::Arc;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub struct AllocContext {
    tm: Arc<TransactionManager>,
    context: ReferenceContext,
}

impl AllocContext {
    pub fn new(tm: Arc<TransactionManager>, context: ReferenceContext) -> Self {
        Self { tm, context }
    }

    pub fn new_block(&mut self) -> Result<WriteProxy> {
        self.tm.new_block(self.context, &BNODE_KIND)
    }

    pub fn is_internal(&mut self, loc: MetadataBlock) -> Result<bool> {
        let b = self.tm.read(loc, &BNODE_KIND)?;
        Ok(read_flags(b.r())? == BTreeFlags::Internal)
    }

    pub fn shadow<NV: Serializable>(&mut self, loc: MetadataBlock) -> Result<WNode<NV>> {
        Ok(w_node(self.tm.shadow(self.context, loc, &BNODE_KIND)?))
    }

    pub fn read<NV: Serializable>(&mut self, loc: MetadataBlock) -> Result<RNode<NV>> {
        Ok(r_node(self.tm.read(loc, &BNODE_KIND)?))
    }
}

//-------------------------------------------------------------------------
