use std::sync::Arc;

use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::packed_array::*;

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

mod btree;
mod check;
mod insert;
mod lookup;
pub mod node;
mod node_cache;
mod remove;
mod simple_node;
mod tests;

//-------------------------------------------------------------------------
