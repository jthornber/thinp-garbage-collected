use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num_enum::TryFromPrimitive;
use std::collections::{BTreeMap, VecDeque};
use std::convert::TryFrom;
use std::io::{Read, Write};
use std::path::Path;

use crate::block_cache::*;
use crate::btree::node::Key;
use crate::btree::*;
use crate::slab::*;
use crate::types::*;

//-------------------------------------------------------------------------

pub type Bytes = Vec<u8>;

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum Entry {
    AllocMetadata(u32, u32), // begin, end
    FreeMetadata(u32, u32),  // begin, end
    GrowMetadata(u32),       // nr_extra_blocks

    AllocData(PBlock, PBlock), // begin, end
    FreeData(PBlock, PBlock),  // begin, end
    GrowData(PBlock),          // nr_extra_blocks

    // FIXME: Add UpdateMappingRoot
    UpdateInfoRoot(NodePtr),

    SetSeq(MetadataBlock, SequenceNr), // Only used when rereading output log
    Zero(MetadataBlock, usize, usize), // begin, end (including node header)
    Literal(MetadataBlock, usize, Bytes), // offset, bytes
    Shadow(MetadataBlock, NodePtr),    // origin
    Overwrite(MetadataBlock, u32, Key, Bytes), // idx, k, v
    Insert(MetadataBlock, u32, Key, Bytes), // idx, k, v
    Prepend(MetadataBlock, Vec<Key>, Vec<Bytes>), // keys, values
    Append(MetadataBlock, Vec<Key>, Vec<Bytes>), // keys, values
    Erase(MetadataBlock, u32, u32),    // idx_b, idx_e
}

//-------------------------------------------------------------------------
