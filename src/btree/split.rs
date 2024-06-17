use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

use crate::block_cache::*;
use crate::btree::node::Key;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

pub type ValFn<'a, V> = Box<dyn Fn(Key, V) -> Option<(Key, V)> + 'a>;

#[allow(dead_code)]
pub fn mk_val_fn<'a, V, F>(f: F) -> ValFn<'a, V>
where
    V: Serializable,
    F: Fn(Key, V) -> Option<(Key, V)> + 'a,
{
    Box::new(f)
}

//-------------------------------------------------------------------------
