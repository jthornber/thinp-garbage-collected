use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

use crate::block_cache::*;
use crate::btree::node::Key;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

pub trait RangeValue
where
    Self: Sized,
{
    fn select_geq(&self, k_old: Key, k_new: Key) -> Option<(Key, Self)>;
    fn select_lt(&self, k_old: Key, k_new: Key) -> Option<(Key, Self)>;
    fn merge(&self, rhs: &Self) -> Option<Self>;
}

//-------------------------------------------------------------------------
