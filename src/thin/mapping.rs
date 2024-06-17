use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rio::{Completion, Rio};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use thinp::io_engine::*;

use crate::allocators::*;
use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::btree::nodes::simple::*;
use crate::btree::range_value::RangeValue;
use crate::btree::BTree;
use crate::btree::*;
use crate::copier::fake::*;
use crate::copier::*;
use crate::core::*;
use crate::journal::batch;
use crate::journal::entry::*;
use crate::journal::*;
use crate::packed_array::*;
use crate::types::*;

//-------------------------------------------------------------------------

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug)]
pub struct Mapping {
    pub b: PBlock,
    pub e: PBlock,
    pub snap_time: u32,
}

impl Mapping {
    pub fn len(&self) -> PBlock {
        self.e - self.b
    }
}

impl RangeValue for Mapping {
    fn select_geq(&self, k_old: Key, k_new: Key) -> Option<(Key, Self)> {
        let len = self.e - self.b;
        if k_old + len > k_new {
            if k_old >= k_new {
                Some((k_old, self.clone()))
            } else {
                let delta = k_new - k_old;
                Some((
                    k_new,
                    Mapping {
                        b: self.b + delta,
                        e: self.e,
                        snap_time: self.snap_time,
                    },
                ))
            }
        } else {
            None
        }
    }

    fn select_lt(&self, k_old: Key, k_new: Key) -> Option<(Key, Self)> {
        if k_old < k_new {
            Some((
                k_old,
                Mapping {
                    b: self.b,
                    e: self.e.min(k_new),
                    snap_time: self.snap_time,
                },
            ))
        } else {
            None
        }
    }

    fn merge(&self, rhs: &Self) -> Option<Self> {
        if self.e == rhs.b && self.snap_time == rhs.snap_time {
            Some(Mapping {
                b: self.b,
                e: rhs.e,
                snap_time: self.snap_time,
            })
        } else {
            None
        }
    }
}

impl Serializable for Mapping {
    fn packed_len() -> usize {
        8 + 8 + 4
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.b)?;
        w.write_u64::<LittleEndian>(self.e)?;
        w.write_u32::<LittleEndian>(self.snap_time)?;
        Ok(())
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let b = r.read_u64::<LittleEndian>()?;
        let e = r.read_u64::<LittleEndian>()?;
        let snap_time = r.read_u32::<LittleEndian>()?;

        Ok(Self { b, e, snap_time })
    }
}

pub type MappingTree = BTree<
    Mapping,
    SimpleNode<NodePtr, SharedProxy>,
    SimpleNode<NodePtr, ExclusiveProxy>,
    SimpleNode<Mapping, SharedProxy>,
    SimpleNode<Mapping, ExclusiveProxy>,
>;

//-------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_mapping(b: PBlock, e: PBlock) -> Mapping {
        Mapping { b, e, snap_time: 0 }
    }

    #[test]
    fn test_select_above() {
        let tests = vec![
            // Case 1: Mapping is entirely above key_begin
            (mk_mapping(0, 100), 0, Some((0, mk_mapping(0, 100)))),
            // Case 2: Mapping starts below and ends above key_begin
            (mk_mapping(0, 100), 50, Some((50, mk_mapping(50, 100)))),
            // Case 3: Mapping is entirely below key_begin
            (mk_mapping(0, 50), 100, None),
            // Case 4: Mapping starts exactly at key_begin
            (mk_mapping(100, 200), 100, Some((100, mk_mapping(100, 200)))),
            // Case 5: Mapping starts above key_begin
            (mk_mapping(150, 250), 100, Some((150, mk_mapping(150, 250)))),
            // Case 6: Mapping starts below and ends exactly at key_begin
            (mk_mapping(50, 100), 100, None),
            // Case 7: Mapping starts below and ends just above key_begin
            (mk_mapping(50, 101), 100, Some((100, mk_mapping(100, 101)))),
            // Case 8: Mapping starts exactly at key_begin and ends just above
            (mk_mapping(100, 101), 100, Some((100, mk_mapping(100, 101)))),
        ];

        for t in tests {
            let r = t.0.select_geq(t.0.b, t.1);
            assert_eq!(r, t.2);
        }
    }

    #[test]
    fn test_select_below() {
        let tests = vec![
            // Case 1: Mapping is entirely below key_end
            (mk_mapping(0, 100), 200, Some((0, mk_mapping(0, 100)))),
            // Case 2: Mapping starts below and ends above key_end
            (mk_mapping(0, 100), 50, Some((0, mk_mapping(0, 50)))),
            // Case 3: Mapping is entirely above key_end
            (mk_mapping(100, 200), 50, None),
            // Case 4: Mapping starts exactly at key_end
            (mk_mapping(100, 200), 100, None),
            // Case 5: Mapping starts below key_end
            (mk_mapping(50, 150), 100, Some((50, mk_mapping(50, 100)))),
            // Case 6: Mapping starts below and ends exactly at key_end
            (mk_mapping(50, 100), 100, Some((50, mk_mapping(50, 100)))),
            // Case 7: Mapping starts below and ends just above key_end
            (mk_mapping(50, 101), 100, Some((50, mk_mapping(50, 100)))),
            // Case 8: Mapping starts exactly at key_end and ends just above
            (mk_mapping(100, 101), 100, None),
        ];
        for t in tests {
            let r = t.0.select_lt(t.0.b, t.1);
            assert_eq!(r, t.2);
        }
    }
}

//-------------------------------------------------------------------------
