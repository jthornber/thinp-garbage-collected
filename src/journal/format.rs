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
use crate::journal::entry::*;
use crate::journal::pack::*;
use crate::slab::*;
use crate::types::*;

//-------------------------------------------------------------------------

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut output = "0x".to_string();
    bytes.iter().fold(output, |mut output, b| {
        let _ = write!(output, "{b:02x}");
        output
    })
}

pub fn format_op(entry: &Entry) -> String {
    use Entry::*;
    match entry {
        AllocMetadata(b, e) => format!("alm\t{}..{}", b, e),
        FreeMetadata(b, e) => format!("frm\t{}..{}", b, e),
        GrowMetadata(extra) => format!("grm\t{}", extra),

        AllocData(b, e) => format!("ald\t{}..{}", b, e),
        FreeData(b, e) => format!("frd\t{}..{}", b, e),
        GrowData(extra) => format!("grd\t{}", extra),

        UpdateInfoRoot(root) => format!("uir {}:{}", root.loc, root.seq_nr),

        SetSeq(loc, seq) => format!("seq\t{} <- {}", loc, seq),
        Zero(loc, begin, end) => format!("zero\t{}@{}..{}", loc, begin, end),
        Literal(loc, offset, bytes) => {
            format!("lit\t {}@{} {}", loc, offset, to_hex(bytes))
        }
        Shadow(loc, origin) => format!("shadow\t{:?} -> {:?}", loc, origin),
        Overwrite(loc, idx, k, v) => {
            format!("ovr\t {}[{}] <- ({}, {})", loc, idx, k, to_hex(v))
        }
        Insert(loc, idx, k, v) => format!("ins\t {}[{}] <- ({}, {})", loc, idx, k, to_hex(v)),
        Prepend(loc, keys, values) => {
            format!(
                "pre\t {} <- ({:?}, {:?})",
                loc,
                keys,
                &values.iter().map(|v| to_hex(v)).collect::<Vec<String>>()
            )
        }
        Append(loc, keys, values) => {
            format!(
                "app\t {} <- ({:?}, {:?})",
                loc,
                keys,
                &values.iter().map(|v| to_hex(v)).collect::<Vec<String>>()
            )
        }
        Erase(loc, idx_b, idx_e) => format!("era\t{}[{}..{}]", loc, idx_b, idx_e),
    }
}

//-------------------------------------------------------------------------
