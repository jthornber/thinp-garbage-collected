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
use crate::slab::*;
use crate::types::*;

//-------------------------------------------------------------------------

#[derive(Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
enum Tag {
    AllocMetadata,
    FreeMetadata,
    GrowMetadata,

    AllocData,
    FreeData,
    GrowData,

    UpdateInfoRoot,

    SetSeq,
    Zero,
    Literal,
    Shadow,
    Overwrite,
    Insert,
    Prepend,
    Append,
    Erase,
}

fn pack_tag<W: Write>(w: &mut W, tag: Tag) -> Result<()> {
    w.write_u8(tag as u8)?;
    Ok(())
}

fn unpack_tag<R: Read>(r: &mut R) -> Result<Tag> {
    let b = r.read_u8()?;
    let tag = Tag::try_from(b)?;
    Ok(tag)
}

fn pack_bytes<W: Write>(w: &mut W, bytes: &[u8]) -> Result<()> {
    w.write_u16::<LittleEndian>(bytes.len() as u16)?;
    w.write_all(bytes)?;
    Ok(())
}

fn unpack_bytes<R: Read>(r: &mut R) -> Result<Bytes> {
    let len = r.read_u16::<LittleEndian>()? as usize;
    let mut buffer = vec![0; len];
    r.read_exact(&mut buffer)?;
    Ok(buffer)
}

fn pack_begin_end_32<W: Write>(w: &mut W, begin: u32, end: u32) -> Result<()> {
    w.write_u32::<LittleEndian>(begin)?;
    w.write_u32::<LittleEndian>(end)?;
    Ok(())
}

fn pack_begin_end<W: Write>(w: &mut W, begin: VBlock, end: VBlock) -> Result<()> {
    w.write_u64::<LittleEndian>(begin)?;
    w.write_u64::<LittleEndian>(end)?;
    Ok(())
}

fn unpack_begin_end_32<R: Read>(r: &mut R) -> Result<(u32, u32)> {
    let b = r.read_u32::<LittleEndian>()?;
    let e = r.read_u32::<LittleEndian>()?;
    Ok((b, e))
}

fn unpack_begin_end<R: Read>(r: &mut R) -> Result<(u64, u64)> {
    let b = r.read_u64::<LittleEndian>()?;
    let e = r.read_u64::<LittleEndian>()?;
    Ok((b, e))
}

fn pack_op<W: Write>(w: &mut W, op: &Entry) -> Result<()> {
    use Entry::*;

    match op {
        AllocMetadata(b, e) => {
            pack_tag(w, Tag::AllocMetadata)?;
            pack_begin_end_32(w, *b, *e)?;
        }
        FreeMetadata(b, e) => {
            pack_tag(w, Tag::FreeMetadata)?;
            pack_begin_end_32(w, *b, *e)?;
        }
        GrowMetadata(extra) => {
            pack_tag(w, Tag::GrowMetadata)?;
            w.write_u32::<LittleEndian>(*extra)?;
        }

        AllocData(b, e) => {
            pack_tag(w, Tag::AllocData)?;
            pack_begin_end(w, *b, *e)?;
        }
        FreeData(b, e) => {
            pack_tag(w, Tag::FreeData)?;
            pack_begin_end(w, *b, *e)?;
        }
        GrowData(extra) => {
            pack_tag(w, Tag::GrowData)?;
            w.write_u64::<LittleEndian>(*extra)?;
        }

        UpdateInfoRoot(root) => {
            pack_tag(w, Tag::UpdateInfoRoot)?;
            w.write_u32::<LittleEndian>(root.loc)?;
            w.write_u32::<LittleEndian>(root.seq_nr)?;
        }

        SetSeq(loc, seq) => {
            pack_tag(w, Tag::SetSeq)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u32::<LittleEndian>(*seq)?;
        }
        Zero(loc, begin, end) => {
            pack_tag(w, Tag::Zero)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(*begin as u16)?;
            w.write_u16::<LittleEndian>(*end as u16)?;
        }
        Literal(loc, offset, bytes) => {
            pack_tag(w, Tag::Literal)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(*offset as u16)?;
            pack_bytes(w, bytes)?;
        }
        Shadow(loc, origin) => {
            pack_tag(w, Tag::Shadow)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u32::<LittleEndian>(origin.loc)?;
            w.write_u32::<LittleEndian>(origin.seq_nr)?;
        }
        Overwrite(loc, idx, k, v) => {
            pack_tag(w, Tag::Overwrite)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(*idx as u16)?;
            w.write_u64::<LittleEndian>(*k)?;
            pack_bytes(w, v)?;
        }
        Insert(loc, idx, k, v) => {
            pack_tag(w, Tag::Insert)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(*idx as u16)?;
            w.write_u64::<LittleEndian>(*k)?;
            pack_bytes(w, v)?;
        }
        Prepend(loc, keys, values) => {
            assert!(keys.len() == values.len());

            pack_tag(w, Tag::Prepend)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(keys.len() as u16)?;
            for (k, v) in keys.iter().zip(values.iter()) {
                w.write_u64::<LittleEndian>(*k)?;
                pack_bytes(w, v)?;
            }
        }
        Append(loc, keys, values) => {
            assert!(keys.len() == values.len());

            pack_tag(w, Tag::Prepend)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(keys.len() as u16)?;
            for (k, v) in keys.iter().zip(values.iter()) {
                w.write_u64::<LittleEndian>(*k)?;
                pack_bytes(w, v)?;
            }
        }
        Erase(loc, idx_b, idx_e) => {
            pack_tag(w, Tag::Erase)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(*idx_b as u16)?;
            w.write_u16::<LittleEndian>(*idx_e as u16)?;
        }
    }

    Ok(())
}

fn unpack_op<R: Read>(r: &mut R) -> Result<Entry> {
    use Entry::*;
    let tag = unpack_tag(r)?;
    match tag {
        Tag::AllocMetadata => {
            let (b, e) = unpack_begin_end_32(r)?;
            Ok(AllocMetadata(b, e))
        }
        Tag::FreeMetadata => {
            let (b, e) = unpack_begin_end_32(r)?;
            Ok(FreeMetadata(b, e))
        }
        Tag::GrowMetadata => {
            let extra = r.read_u32::<LittleEndian>()?;
            Ok(GrowMetadata(extra))
        }

        Tag::AllocData => {
            let (b, e) = unpack_begin_end(r)?;
            Ok(AllocData(b, e))
        }
        Tag::FreeData => {
            let (b, e) = unpack_begin_end(r)?;
            Ok(FreeData(b, e))
        }
        Tag::GrowData => {
            let extra = r.read_u64::<LittleEndian>()?;
            Ok(GrowData(extra))
        }

        Tag::UpdateInfoRoot => {
            let loc = r.read_u32::<LittleEndian>()?;
            let seq_nr = r.read_u32::<LittleEndian>()?;

            Ok(UpdateInfoRoot(NodePtr { loc, seq_nr }))
        }

        Tag::SetSeq => {
            let loc = r.read_u32::<LittleEndian>()?;
            let seq = r.read_u32::<LittleEndian>()?;
            Ok(SetSeq(loc, seq))
        }
        Tag::Zero => {
            let loc = r.read_u32::<LittleEndian>()?;
            let begin = r.read_u16::<LittleEndian>()? as usize;
            let end = r.read_u16::<LittleEndian>()? as usize;
            Ok(Zero(loc, begin, end))
        }
        Tag::Literal => {
            let loc = r.read_u32::<LittleEndian>()?;
            let offset = r.read_u16::<LittleEndian>()? as usize;
            let bytes = unpack_bytes(r)?;
            Ok(Literal(loc, offset, bytes))
        }
        Tag::Shadow => {
            let loc = r.read_u32::<LittleEndian>()?;
            let origin = r.read_u32::<LittleEndian>()?;
            let seq_nr = r.read_u32::<LittleEndian>()?;
            Ok(Shadow(
                loc,
                NodePtr {
                    loc: origin,
                    seq_nr,
                },
            ))
        }
        Tag::Overwrite => {
            let loc = r.read_u32::<LittleEndian>()?;
            let idx = r.read_u16::<LittleEndian>()? as u32;
            let k = r.read_u64::<LittleEndian>()?;
            let v = unpack_bytes(r)?;
            Ok(Overwrite(loc, idx, k, v))
        }
        Tag::Insert => {
            let loc = r.read_u32::<LittleEndian>()?;
            let idx = r.read_u16::<LittleEndian>()? as u32;
            let k = r.read_u64::<LittleEndian>()?;
            let v = unpack_bytes(r)?;
            Ok(Insert(loc, idx, k, v))
        }
        Tag::Prepend => {
            let loc = r.read_u32::<LittleEndian>()?;
            let len = r.read_u16::<LittleEndian>()? as usize;
            let mut keys = Vec::with_capacity(len);
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                keys.push(r.read_u64::<LittleEndian>()?);
                values.push(unpack_bytes(r)?);
            }
            Ok(Prepend(loc, keys, values))
        }
        Tag::Append => {
            let loc = r.read_u32::<LittleEndian>()?;
            let len = r.read_u16::<LittleEndian>()? as usize;
            let mut keys = Vec::with_capacity(len);
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                keys.push(r.read_u64::<LittleEndian>()?);
                values.push(unpack_bytes(r)?);
            }
            Ok(Append(loc, keys, values))
        }
        Tag::Erase => {
            let loc = r.read_u32::<LittleEndian>()?;
            let idx_b = r.read_u16::<LittleEndian>()? as u32;
            let idx_e = r.read_u16::<LittleEndian>()? as u32;
            Ok(Erase(loc, idx_b, idx_e))
        }
    }
}

pub fn pack_ops<W: Write>(w: &mut W, ops: &[Entry]) -> Result<()> {
    w.write_u32::<LittleEndian>(ops.len() as u32)?;
    for op in ops {
        pack_op(w, op)?;
    }
    Ok(())
}

pub fn unpack_ops<R: Read>(r: &mut R) -> Result<Vec<Entry>> {
    let nr_ops = r.read_u32::<LittleEndian>()? as usize;
    let mut ops = Vec::with_capacity(nr_ops);
    for _ in 0..nr_ops {
        let op = unpack_op(r)?;
        ops.push(op);
    }
    Ok(ops)
}

//-------------------------------------------------------------------------
