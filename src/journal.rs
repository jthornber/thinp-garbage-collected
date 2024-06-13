use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num_enum::TryFromPrimitive;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::io::{Read, Write};
use std::path::Path;

use crate::block_cache::*;
use crate::btree::*;
use crate::slab::*;
use crate::types::*;

//-------------------------------------------------------------------------

pub type Bytes = Vec<u8>;

// FIXME: we need to journal ops that are not specific to a node.  eg,
// allocating a data range.
/// Operations that can be performed on a node.
#[derive(Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum Entry {
    AllocMetadata(VBlock, VBlock), // begin, end
    FreeMetadata(VBlock, VBlock),  // begin, end

    AllocData(PBlock, PBlock), // begin, end
    FreeData(VBlock, VBlock),  // begin, end

    NewDev(ThinID, VBlock, MetadataBlock), // id, size, id, root
    NewRoot(ThinID, MetadataBlock),
    DelDev(ThinID),

    SetSeq(MetadataBlock, SequenceNr), // Only used when rereading output log
    Zero(MetadataBlock, usize, usize), // begin, end (including node header)
    Literal(MetadataBlock, usize, Bytes), // offset, bytes
    Shadow(MetadataBlock, NodePtr),    // origin
    Overwrite(MetadataBlock, u32, u32, Bytes), // idx, k, v
    Insert(MetadataBlock, u32, u32, Bytes), // idx, k, v
    Prepend(MetadataBlock, Vec<u32>, Vec<Bytes>), // keys, values
    Append(MetadataBlock, Vec<u32>, Vec<Bytes>), // keys, values
    Erase(MetadataBlock, u32, u32),    // idx_b, idx_e
}

#[derive(Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
enum Tag {
    AllocMetadata,
    AllocData,

    FreeMetadata,
    FreeData,

    NewDev,
    NewRoot,
    DelDev,

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

fn pack_begin_end<W: Write>(w: &mut W, begin: VBlock, end: VBlock) -> Result<()> {
    w.write_u64::<LittleEndian>(begin)?;
    w.write_u64::<LittleEndian>(end)?;
    Ok(())
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
            pack_begin_end(w, *b, *e)?;
        }
        AllocData(b, e) => {
            pack_tag(w, Tag::AllocData)?;
            pack_begin_end(w, *b, *e)?;
        }

        FreeData(b, e) => {
            pack_tag(w, Tag::FreeData)?;
            pack_begin_end(w, *b, *e)?;
        }
        FreeMetadata(b, e) => {
            pack_tag(w, Tag::FreeMetadata)?;
            pack_begin_end(w, *b, *e)?;
        }
        NewDev(id, size, root) => {
            pack_tag(w, Tag::NewDev)?;
            w.write_u64::<LittleEndian>(*id)?;
            w.write_u64::<LittleEndian>(*size)?;
            w.write_u32::<LittleEndian>(*root)?;
        }
        NewRoot(id, root) => {
            pack_tag(w, Tag::NewRoot)?;
            w.write_u64::<LittleEndian>(*id)?;
            w.write_u32::<LittleEndian>(*root)?;
        }
        DelDev(id) => {
            pack_tag(w, Tag::DelDev)?;
            w.write_u64::<LittleEndian>(*id)?;
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
            w.write_u32::<LittleEndian>(*k)?;
            pack_bytes(w, v)?;
        }
        Insert(loc, idx, k, v) => {
            pack_tag(w, Tag::Insert)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(*idx as u16)?;
            w.write_u32::<LittleEndian>(*k)?;
            pack_bytes(w, v)?;
        }
        Prepend(loc, keys, values) => {
            assert!(keys.len() == values.len());

            pack_tag(w, Tag::Prepend)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(keys.len() as u16)?;
            for (k, v) in keys.iter().zip(values.iter()) {
                w.write_u32::<LittleEndian>(*k)?;
                pack_bytes(w, v)?;
            }
        }
        Append(loc, keys, values) => {
            assert!(keys.len() == values.len());

            pack_tag(w, Tag::Prepend)?;
            w.write_u32::<LittleEndian>(*loc)?;
            w.write_u16::<LittleEndian>(keys.len() as u16)?;
            for (k, v) in keys.iter().zip(values.iter()) {
                w.write_u32::<LittleEndian>(*k)?;
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
            let (b, e) = unpack_begin_end(r)?;
            Ok(AllocMetadata(b, e))
        }
        Tag::FreeMetadata => {
            let (b, e) = unpack_begin_end(r)?;
            Ok(FreeMetadata(b, e))
        }

        Tag::AllocData => {
            let (b, e) = unpack_begin_end(r)?;
            Ok(AllocData(b, e))
        }
        Tag::FreeData => {
            let (b, e) = unpack_begin_end(r)?;
            Ok(FreeData(b, e))
        }

        Tag::NewDev => {
            let id = r.read_u64::<LittleEndian>()?;
            let size = r.read_u64::<LittleEndian>()?;
            let root = r.read_u32::<LittleEndian>()?;
            Ok(NewDev(id, size, root))
        }
        Tag::NewRoot => {
            let id = r.read_u64::<LittleEndian>()?;
            let root = r.read_u32::<LittleEndian>()?;
            Ok(NewRoot(id, root))
        }
        Tag::DelDev => {
            let id = r.read_u64::<LittleEndian>()?;
            Ok(DelDev(id))
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
            let k = r.read_u32::<LittleEndian>()?;
            let v = unpack_bytes(r)?;
            Ok(Overwrite(loc, idx, k, v))
        }
        Tag::Insert => {
            let loc = r.read_u32::<LittleEndian>()?;
            let idx = r.read_u16::<LittleEndian>()? as u32;
            let k = r.read_u32::<LittleEndian>()?;
            let v = unpack_bytes(r)?;
            Ok(Insert(loc, idx, k, v))
        }
        Tag::Prepend => {
            let loc = r.read_u32::<LittleEndian>()?;
            let len = r.read_u16::<LittleEndian>()? as usize;
            let mut keys = Vec::with_capacity(len);
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                keys.push(r.read_u32::<LittleEndian>()?);
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
                keys.push(r.read_u32::<LittleEndian>()?);
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

fn pack_ops<W: Write>(w: &mut W, ops: &[Entry]) -> Result<()> {
    w.write_u32::<LittleEndian>(ops.len() as u32)?;
    for op in ops {
        pack_op(w, op)?;
    }
    Ok(())
}

fn unpack_ops<R: Read>(r: &mut R) -> Result<Vec<Entry>> {
    let nr_ops = r.read_u32::<LittleEndian>()? as usize;
    let mut ops = Vec::with_capacity(nr_ops);
    for _ in 0..nr_ops {
        let op = unpack_op(r)?;
        ops.push(op);
    }
    Ok(ops)
}

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut output = "0x".to_string();
    bytes.iter().fold(output, |mut output, b| {
        let _ = write!(output, "{b:02x}");
        output
    })
}

fn format_op(entry: &Entry) -> String {
    use Entry::*;
    match entry {
        AllocMetadata(b, e) => format!("alloc-m\t{}..{}", b, e),
        FreeMetadata(b, e) => format!("free-m\t{}..{}", b, e),
        AllocData(b, e) => format!("alloc-d\t{}..{}", b, e),
        FreeData(b, e) => format!("free-d\t{}..{}", b, e),
        NewDev(id, size, root) => format!("NewDev: id={}, size={}, root={}", id, size, root),
        NewRoot(id, root) => format!("NewRoot: id={}, root={}", id, root),
        DelDev(id) => format!("DelDev: id={}", id),
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

pub struct Journal {
    slab: SlabFile,
    ops: Vec<Entry>,
    seqs: BTreeMap<MetadataBlock, SequenceNr>,
}

impl Drop for Journal {
    fn drop(&mut self) {
        self.sync().unwrap();
        self.slab.close();
    }
}

type NotifyFn = Box<dyn FnOnce()>;

impl Journal {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let slab = SlabFileBuilder::create(path)
            .read(true)
            .write(true)
            .compressed(true)
            .cache_nr_entries(16)
            .queue_depth(4)
            .build()?;

        Ok(Self {
            slab,
            ops: Vec::with_capacity(128),
            seqs: BTreeMap::new(),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, write: bool) -> Result<Self> {
        let slab = SlabFileBuilder::open(path)
            .read(true)
            .write(write)
            .cache_nr_entries(16)
            .queue_depth(4)
            .build()?;

        Ok(Self {
            slab,
            ops: Vec::with_capacity(128),
            seqs: BTreeMap::new(),
        })
    }

    /// Node ptr refers to the node before the op, after the op
    /// the seq_nr will be one higher.
    pub fn add_op(&mut self, op: Entry) -> Result<()> {
        self.ops.push(op);
        Ok(())
    }

    /// The callback will be made once all ops prior to this call have been
    /// persisted to disk.  Used to ensure the journal is written before
    /// btree nodes.
    pub fn add_barrier(&mut self, callback: NotifyFn) -> Result<()> {
        todo!()
    }

    pub fn sync(&mut self) -> Result<()> {
        // hack
        if self.ops.is_empty() {
            return Ok(());
        }

        let mut w: Vec<u8> = Vec::new();

        pack_ops(&mut w, &self.ops)?;
        self.ops.clear();

        self.slab.write_slab(&w)?;

        Ok(())
    }

    pub fn up_to_date(&mut self, n: &NodePtr) -> Result<bool> {
        if let Some(seq) = self.seqs.get(&n.loc) {
            if n.seq_nr == *seq {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(anyhow!("no sequence nr for {}", n.loc))
        }
    }

    pub fn get_ops(
        &mut self,
        _loc: MetadataBlock,
        _seq_old: SequenceNr,
        _seq_new: SequenceNr,
    ) -> Result<Vec<Entry>> {
        todo!()
    }

    pub fn dump<W: Write>(&mut self, out: &mut W) -> Result<()> {
        for s in 0..self.slab.get_nr_slabs() {
            let mut bytes = self.slab.read(s as u32)?;
            let mut r = std::io::Cursor::new(bytes.as_ref());
            let ops = unpack_ops(&mut r)?;
            for op in &ops {
                writeln!(out, "    {}", format_op(op))?;
            }
        }
        Ok(())
    }
}

//-------------------------------------------------------------------------
