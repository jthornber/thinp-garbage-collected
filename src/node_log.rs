use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num_enum::TryFromPrimitive;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::io::{Read, Write};
use std::path::Path;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::slab::*;

//-------------------------------------------------------------------------

pub type Bytes = Vec<u8>;

/// Operations that can be performed on a node.
#[derive(Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum NodeOp {
    SetSeq(SequenceNr),            // Only used when rereading output log
    Zero(usize, usize),            // begin, end (including node header)
    Literal(usize, Bytes),         // offset, bytes
    Shadow(NodePtr),               // origin
    Overwrite(u32, u32, Bytes),    // idx, k, v
    Insert(u32, u32, Bytes),       // idx, k, v
    Prepend(Vec<u32>, Vec<Bytes>), // keys, values
    Append(Vec<u32>, Vec<Bytes>),  // keys, values
    Erase(u32, u32),               // idx_b, idx_e
}

#[derive(Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
enum Tag {
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

fn pack_tag(tag: Tag) -> u8 {
    (tag as u8)
}

fn unpack_tag(b: u8) -> Result<Tag> {
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

fn pack_op<W: Write>(w: &mut W, op: &NodeOp) -> Result<()> {
    use NodeOp::*;

    match op {
        SetSeq(seq) => {
            w.write_u8(pack_tag(Tag::SetSeq))?;
            w.write_u32::<LittleEndian>(*seq)?;
        }
        Zero(begin, end) => {
            w.write_u8(pack_tag(Tag::Zero))?;
            w.write_u16::<LittleEndian>(*begin as u16)?;
            w.write_u16::<LittleEndian>(*end as u16)?;
        }
        Literal(offset, bytes) => {
            w.write_u8(pack_tag(Tag::Literal))?;
            w.write_u16::<LittleEndian>(*offset as u16)?;
            pack_bytes(w, bytes)?;
        }
        Shadow(origin) => {
            w.write_u8(pack_tag(Tag::Shadow))?;
            w.write_u32::<LittleEndian>(origin.loc)?;
            w.write_u32::<LittleEndian>(origin.seq_nr)?;
        }
        Overwrite(idx, k, v) => {
            w.write_u8(pack_tag(Tag::Overwrite))?;
            w.write_u16::<LittleEndian>(*idx as u16)?;
            w.write_u32::<LittleEndian>(*k)?;
            pack_bytes(w, v)?;
        }
        Insert(idx, k, v) => {
            w.write_u8(pack_tag(Tag::Insert))?;
            w.write_u16::<LittleEndian>(*idx as u16)?;
            w.write_u32::<LittleEndian>(*k)?;
            pack_bytes(w, v)?;
        }
        Prepend(keys, values) => {
            assert!(keys.len() == values.len());

            w.write_u8(pack_tag(Tag::Prepend))?;
            w.write_u16::<LittleEndian>(keys.len() as u16)?;
            for (k, v) in keys.iter().zip(values.iter()) {
                w.write_u32::<LittleEndian>(*k)?;
                pack_bytes(w, v)?;
            }
        }
        Append(keys, values) => {
            assert!(keys.len() == values.len());

            w.write_u8(pack_tag(Tag::Append))?;
            w.write_u8(pack_tag(Tag::Prepend))?;
            w.write_u16::<LittleEndian>(keys.len() as u16)?;
            for (k, v) in keys.iter().zip(values.iter()) {
                w.write_u32::<LittleEndian>(*k)?;
                pack_bytes(w, v)?;
            }
        }
        Erase(idx_b, idx_e) => {
            w.write_u8(pack_tag(Tag::Erase))?;
            w.write_u16::<LittleEndian>(*idx_b as u16)?;
            w.write_u16::<LittleEndian>(*idx_e as u16)?;
        }
    }

    Ok(())
}

fn pack_node_ops<W: Write>(w: &mut W, loc: MetadataBlock, ops: &[NodeOp]) -> Result<()> {
    w.write_u32::<LittleEndian>(loc)?;
    w.write_u16::<LittleEndian>(ops.len() as u16)?;
    for op in ops {
        pack_op(w, op)?;
    }
    Ok(())
}

fn unpack_op<R: Read>(r: &mut R) -> Result<NodeOp> {
    use NodeOp::*;
    let tag = unpack_tag(r.read_u8()?)?;
    match tag {
        Tag::SetSeq => {
            let seq = r.read_u32::<LittleEndian>()?;
            Ok(SetSeq(seq))
        }
        Tag::Zero => {
            let begin = r.read_u16::<LittleEndian>()? as usize;
            let end = r.read_u16::<LittleEndian>()? as usize;
            Ok(Zero(begin, end))
        }
        Tag::Literal => {
            let offset = r.read_u16::<LittleEndian>()? as usize;
            let bytes = unpack_bytes(r)?;
            Ok(Literal(offset, bytes))
        }
        Tag::Shadow => {
            let loc = r.read_u32::<LittleEndian>()?;
            let seq_nr = r.read_u32::<LittleEndian>()?;
            Ok(Shadow(NodePtr { loc, seq_nr }))
        }
        Tag::Overwrite => {
            let idx = r.read_u16::<LittleEndian>()? as u32;
            let k = r.read_u32::<LittleEndian>()?;
            let v = unpack_bytes(r)?;
            Ok(Overwrite(idx, k, v))
        }
        Tag::Insert => {
            let idx = r.read_u16::<LittleEndian>()? as u32;
            let k = r.read_u32::<LittleEndian>()?;
            let v = unpack_bytes(r)?;
            Ok(Insert(idx, k, v))
        }
        Tag::Prepend => {
            let len = r.read_u16::<LittleEndian>()? as usize;
            let mut keys = Vec::with_capacity(len);
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                keys.push(r.read_u32::<LittleEndian>()?);
                values.push(unpack_bytes(r)?);
            }
            Ok(Prepend(keys, values))
        }
        Tag::Append => {
            let len = r.read_u16::<LittleEndian>()? as usize;
            let mut keys = Vec::with_capacity(len);
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                keys.push(r.read_u32::<LittleEndian>()?);
                values.push(unpack_bytes(r)?);
            }
            Ok(Append(keys, values))
        }
        Tag::Erase => {
            let idx_b = r.read_u16::<LittleEndian>()? as u32;
            let idx_e = r.read_u16::<LittleEndian>()? as u32;
            Ok(Erase(idx_b, idx_e))
        }
    }
}

fn unpack_node_ops<R: Read>(r: &mut R) -> Result<(MetadataBlock, Vec<NodeOp>)> {
    let loc = r.read_u32::<LittleEndian>()?;
    let num_ops = r.read_u16::<LittleEndian>()? as usize;
    let mut ops = Vec::with_capacity(num_ops);
    for _ in 0..num_ops {
        let op = unpack_op(r)?;
        ops.push(op);
    }
    Ok((loc, ops))
}

pub struct NodeLog {
    slab: SlabFile,
    nodes: BTreeMap<MetadataBlock, Vec<NodeOp>>,
    seqs: BTreeMap<MetadataBlock, SequenceNr>,
}

impl NodeLog {
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
            nodes: BTreeMap::new(),
            seqs: BTreeMap::new(),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, write: bool) -> Result<Self> {
        let slab = SlabFileBuilder::open(path)
            .read(true)
            .write(write)
            .compressed(true)
            .cache_nr_entries(16)
            .queue_depth(4)
            .build()?;

        Ok(Self {
            slab,
            nodes: BTreeMap::new(),
            seqs: BTreeMap::new(),
        })
    }

    /// Node ptr refers to the node before the op, after the op
    /// the seq_nr will be one higher.
    pub fn add_op(&mut self, n: &NodePtr, op: &NodeOp) -> Result<()> {
        self.nodes
            .entry(n.loc)
            .and_modify(|ops| ops.push(op.clone()));
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let mut w: Vec<u8> = Vec::new();

        let mut nodes = BTreeMap::new();
        std::mem::swap(&mut nodes, &mut self.nodes);

        w.write_u32::<LittleEndian>(nodes.len() as u32)?;

        for (n, ops) in nodes {
            pack_node_ops(&mut w, n, &ops)?;
        }

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
    ) -> Result<Vec<NodeOp>> {
        todo!()
    }
}

//-------------------------------------------------------------------------
