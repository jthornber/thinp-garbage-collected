use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num_enum::TryFromPrimitive;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::convert::TryFrom;
use std::io::{Read, Write};
use std::path::Path;
use std::thread_local;

use crate::block_cache::*;
use crate::btree::node::Key;
use crate::btree::*;
use crate::journal::entry::*;
use crate::journal::format::*;
use crate::journal::pack::*;
use crate::slab::*;
use crate::types::*;

//-------------------------------------------------------------------------

thread_local! {
    static BATCH: RefCell<Option<Vec<Entry>>> = const {RefCell::new(None)};
}

pub fn begin_batch() -> Result<()> {
    BATCH.with(|f| {
        if f.borrow_mut().is_some() {
            Err(anyhow!("already in batch"))
        } else {
            *f.borrow_mut() = Some(Vec::new());
            Ok(())
        }
    })
}

pub fn end_batch() -> Result<Vec<Entry>> {
    BATCH.with(|f| {
        if f.borrow_mut().is_some() {
            let mut entries = None;
            std::mem::swap(&mut entries, &mut *f.borrow_mut());
            Ok(entries.unwrap())
        } else {
            Err(anyhow!("not in a batch"))
        }
    })
}

pub fn add_entry(e: Entry) -> Result<()> {
    BATCH.with(|f| {
        if let Some(ref mut batch) = *f.borrow_mut() {
            batch.push(e);
            Ok(())
        } else {
            Err(anyhow!("not in a batch"))
        }
    })
}

pub fn add_entries(es: &[Entry]) -> Result<()> {
    BATCH.with(|f| {
        if let Some(ref mut batch) = *f.borrow_mut() {
            batch.extend_from_slice(es);
            Ok(())
        } else {
            Err(anyhow!("not in a batch"))
        }
    })
}

//-------------------------------------------------------------------------
