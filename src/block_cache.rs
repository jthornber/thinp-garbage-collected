use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use linked_hash_map::*;
use std::collections::BTreeMap;
use std::io::{self, Result};
use std::io::{Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;
use thinp::io_engine::*;

use crate::byte_types::*;

//-------------------------------------------------------------------------

pub type MetadataBlock = u32;

fn fail_(msg: String) -> Result<()> {
    Err(io::Error::new(io::ErrorKind::Other, msg))
}

fn get_tid_() -> ThreadId {
    std::thread::current().id()
}

//-------------------------------------------------------------------------

#[derive(Eq, PartialEq)]
enum LockState {
    Unlocked,
    Shared(usize),

    // We record the thread id so we can spot dead locks
    Exclusive(ThreadId),
}

struct EntryInner {
    lock: LockState,
    dirty: bool,
    block: Block,
}

struct CacheEntry {
    inner: Mutex<EntryInner>,
    cond: Condvar,
}

impl CacheEntry {
    fn new_shared(block: Block) -> CacheEntry {
        CacheEntry {
            inner: Mutex::new(EntryInner {
                lock: LockState::Shared(1),
                dirty: false,
                block,
            }),
            cond: Condvar::new(),
        }
    }

    fn new_exclusive(block: Block) -> CacheEntry {
        CacheEntry {
            inner: Mutex::new(EntryInner {
                lock: LockState::Exclusive(get_tid_()),
                dirty: true,
                block,
            }),
            cond: Condvar::new(),
        }
    }

    fn is_dirty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.dirty
    }

    fn clear_dirty(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.dirty = false
    }

    fn is_held(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.lock != LockState::Unlocked
    }

    // Returns true on success, if false you will need to wait for the lock
    fn shared_lock(&self) -> bool {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                inner.lock = Shared(1);
                true
            }
            Shared(n) => {
                inner.lock = Shared(n + 1);
                true
            }
            Exclusive(_tid) => false,
        }
    }

    // Returns true on success, if false you will need to wait for the lock
    fn exclusive_lock(&self) -> bool {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                inner.lock = Exclusive(get_tid_());
                inner.dirty = true;
                true
            }
            Shared(_) => false,
            Exclusive(tid) => {
                if tid == get_tid_() {
                    panic!("thread attempting to lock block {} twice", inner.block.loc);
                }
                false
            }
        }
    }

    fn unlock(&self) {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                panic!("Unlocking an unlocked block {}", inner.block.loc);
            }
            Shared(1) => {
                inner.lock = Unlocked;
            }
            Shared(n) => {
                inner.lock = Shared(n - 1);
            }
            Exclusive(tid) => {
                assert!(tid == get_tid_());
                inner.lock = Unlocked;
            }
        }
        self.cond.notify_all();
    }
}

//-------------------------------------------------------------------------

enum LockResult {
    Locked(Arc<CacheEntry>),
    Busy(Arc<CacheEntry>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    AlreadyPresent,
    Added,
    AddAndEvict(u32),
}

struct MetadataCacheInner {
    nr_blocks: u32,
    nr_held: usize,
    capacity: usize,
    engine: Arc<dyn IoEngine>,

    // The LRU lists only contain blocks that are not currently locked.
    lru: LinkedHashMap<u32, u32>,
    cache: BTreeMap<u32, Arc<CacheEntry>>,
}

impl MetadataCacheInner {
    pub fn new(engine: Arc<dyn IoEngine>, capacity: usize) -> Result<Self> {
        let nr_blocks = engine.get_nr_blocks() as u32;
        Ok(Self {
            nr_blocks,
            nr_held: 0,
            capacity,
            engine,
            lru: LinkedHashMap::new(),
            cache: BTreeMap::new(),
        })
    }

    pub fn nr_blocks(&self) -> u32 {
        self.nr_blocks
    }

    pub fn nr_held(&self) -> usize {
        self.nr_held
    }

    pub fn residency(&self) -> usize {
        self.lru.len()
    }

    fn lru_push_(&mut self, loc: u32) -> PushResult {
        use PushResult::*;

        if self.lru.contains_key(&loc) {
            AlreadyPresent
        } else if self.lru.len() < self.capacity {
            self.lru.insert(loc, loc);
            Added
        } else {
            let old = self.lru.pop_front().unwrap();
            self.lru.insert(loc, loc);
            AddAndEvict(old.1)
        }
    }

    fn insert_lru_(&mut self, loc: u32) -> Result<()> {
        match self.lru_push_(loc) {
            PushResult::AlreadyPresent => {
                panic!("AlreadyPresent")
            }
            PushResult::Added => {
                // Nothing
            }
            PushResult::AddAndEvict(old) => {
                let old_entry = self.cache.remove(&old).unwrap();
                if old_entry.is_dirty() {
                    self.writeback_(&old_entry)?;
                }
            }
        }

        Ok(())
    }

    fn remove_lru_(&mut self, loc: u32) {
        self.lru.remove(&loc);
    }

    fn read_(&mut self, loc: u32) -> Result<Block> {
        let block = self.engine.read(loc as u64)?;
        Ok(block)
    }

    fn writeback_(&self, entry: &CacheEntry) -> Result<()> {
        let inner = entry.inner.lock().unwrap();
        self.engine.write(&inner.block)?;
        Ok(())
    }

    fn unlock(&mut self, loc: u32) -> Result<()> {
        let entry = self.cache.get_mut(&loc).unwrap();
        entry.unlock();
        self.insert_lru_(loc)?;
        Ok(())
    }

    // Returns true on success
    pub fn shared_lock(&mut self, loc: u32) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.shared_lock() {
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let entry = Arc::new(CacheEntry::new_shared(self.read_(loc)?));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    pub fn gc_lock(&mut self, loc: u32) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.shared_lock() {
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                panic!("cannot gc_lock an exclusive locked block");
            }
        } else {
            let entry = Arc::new(CacheEntry::new_shared(self.read_(loc)?));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry))
        }
    }

    pub fn exclusive_lock(&mut self, loc: u32) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.exclusive_lock() {
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let entry = Arc::new(CacheEntry::new_exclusive(self.read_(loc)?));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    /// Exclusive lock and zero the data (avoids reading the block)
    pub fn zero_lock(&mut self, loc: u32) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.exclusive_lock() {
                let inner = entry.inner.lock().unwrap();
                let data = inner.block.get_data();
                unsafe {
                    std::ptr::write_bytes(data.as_mut_ptr(), 0, BLOCK_SIZE);
                }
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let block = Block::zeroed(loc as u64);
            let entry = Arc::new(CacheEntry::new_exclusive(block));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    /// Writeback all dirty blocks
    // FIXME: synchronous!
    pub fn flush(&mut self) -> Result<()> {
        for entry in self.cache.values() {
            if !entry.is_held() && entry.is_dirty() {
                self.writeback_(entry)?;
                entry.clear_dirty();
            }
        }

        Ok(())
    }
}

//-------------------------------------------------------------------------

#[derive(Clone)]
pub struct SharedProxy_ {
    pub loc: u32,
    cache: Arc<MetadataCache>,
    entry: Arc<CacheEntry>,
}

impl Drop for SharedProxy_ {
    fn drop(&mut self) {
        self.cache.unlock_(self.loc);
    }
}

#[derive(Clone)]
pub struct SharedProxy {
    proxy: Arc<SharedProxy_>,
    begin: usize,
    end: usize,
}

impl SharedProxy {
    pub fn loc(&self) -> u32 {
        self.proxy.loc
    }
}

impl Readable for SharedProxy {
    fn r(&self) -> &[u8] {
        let inner = self.proxy.entry.inner.lock().unwrap();
        &inner.block.get_data()[self.begin..self.end]
    }

    // FIXME: should split_at consume self?
    fn split_at(&self, offset: usize) -> (Self, Self) {
        assert!(offset < (self.end - self.begin));
        (
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin,
                end: self.begin + offset,
            },
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin + offset,
                end: self.end,
            },
        )
    }
}

//-------------------------------------------------------------------------

#[derive(Clone)]
pub struct ExclusiveProxy_ {
    pub loc: MetadataBlock,
    cache: Arc<MetadataCache>,
    entry: Arc<CacheEntry>,
}

impl Drop for ExclusiveProxy_ {
    fn drop(&mut self) {
        self.cache.unlock_(self.loc);
    }
}

#[derive(Clone)]
pub struct ExclusiveProxy {
    proxy: Arc<ExclusiveProxy_>,
    begin: usize,
    end: usize,
}

impl ExclusiveProxy {
    pub fn loc(&self) -> MetadataBlock {
        self.proxy.loc
    }
}

impl Readable for ExclusiveProxy {
    fn r(&self) -> &[u8] {
        let inner = self.proxy.entry.inner.lock().unwrap();
        &inner.block.get_data()[self.begin..self.end]
    }

    fn split_at(&self, offset: usize) -> (Self, Self) {
        assert!(offset < (self.end - self.begin));
        (
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin,
                end: self.begin + offset,
            },
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin + offset,
                end: self.end,
            },
        )
    }
}

impl Writeable for ExclusiveProxy {
    fn rw(&mut self) -> &mut [u8] {
        let inner = self.proxy.entry.inner.lock().unwrap();
        &mut inner.block.get_data()[self.begin..self.end]
    }
}

//-------------------------------------------------------------------------

pub struct MetadataCache {
    inner: Mutex<MetadataCacheInner>,
}

impl Drop for MetadataCache {
    fn drop(&mut self) {
        self.flush()
            .expect("flush failed when dropping metadata cache");
    }
}

impl MetadataCache {
    pub fn new(engine: Arc<dyn IoEngine>, capacity: usize) -> Result<Self> {
        let inner = MetadataCacheInner::new(engine, capacity)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    pub fn nr_blocks(&self) -> u32 {
        let inner = self.inner.lock().unwrap();
        inner.nr_blocks()
    }

    pub fn nr_held(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.nr_held()
    }

    pub fn residency(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.residency()
    }

    pub fn shared_lock(self: &Arc<Self>, loc: u32) -> Result<SharedProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.shared_lock(loc)? {
                Locked(entry) => {
                    let proxy_ = SharedProxy_ {
                        loc,
                        cache: self.clone(),
                        entry: entry.clone(),
                    };

                    let proxy = SharedProxy {
                        proxy: Arc::new(proxy_),
                        begin: 0,
                        end: BLOCK_SIZE,
                    };

                    return Ok(proxy);
                }
                Busy(entry) => self.wait_on_entry_(&entry),
            }
        }
    }

    pub fn gc_lock(self: Arc<Self>, loc: u32) -> Result<SharedProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        match inner.gc_lock(loc)? {
            Locked(entry) => {
                let proxy_ = SharedProxy_ {
                    loc,
                    cache: self.clone(),
                    entry: entry.clone(),
                };

                let proxy = SharedProxy {
                    proxy: Arc::new(proxy_),
                    begin: 0,
                    end: BLOCK_SIZE,
                };

                Ok(proxy)
            }
            Busy(_) => {
                panic!("gc_lock blocked!");
            }
        }
    }

    pub fn exclusive_lock(self: &Arc<Self>, loc: u32) -> Result<ExclusiveProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.exclusive_lock(loc)? {
                Locked(entry) => {
                    let proxy_ = ExclusiveProxy_ {
                        loc,
                        cache: self.clone(),
                        entry: entry.clone(),
                    };

                    let proxy = ExclusiveProxy {
                        proxy: Arc::new(proxy_),
                        begin: 0,
                        end: BLOCK_SIZE,
                    };

                    return Ok(proxy);
                }
                Busy(entry) => self.wait_on_entry_(&entry),
            }
        }
    }

    /// Exclusive lock and zero the data (avoids reading the block)
    pub fn zero_lock(self: &Arc<Self>, loc: u32) -> Result<ExclusiveProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.zero_lock(loc)? {
                Locked(entry) => {
                    let proxy_ = ExclusiveProxy_ {
                        loc,
                        cache: self.clone(),
                        entry: entry.clone(),
                    };

                    let proxy = ExclusiveProxy {
                        proxy: Arc::new(proxy_),
                        begin: 0,
                        end: BLOCK_SIZE,
                    };

                    return Ok(proxy);
                }
                Busy(entry) => self.wait_on_entry_(&entry),
            }
        }
    }

    /// Writeback all dirty blocks
    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.flush()
    }

    // for use by the proxies only
    fn unlock_(&self, loc: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.unlock(loc).expect("unlock failed");
    }

    // Do not call this with the top level cache lock held
    fn wait_on_entry_(&self, entry: &CacheEntry) {
        let inner = entry.inner.lock().unwrap();
        let _guard = entry.cond.wait(inner).unwrap();
    }
}

//-------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::*;
    use anyhow::{ensure, Result};
    use std::io;
    use std::sync::Arc;

    fn stamp(data: &mut [u8], byte: u8) -> Result<()> {
        let len = data.len();
        let mut w = io::Cursor::new(data);

        // FIXME: there must be a std function for this
        for _ in 0..len {
            w.write_u8(byte)?;
        }

        Ok(())
    }

    fn verify(data: &[u8], byte: u8) {
        for b in data.iter() {
            assert!(*b == byte);
        }
    }

    fn mk_engine(nr_blocks: u32) -> Arc<dyn IoEngine> {
        Arc::new(CoreIoEngine::new(nr_blocks as u64))
    }

    #[test]
    fn test_create() -> Result<()> {
        let engine = mk_engine(16);
        let _cache = Arc::new(MetadataCache::new(engine, 16)?);
        Ok(())
    }

    #[test]
    fn test_new_block() -> Result<()> {
        let engine = mk_engine(16);
        let cache = Arc::new(MetadataCache::new(engine, 16)?);
        let mut wp = cache.zero_lock(0)?;
        stamp(wp.rw(), 21)?;
        drop(wp);

        cache.flush()?;

        let rp = cache.shared_lock(0)?;

        let data = rp.r();
        verify(data, 21);

        Ok(())
    }

    #[test]
    fn test_rolling_writes() -> Result<()> {
        let nr_blocks = 1024u32;
        let engine = mk_engine(nr_blocks);

        {
            const CACHE_SIZE: usize = 16;
            let cache = Arc::new(MetadataCache::new(engine.clone(), CACHE_SIZE)?);

            for i in 0..nr_blocks {
                let mut wp = cache.zero_lock(i)?;
                stamp(wp.rw(), i as u8)?;
                ensure!(cache.residency() <= CACHE_SIZE);
            }
        }

        {
            let cache = Arc::new(MetadataCache::new(engine, 16)?);

            for i in 0..nr_blocks {
                let rp = cache.shared_lock(i)?;
                verify(rp.r(), i as u8);
            }
        }

        Ok(())
    }

    #[test]
    fn test_write_twice() -> Result<()> {
        let nr_blocks = 1024u32;
        let engine = mk_engine(nr_blocks);

        {
            const CACHE_SIZE: usize = 16;
            let cache = Arc::new(MetadataCache::new(engine.clone(), CACHE_SIZE)?);

            for i in 0..nr_blocks {
                let mut wp = cache.zero_lock(i)?;
                stamp(wp.rw(), i as u8)?;
                ensure!(cache.residency() <= CACHE_SIZE);
            }
        }

        {
            const CACHE_SIZE: usize = 16;
            let cache = Arc::new(MetadataCache::new(engine.clone(), CACHE_SIZE)?);

            for i in 0..nr_blocks {
                let mut wp = cache.zero_lock(i)?;
                stamp(wp.rw(), (i * 3) as u8)?;
                ensure!(cache.residency() <= CACHE_SIZE);
            }
        }

        {
            let cache = Arc::new(MetadataCache::new(engine, 16)?);

            for i in 0..nr_blocks {
                let rp = cache.shared_lock(i)?;
                verify(rp.r(), (i * 3) as u8);
            }
        }

        Ok(())
    }

    #[test]
    fn test_zerolock_cached_block() -> Result<()> {
        let engine = mk_engine(16);
        let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);
        {
            cache.zero_lock(0)?;
        }
        {
            cache.zero_lock(0)?;
        }
        Ok(())
    }
}

//-------------------------------------------------------------------------
