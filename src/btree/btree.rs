use anyhow::{ensure, Result};
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use crate::block_allocator::BlockRef;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::insert;
use crate::btree::node::*;
use crate::btree::node_alloc::*;
use crate::btree::remove;
use crate::btree::simple_node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub struct BTree<
    V: Serializable + Copy,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
> {
    tm: Arc<TransactionManager>,
    context: ReferenceContext,
    root: u32,
    phantom_v: std::marker::PhantomData<V>,
    phantom_inode: std::marker::PhantomData<INode>,
    phantom_lnode: std::marker::PhantomData<LNode>,
}

impl<
        V: Serializable + Copy,
        INode: NodeW<MetadataBlock, WriteProxy>,
        LNode: NodeW<V, WriteProxy>,
    > BTree<V, INode, LNode>
{
    pub fn open_tree(tm: Arc<TransactionManager>, context: ReferenceContext, root: u32) -> Self {
        Self {
            tm,
            context,
            root,
            phantom_v: std::marker::PhantomData,
            phantom_inode: std::marker::PhantomData,
            phantom_lnode: std::marker::PhantomData,
        }
    }

    pub fn empty_tree(tm: Arc<TransactionManager>, context: ReferenceContext) -> Result<Self> {
        let root = {
            let root = tm.new_block(context, &BNODE_KIND)?;
            let loc = root.loc();
            LNode::init(loc, root, true)?;
            loc
        };

        Ok(Self {
            tm,
            context,
            root,
            phantom_v: std::marker::PhantomData,
            phantom_inode: std::marker::PhantomData,
            phantom_lnode: std::marker::PhantomData,
        })
    }

    pub fn clone(&self, context: ReferenceContext) -> Self {
        Self {
            tm: self.tm.clone(),
            context,
            root: self.root,
            phantom_v: std::marker::PhantomData,
            phantom_inode: std::marker::PhantomData,
            phantom_lnode: std::marker::PhantomData,
        }
    }

    pub fn root(&self) -> u32 {
        self.root
    }

    //-------------------------------

    pub fn lookup(&self, key: u32) -> Result<Option<V>> {
        let mut block = self.tm.read(self.root, &BNODE_KIND)?;

        loop {
            let flags = read_flags(block.r())?;

            match flags {
                BTreeFlags::Internal => {
                    let node = SimpleNode::<u32, ReadProxy>::new(block.loc(), block);

                    let idx = node.keys.bsearch(&key);
                    if idx < 0 || idx >= node.nr_entries.get() as isize {
                        return Ok(None);
                    }

                    let child = node.values.get(idx as usize);
                    block = self.tm.read(child, &BNODE_KIND)?;
                }
                BTreeFlags::Leaf => {
                    let node = SimpleNode::<V, ReadProxy>::new(block.loc(), block);

                    let idx = node.keys.bsearch(&key);
                    if idx < 0 || idx >= node.nr_entries.get() as isize {
                        return Ok(None);
                    }

                    return if node.keys.get(idx as usize) == key {
                        Ok(Some(node.values.get(idx as usize)))
                    } else {
                        Ok(None)
                    };
                }
            }
        }
    }

    fn mk_alloc(&self) -> NodeAlloc {
        NodeAlloc::new(self.tm.clone(), self.context)
    }

    pub fn insert(&mut self, key: u32, value: &V) -> Result<()> {
        let mut alloc = self.mk_alloc();
        self.root = insert::insert::<V, INode, LNode>(&mut alloc, self.root, key, value)?;
        Ok(())
    }

    pub fn remove(&mut self, key: u32) -> Result<()> {
        let mut alloc = self.mk_alloc();
        let root = remove::remove::<V, INode, LNode>(&mut alloc, self.root, key)?;
        self.root = root;
        Ok(())
    }

    pub fn remove_geq(&mut self, key: u32, val_fn: &remove::ValFn<V>) -> Result<()> {
        let mut alloc = self.mk_alloc();
        let new_root = remove::remove_geq::<V, INode, LNode>(&mut alloc, self.root, key, val_fn)?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove_lt(&mut self, key: u32, val_fn: &remove::ValFn<V>) -> Result<()> {
        let mut alloc = self.mk_alloc();
        let new_root = remove::remove_lt::<V, INode, LNode>(&mut alloc, self.root, key, val_fn)?;
        self.root = new_root;
        Ok(())
    }

    //-------------------------------

    /// Returns a vec of key, value pairs
    pub fn lookup_range(&self, _key_low: u32, _key_high: u32) -> Result<Vec<(u32, V)>> {
        todo!();
    }

    pub fn insert_range(&mut self, _kvs: &[(u32, V)]) -> Result<()> {
        todo!();
    }

    pub fn remove_range(
        &mut self,
        key_begin: u32,
        key_end: u32,
        val_lt: &remove::ValFn<V>,
        val_geq: &remove::ValFn<V>,
    ) -> Result<()> {
        let mut alloc = self.mk_alloc();
        self.root = remove::remove_range::<V, INode, LNode>(
            &mut alloc, self.root, key_begin, key_end, val_lt, val_geq,
        )?;
        Ok(())
    }

    //-------------------------------

    fn check_(
        &self,
        loc: u32,
        key_min: u32,
        key_max: Option<u32>,
        seen: &mut BTreeSet<u32>,
    ) -> Result<u32> {
        let mut total = 0;

        ensure!(!seen.contains(&loc));
        seen.insert(loc);

        let block = self.tm.read(loc, &BNODE_KIND).unwrap();
        let node = r_node(block);

        // check the keys
        let mut last = None;
        for i in 0..node.nr_entries.get() {
            let k = node.keys.get(i as usize);
            ensure!(k >= key_min);

            if let Some(key_max) = key_max {
                ensure!(k < key_max);
            }

            if let Some(last) = last {
                if k <= last {
                    eprintln!("keys out of order: {}, {}", last, k);
                    ensure!(k > last);
                }
            }
            last = Some(k);
        }

        if node.flags.get() == BTreeFlags::Internal as u32 {
            for i in 0..node.nr_entries.get() {
                let kmin = node.keys.get(i as usize);
                let kmax = if i == node.nr_entries.get() - 1 {
                    None
                } else {
                    Some(node.keys.get(i as usize + 1))
                };
                let loc = node.values.get(i as usize);
                total += self.check_(loc, kmin, kmax, seen)?;
            }
        } else {
            total += node.keys.len() as u32;
        }

        Ok(total)
    }

    /// Checks the btree is well formed and returns the number of entries
    /// in the tree.
    pub fn check(&self) -> Result<u32> {
        let mut seen = BTreeSet::new();
        self.check_(self.root, 0, None, &mut seen)
    }
}

pub fn btree_refs(data: &ReadProxy, queue: &mut VecDeque<BlockRef>) {
    let node = r_node(data.clone());

    if read_flags(data.r()).unwrap() == BTreeFlags::Leaf {
        // FIXME: values should be refs, except in the btree unit tests
    } else {
        for i in 0..node.nr_entries.get() {
            queue.push_back(BlockRef::Metadata(node.values.get(i as usize)));
        }
    }
}

//-------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_allocator::*;
    use crate::core::*;
    use crate::scope_id;
    use anyhow::{ensure, Result};
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::io::{self, Read, Write};
    use std::sync::{Arc, Mutex};
    use test_log::test;
    use thinp::io_engine::*;

    fn mk_engine(nr_blocks: u32) -> Arc<dyn IoEngine> {
        Arc::new(CoreIoEngine::new(nr_blocks as u64))
    }

    fn mk_allocator(
        cache: Arc<MetadataCache>,
        nr_data_blocks: u64,
    ) -> Result<Arc<Mutex<BlockAllocator>>> {
        const SUPERBLOCK_LOCATION: u32 = 0;
        let allocator = BlockAllocator::new(cache, nr_data_blocks, SUPERBLOCK_LOCATION)?;
        Ok(Arc::new(Mutex::new(allocator)))
    }

    // We'll test with a value type that is a different size to the internal node values (u32).
    #[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Copy, Clone)]
    struct Value {
        v: u32,
        len: u32,
    }

    impl Serializable for Value {
        fn packed_len() -> usize {
            6
        }

        fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
            w.write_u32::<LittleEndian>(self.v)?;
            w.write_u16::<LittleEndian>(self.len as u16)?;
            Ok(())
        }

        fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
            let v = r.read_u32::<LittleEndian>()?;
            let len = r.read_u16::<LittleEndian>()?;
            Ok(Self { v, len: len as u32 })
        }
    }

    type TestTree =
        BTree<Value, SimpleNode<MetadataBlock, WriteProxy>, SimpleNode<Value, WriteProxy>>;

    #[allow(dead_code)]
    struct Fixture {
        engine: Arc<dyn IoEngine>,
        cache: Arc<MetadataCache>,
        allocator: Arc<Mutex<BlockAllocator>>,
        tm: Arc<TransactionManager>,
        tree: TestTree,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, nr_data_blocks: u64) -> Result<Self> {
            let engine = mk_engine(nr_metadata_blocks);
            let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);
            let allocator = mk_allocator(cache.clone(), nr_data_blocks)?;
            let tm = Arc::new(TransactionManager::new(allocator.clone(), cache.clone()));
            let tree = BTree::empty_tree(tm.clone(), ReferenceContext::ThinId(0))?;

            Ok(Self {
                engine,
                cache,
                allocator,
                tm,
                tree,
            })
        }

        fn clone(&self, context: ReferenceContext) -> Self {
            Self {
                engine: self.engine.clone(),
                cache: self.cache.clone(),
                allocator: self.allocator.clone(),
                tm: self.tm.clone(),
                tree: self.tree.clone(context),
            }
        }

        fn check(&self) -> Result<u32> {
            self.tree.check()
        }

        fn lookup(&self, key: u32) -> Option<Value> {
            self.tree.lookup(key).unwrap()
        }

        fn insert(&mut self, key: u32, value: &Value) -> Result<()> {
            self.tree.insert(key, value)
        }

        fn remove(&mut self, key: u32) -> Result<()> {
            self.tree.remove(key)
        }

        fn commit(&mut self) -> Result<()> {
            let roots = vec![self.tree.root()];
            self.tm.commit(&roots)
        }
    }

    fn mk_value(v: u32) -> Value {
        Value { v, len: 3 }
    }

    #[test]
    fn empty_btree() -> Result<()> {
        const NR_BLOCKS: u32 = 1024;
        const NR_DATA_BLOCKS: u64 = 102400;

        let mut fix = Fixture::new(NR_BLOCKS, NR_DATA_BLOCKS)?;
        fix.commit()?;

        Ok(())
    }

    #[test]
    fn lookup_fails() -> Result<()> {
        let fix = Fixture::new(1024, 102400)?;
        ensure!(fix.lookup(0).is_none());
        ensure!(fix.lookup(1234).is_none());
        Ok(())
    }

    #[test]
    fn insert_single() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        fix.insert(0, &mk_value(100))?;
        ensure!(fix.lookup(0) == Some(mk_value(100)));

        Ok(())
    }

    fn insert_test_(fix: &mut Fixture, keys: &[u32]) -> Result<()> {
        for (i, k) in keys.iter().enumerate() {
            fix.insert(*k, &mk_value(*k * 2))?;
            if i % 1000 == 0 {
                let n = fix.check()?;
                ensure!(n == i as u32 + 1);
            }
        }

        fix.commit()?;

        for k in keys {
            ensure!(fix.lookup(*k) == Some(mk_value(k * 2)));
        }

        let n = fix.check()?;
        ensure!(n == keys.len() as u32);

        Ok(())
    }

    fn insert_test(keys: &[u32]) -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;
        insert_test_(&mut fix, keys)
    }

    #[test]
    fn insert_sequence() -> Result<()> {
        let count = 100_000;
        insert_test(&(0..count).collect::<Vec<u32>>())
    }

    #[test]
    fn insert_random() -> Result<()> {
        let count = 100_000;
        let mut keys: Vec<u32> = (0..count).collect();

        // shuffle the keys
        let mut rng = rand::thread_rng();
        keys.shuffle(&mut rng);

        insert_test(&keys)
    }

    #[test]
    fn remove_single() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let key = 100;
        let val = 123;
        fix.insert(key, &mk_value(val))?;
        ensure!(fix.lookup(key) == Some(mk_value(val)));
        fix.remove(key)?;
        ensure!(fix.lookup(key) == None);
        Ok(())
    }

    #[test]
    fn remove_random() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        // build a big btree
        // let count = 100_000;
        let count = 10_000;
        for i in 0..count {
            fix.insert(i, &mk_value(i * 3))?;
        }
        eprintln!("built tree");

        let mut keys: Vec<u32> = (0..count).collect();
        let mut rng = rand::thread_rng();
        keys.shuffle(&mut rng);

        for (i, k) in keys.into_iter().enumerate() {
            ensure!(fix.lookup(k).is_some());
            fix.remove(k)?;
            ensure!(fix.lookup(k).is_none());
            if i % 100 == 0 {
                eprintln!("removed {}", i);

                let n = fix.check()?;
                ensure!(n == count - i as u32 - 1);
                eprintln!("checked tree");
            }
        }

        Ok(())
    }

    #[test]
    fn rolling_insert_remove() -> Result<()> {
        // If the GC is not working then we'll run out of metadata space.
        let mut fix = Fixture::new(32, 10240)?;
        fix.commit()?;

        for k in 0..1_000_000 {
            fix.insert(k, &mk_value(k * 3))?;
            if k > 100 {
                fix.remove(k - 100)?;
            }

            if k % 100 == 0 {
                eprintln!("inserted {} entries", k);
                fix.commit()?;
            }
        }

        Ok(())
    }

    #[test]
    fn remove_geq_empty() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let no_split = |k: u32, v: Value| Some((k, v));

        fix.tree.remove_geq(100, &remove::mk_val_fn(no_split))?;
        ensure!(fix.tree.check()? == 0);
        Ok(())
    }

    #[test]
    fn remove_lt_empty() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let no_split = |k: u32, v: Value| Some((k, v));

        fix.tree.remove_lt(100, &remove::mk_val_fn(no_split))?;
        ensure!(fix.tree.check()? == 0);
        Ok(())
    }

    fn build_tree(fix: &mut Fixture, count: u32) -> Result<()> {
        fix.commit()?;

        for i in 0..count {
            fix.insert(i, &mk_value(i * 3))?;
        }

        Ok(())
    }

    fn remove_geq_and_verify(fix: &mut Fixture, cut: u32) -> Result<()> {
        let no_split = |k: u32, v: Value| Some((k, v));
        fix.tree.remove_geq(cut, &remove::mk_val_fn(no_split))?;
        ensure!(fix.tree.check()? == cut);

        // FIXME: use lookup_range() to verify
        /*
                let mut c = fix.tree.cursor(0)?;

                // Check all entries are below `cut`
                for i in 0..cut {
                    let (k, v) = c.get()?.unwrap();
                    ensure!(k == i);
                    ensure!(v.v == i * 3);
                    c.next_entry()?;
                }
        */

        Ok(())
    }

    fn remove_lt_and_verify(fix: &mut Fixture, count: u32, cut: u32) -> Result<()> {
        let no_split = |k: u32, v: Value| Some((k, v));
        fix.tree.remove_lt(cut, &remove::mk_val_fn(no_split))?;
        ensure!(fix.tree.check()? == count - cut);

        // FIXME: use lookup_range() to verify
        /*
                let mut c = fix.tree.cursor(0)?;

                // Check all entries are above `cut`
                for i in cut..count {
                    let (k, v) = c.get()?.unwrap();
                    ensure!(k == i);
                    ensure!(v.v == i * 3);
                    c.next_entry()?;
                }
        */

        Ok(())
    }

    #[test]
    fn remove_geq_small() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        build_tree(&mut fix, 100)?;
        remove_geq_and_verify(&mut fix, 50)
    }

    #[test]
    fn remove_lt_small() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let count = 100;
        build_tree(&mut fix, count)?;
        remove_lt_and_verify(&mut fix, count, 50)
    }

    #[test]
    fn remove_geq_large() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let nr_entries = 10_000;
        build_tree(&mut fix, nr_entries)?;

        // FIXME: if this is too high we run out of space I think
        let nr_loops = 50;

        for i in 0..nr_loops {
            eprintln!("loop {}", i);
            let scope = scope_id::new_scope(fix.tm.scopes());
            let mut fix = fix.clone(ReferenceContext::Scoped(scope.id));
            let cut = rand::thread_rng().gen_range(0..nr_entries);
            remove_geq_and_verify(&mut fix, cut)?;
        }

        Ok(())
    }

    #[test]
    fn remove_lt_large() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let nr_entries = 10_000;
        build_tree(&mut fix, nr_entries)?;

        let nr_loops = 50;

        for i in 0..nr_loops {
            eprintln!("loop {}", i);
            let scope = scope_id::new_scope(fix.tm.scopes());
            let mut fix = fix.clone(ReferenceContext::Scoped(scope.id));
            let cut = rand::thread_rng().gen_range(0..nr_entries);
            remove_lt_and_verify(&mut fix, nr_entries, cut)?;
        }

        Ok(())
    }
    #[test]
    fn remove_geq_split() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;

        let cut = 150;
        let split = |k: u32, v: Value| {
            if k + v.len > cut {
                Some((
                    k,
                    Value {
                        v: v.v,
                        len: cut - k,
                    },
                ))
            } else {
                Some((k, v))
            }
        };

        fix.insert(100, &Value { v: 200, len: 100 })?;
        fix.tree.remove_geq(150, &remove::mk_val_fn(split))?;

        ensure!(fix.tree.check()? == 1);
        ensure!(fix.tree.lookup(100)?.unwrap() == Value { v: 200, len: 50 });

        Ok(())
    }

    #[test]
    fn remove_lt_split() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;

        let cut = 150;
        let split = |k: u32, v: Value| {
            if k < cut && k + v.len >= cut {
                Some((
                    cut,
                    Value {
                        v: v.v,
                        len: (k + v.len) - cut,
                    },
                ))
            } else {
                Some((k, v))
            }
        };

        fix.insert(100, &Value { v: 200, len: 100 })?;
        fix.tree.remove_lt(150, &remove::mk_val_fn(split))?;

        ensure!(fix.tree.check()? == 1);
        ensure!(fix.tree.lookup(150)?.unwrap() == Value { v: 200, len: 50 });

        Ok(())
    }

    #[test]
    fn remove_range_small() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let range_begin = 150;
        let range_end = 175;

        let split_low = move |k: u32, v: Value| {
            if k + v.len > range_begin {
                Some((
                    k,
                    Value {
                        v: v.v,
                        len: range_begin - k,
                    },
                ))
            } else {
                Some((k, v))
            }
        };

        let split_high = move |k: u32, v: Value| {
            if k < range_end && k + v.len >= range_end {
                Some((
                    range_end,
                    Value {
                        v: v.v,
                        len: (k + v.len) - range_end,
                    },
                ))
            } else {
                Some((k, v))
            }
        };

        fix.insert(100, &Value { v: 200, len: 100 })?;
        fix.tree.remove_range(
            range_begin,
            range_end,
            &remove::mk_val_fn(split_high),
            &remove::mk_val_fn(split_low),
        )?;

        ensure!(fix.tree.check()? == 2);
        ensure!(fix.tree.lookup(100)?.unwrap() == Value { v: 200, len: 50 });
        ensure!(fix.tree.lookup(175)?.unwrap() == Value { v: 200, len: 25 });

        Ok(())
    }

    #[test]
    fn remove_range_large() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let nr_entries = 500;

        for i in 0..nr_entries {
            fix.insert(i * 10, &Value { v: i * 3, len: 10 })?;
        }
        fix.commit()?;

        let range_begin = 1001;
        let range_end = 2005;

        let split_low = |k: u32, v: Value| {
            if k + v.len > range_begin {
                Some((
                    k,
                    Value {
                        v: v.v,
                        len: range_begin - k,
                    },
                ))
            } else {
                Some((k, v))
            }
        };

        let split_high = |k: u32, v: Value| {
            if k < range_end && k + v.len >= range_end {
                Some((
                    range_end,
                    Value {
                        v: v.v,
                        len: (k + v.len) - range_end,
                    },
                ))
            } else {
                Some((k, v))
            }
        };

        fix.tree.remove_range(
            range_begin,
            range_end,
            &remove::mk_val_fn(split_low),
            &remove::mk_val_fn(split_high),
        )?;
        // fix.tree.remove_lt(range_end, split_high)?;

        // FIXME: use lookup_range() to verify
        /*
                let mut c = fix.tree.cursor(0)?;
                loop {
                    let (k, v) = c.get()?.unwrap();
                    eprintln!("{}: {:?}", k, v);

                    if !c.next_entry()? {
                        break;
                    }
                }
        */

        /*
        ensure!(fix.tree.check()? == 2);
        ensure!(fix.tree.lookup(100)?.unwrap() == Value { v: 200, len: 50 });
        ensure!(fix.tree.lookup(175)?.unwrap() == Value { v: 200, len: 25 });
        */

        Ok(())
    }
}

//---------------------------------
