use anyhow::{ensure, Result};
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use crate::block_cache::*;
use crate::btree::insert;
use crate::btree::lookup;
use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::btree::remove;
use crate::packed_array::*;

//-------------------------------------------------------------------------

pub struct BTree<V: Serializable + Copy, INodeR, INodeW, LNodeR, LNodeW> {
    cache: Arc<NodeCache>,
    root: NodePtr,
    phantom_v: std::marker::PhantomData<V>,
    phantom_inode_r: std::marker::PhantomData<INodeR>,
    phantom_inode_w: std::marker::PhantomData<INodeW>,
    phantom_lnode_r: std::marker::PhantomData<LNodeR>,
    phantom_lnode_w: std::marker::PhantomData<LNodeW>,
}

impl<
        V: Serializable + Copy,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    pub fn open_tree(cache: Arc<NodeCache>, root: NodePtr) -> Self {
        Self {
            cache,
            root,
            phantom_v: std::marker::PhantomData,
            phantom_inode_r: std::marker::PhantomData,
            phantom_inode_w: std::marker::PhantomData,
            phantom_lnode_r: std::marker::PhantomData,
            phantom_lnode_w: std::marker::PhantomData,
        }
    }

    pub fn empty_tree(cache: Arc<NodeCache>) -> Result<Self> {
        let node = cache.new_node::<V, LNodeW>(true)?;
        let root = node.n_ptr();

        Ok(Self {
            cache,
            root,
            phantom_v: std::marker::PhantomData,
            phantom_inode_r: std::marker::PhantomData,
            phantom_inode_w: std::marker::PhantomData,
            phantom_lnode_r: std::marker::PhantomData,
            phantom_lnode_w: std::marker::PhantomData,
        })
    }

    // FIXME: name clash with trait
    pub fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            root: self.root,
            phantom_v: std::marker::PhantomData,
            phantom_inode_r: std::marker::PhantomData,
            phantom_inode_w: std::marker::PhantomData,
            phantom_lnode_r: std::marker::PhantomData,
            phantom_lnode_w: std::marker::PhantomData,
        }
    }

    pub fn root(&self) -> NodePtr {
        self.root
    }

    //-------------------------------

    pub fn lookup(&self, key: u32) -> Result<Option<V>> {
        lookup::lookup::<V, INodeR, LNodeR>(&self.cache, self.root, key)
    }

    pub fn insert(&mut self, key: u32, value: &V) -> Result<()> {
        self.root =
            insert::insert::<V, INodeW, LNodeW>(self.cache.as_ref(), self.root, key, value)?;
        Ok(())
    }

    pub fn remove(&mut self, key: u32) -> Result<()> {
        let root = remove::remove::<V, INodeW, LNodeW>(self.cache.as_ref(), self.root, key)?;
        self.root = root;
        Ok(())
    }

    pub fn remove_geq(&mut self, key: u32, val_fn: &ValFn<V>) -> Result<()> {
        let new_root =
            remove::remove_geq::<V, INodeW, LNodeW>(self.cache.as_ref(), self.root, key, val_fn)?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove_lt(&mut self, key: u32, val_fn: &ValFn<V>) -> Result<()> {
        let new_root =
            remove::remove_lt::<V, INodeW, LNodeW>(self.cache.as_ref(), self.root, key, val_fn)?;
        self.root = new_root;
        Ok(())
    }

    //-------------------------------

    /// Returns a vec of key, value pairs
    pub fn lookup_range(
        &self,
        key_begin: u32,
        key_end: u32,
        select_above: &ValFn<V>,
        select_below: &ValFn<V>,
    ) -> Result<Vec<(u32, V)>> {
        lookup::lookup_range::<V, INodeR, LNodeR>(
            self.cache.as_ref(),
            self.root,
            key_begin,
            key_end,
            select_below,
            select_above,
        )
    }

    pub fn remove_range(
        &mut self,
        key_begin: u32,
        key_end: u32,
        val_lt: &ValFn<V>,
        val_geq: &ValFn<V>,
    ) -> Result<()> {
        self.root = remove::remove_range::<V, INodeW, LNodeW>(
            self.cache.as_ref(),
            self.root,
            key_begin,
            key_end,
            val_lt,
            val_geq,
        )?;
        Ok(())
    }

    //-------------------------------

    fn check_keys_<NV: Serializable, Node: NodeR<NV, SharedProxy>>(
        node: &Node,
        key_min: u32,
        key_max: Option<u32>,
    ) -> Result<()> {
        // check the keys
        let mut last = None;
        for i in 0..node.nr_entries() {
            let k = node.get_key(i);
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
        Ok(())
    }

    fn check_(
        &self,
        n_ptr: NodePtr,
        key_min: u32,
        key_max: Option<u32>,
        seen: &mut BTreeSet<u32>,
    ) -> Result<u32> {
        let mut total = 0;

        ensure!(!seen.contains(&n_ptr.loc));
        seen.insert(n_ptr.loc);

        if self.cache.is_internal(n_ptr)? {
            let node: INodeR = self.cache.read(n_ptr)?;

            Self::check_keys_(&node, key_min, key_max)?;

            for i in 0..node.nr_entries() {
                let kmin = node.get_key(i);
                // FIXME: redundant if, get_key_safe will handle it
                let kmax = if i == node.nr_entries() - 1 {
                    None
                } else {
                    node.get_key_safe(i + 1)
                };
                let loc = node.get_value(i);
                total += self.check_(loc, kmin, kmax, seen)?;
            }
        } else {
            let node: LNodeR = self.cache.read(n_ptr)?;
            Self::check_keys_(&node, key_min, key_max)?;
            total += node.nr_entries() as u32;
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

/*
pub fn btree_refs(r_proxy: &SharedProxy, queue: &mut VecDeque<BlockRef>) {
    let flags = read_flags(&r_proxy).expect("couldn't read node");

    match flags {
        BTreeFlags::Internal => {
            // FIXME: hard coded for now.  No point fixing this until we've switched
            // to log based transactions.
            let node = crate::btree::simple_node::SimpleNode::<NodePtr, SharedProxy>::open(
                r_proxy.loc(),
                r_proxy.clone(),
            )
            .unwrap();
            for i in 0..node.nr_entries.get() {
                queue.push_back(BlockRef::Metadata(node.values.get(i as usize).loc));
            }
        }
        BTreeFlags::Leaf => {
            // FIXME: values should be refs, except in the btree unit tests
        }
    }
}
*/

//-------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::btree::simple_node::*;
    use crate::core::*;
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

    type TestTree = BTree<
        Value,
        SimpleNode<NodePtr, SharedProxy>,
        SimpleNode<NodePtr, ExclusiveProxy>,
        SimpleNode<Value, SharedProxy>,
        SimpleNode<Value, ExclusiveProxy>,
    >;

    #[allow(dead_code)]
    struct Fixture {
        engine: Arc<dyn IoEngine>,
        cache: Arc<NodeCache>,
        tree: TestTree,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, nr_data_blocks: u64) -> Result<Self> {
            let engine = mk_engine(nr_metadata_blocks);
            let block_cache = Arc::new(BlockCache::new(engine.clone(), 16)?);
            let node_cache = Arc::new(NodeCache::new(block_cache));
            let tree = BTree::empty_tree(node_cache.clone())?;

            Ok(Self {
                engine,
                cache: node_cache,
                tree,
            })
        }

        fn clone(&self) -> Self {
            Self {
                engine: self.engine.clone(),
                cache: self.cache.clone(),
                tree: self.tree.clone(),
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
            let roots = vec![self.tree.root().loc];

            // FIXME: finish
            Ok(())
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

        fix.tree.remove_geq(100, &mk_val_fn(no_split))?;
        ensure!(fix.tree.check()? == 0);
        Ok(())
    }

    #[test]
    fn remove_lt_empty() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let no_split = |k: u32, v: Value| Some((k, v));

        fix.tree.remove_lt(100, &mk_val_fn(no_split))?;
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
        fix.tree.remove_geq(cut, &mk_val_fn(no_split))?;
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
        fix.tree.remove_lt(cut, &mk_val_fn(no_split))?;
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
            let mut fix = fix.clone();
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
            let mut fix = fix.clone();
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
        fix.tree.remove_geq(150, &mk_val_fn(split))?;

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
        fix.tree.remove_lt(150, &mk_val_fn(split))?;

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
            &mk_val_fn(split_high),
            &mk_val_fn(split_low),
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
            &mk_val_fn(split_low),
            &mk_val_fn(split_high),
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
