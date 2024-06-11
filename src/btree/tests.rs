//-------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use anyhow::{ensure, Result};
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::io::{self, Read, Write};
    use std::sync::Arc;
    use thinp::io_engine::*;

    use crate::block_cache::*;
    use crate::btree::node::*;
    use crate::btree::node_cache::*;
    use crate::btree::simple_node::*;
    use crate::btree::BTree;
    use crate::buddy_alloc::*;
    use crate::core::*;
    use crate::packed_array::*;

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
        snap_time: u32,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, _nr_data_blocks: u64) -> Result<Self> {
            // We only cope with powers of two atm.
            assert!(nr_metadata_blocks.count_ones() == 1);

            let engine = mk_engine(nr_metadata_blocks);
            let block_cache = Arc::new(BlockCache::new(engine.clone(), 16)?);
            let alloc = BuddyAllocator::new(nr_metadata_blocks as u64);
            let node_cache = Arc::new(NodeCache::new(block_cache, alloc));
            let tree = BTree::empty_tree(node_cache.clone())?;

            Ok(Self {
                engine,
                cache: node_cache,
                tree,
                snap_time: 0,
            })
        }

        fn snap(&mut self) -> Self {
            self.snap_time += 1;
            Self {
                engine: self.engine.clone(),
                cache: self.cache.clone(),
                tree: self.tree.snap(self.snap_time),
                snap_time: self.snap_time,
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
            // let roots = vec![self.tree.root().loc];

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
        // FIXME: put back once garbage collection is working again
        // let mut fix = Fixture::new(1024, 102400)?;
        let mut fix = Fixture::new(4096, 102400)?;
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
        // let nr_loops = 50;

        //for i in 0..nr_loops {
        // eprintln!("loop {}", i);
        //let mut fix = fix.clone();
        let cut = rand::thread_rng().gen_range(0..nr_entries);
        remove_geq_and_verify(&mut fix, cut)?;
        // }

        Ok(())
    }

    #[test]
    fn remove_lt_large() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let nr_entries = 10_000;
        build_tree(&mut fix, nr_entries)?;

        // let nr_loops = 50;

        // for i in 0..nr_loops {
        // eprintln!("loop {}", i);
        // let mut fix = fix.clone();
        let cut = rand::thread_rng().gen_range(0..nr_entries);
        remove_lt_and_verify(&mut fix, nr_entries, cut)?;
        // }

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
