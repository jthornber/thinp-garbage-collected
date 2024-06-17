use anyhow::Result;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::btree::split::Split;
use crate::btree::BTree;
use crate::packed_array::*;

//-------------------------------------------------------------------------

impl<
        V: Serializable + Copy,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    pub fn lookup(&self, key: Key) -> Result<Option<V>> {
        let mut n_ptr = self.root;

        loop {
            if self.cache.is_internal(n_ptr)? {
                let node: INodeR = self.cache.read(n_ptr)?;

                let idx = node.lower_bound(key);
                if idx < 0 || idx >= node.nr_entries() as isize {
                    return Ok(None);
                }

                n_ptr = node.get_value(idx as usize);
            } else {
                let node: LNodeR = self.cache.read(n_ptr)?;

                let idx = node.lower_bound(key);
                if idx < 0 || idx >= node.nr_entries() as isize {
                    return Ok(None);
                }

                return if node.get_key(idx as usize) == key {
                    Ok(node.get_value_safe(idx as usize))
                } else {
                    Ok(None)
                };
            }
        }
    }
}

//-------------------------------------------------------------------------

// All usizes are indexes
#[derive(Debug, PartialEq, Eq)]
enum NodeOp {
    AboveAndBelow(usize),
    Above(usize),
    Below(usize),
    All(usize),
}

type NodeProgram = Vec<NodeOp>;

fn lower_bound<V: Serializable, N: NodeR<V, SharedProxy>>(node: &N, key: Key) -> usize {
    let idx = node.lower_bound(key);
    if idx < 0 {
        0
    } else {
        idx as usize
    }
}

fn get_prog<V: Serializable, N: NodeR<V, SharedProxy>>(
    node: &N,
    key_begin: Key,
    key_end: Key,
) -> NodeProgram {
    use NodeOp::*;

    let mut prog = Vec::new();

    if node.is_empty() {
        return prog;
    }

    let mut idx_b = lower_bound(node, key_begin);
    let idx_e = lower_bound(node, key_end);

    if node.get_key(idx_b) >= key_end {
        return prog;
    }

    if idx_b == idx_e {
        prog.push(AboveAndBelow(idx_b));
        return prog;
    }

    if node.get_key(idx_b) < key_begin {
        prog.push(Above(idx_b));
        idx_b += 1;
    }

    for i in idx_b..idx_e {
        prog.push(All(i));
    }

    if node.get_key(idx_e) < key_end {
        prog.push(Below(idx_e));
    }

    prog
}

fn get_prog_above<V: Serializable, N: NodeR<V, SharedProxy>>(node: &N, key: Key) -> NodeProgram {
    use NodeOp::*;

    let mut prog = Vec::new();

    if node.is_empty() {
        return prog;
    }

    let mut idx = lower_bound(node, key);

    if node.get_key(idx) < key {
        prog.push(Above(idx));
        idx += 1;
    }

    for i in idx..node.nr_entries() {
        prog.push(All(i));
    }

    prog
}

fn get_prog_below<V: Serializable, N: NodeR<V, SharedProxy>>(node: &N, key: Key) -> NodeProgram {
    use NodeOp::*;

    let mut prog = Vec::new();

    if node.is_empty() {
        return prog;
    }

    let idx = lower_bound(node, key);

    for i in idx..node.nr_entries() {
        prog.push(All(i));
    }

    if node.get_key(idx) < key {
        prog.push(AboveAndBelow(idx));
    }

    prog
}

impl<
        V: Serializable + Copy + Split,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    fn select_above(&self, n_ptr: NodePtr, key: Key, results: &mut Vec<(Key, V)>) -> Result<()> {
        use NodeOp::*;

        if self.cache.is_internal(n_ptr)? {
            let node: INodeR = self.cache.read(n_ptr)?;

            for op in get_prog_above(&node, key) {
                match op {
                    AboveAndBelow(_) | Below(_) => {
                        unreachable!();
                    }
                    Above(idx) => {
                        self.select_above(node.get_value(idx), key, results)?;
                    }
                    All(idx) => {
                        self.select_all(node.get_value(idx), results)?;
                    }
                }
            }
        } else {
            let node: LNodeR = self.cache.read(n_ptr)?;
            for op in get_prog_above::<V, LNodeR>(&node, key) {
                match op {
                    AboveAndBelow(_) => {
                        unreachable!();
                    }
                    Below(_) => {
                        unreachable!();
                    }
                    Above(idx) => {
                        if let Some((nk, nv)) =
                            node.get_value(idx).select_geq(node.get_key(idx), key)
                        {
                            results.push((nk, nv));
                        }
                    }
                    All(idx) => {
                        results.push((node.get_key(idx), node.get_value(idx)));
                    }
                }
            }
        }

        Ok(())
    }

    fn select_below(&self, n_ptr: NodePtr, key: Key, results: &mut Vec<(Key, V)>) -> Result<()> {
        use NodeOp::*;

        if self.cache.is_internal(n_ptr)? {
            let node: INodeR = self.cache.read(n_ptr)?;

            for op in get_prog_below(&node, key) {
                match op {
                    AboveAndBelow(_) | Above(_) => {
                        unreachable!();
                    }
                    Below(idx) => {
                        self.select_below(node.get_value(idx), key, results)?;
                    }
                    All(idx) => {
                        self.select_all(node.get_value(idx), results)?;
                    }
                }
            }
        } else {
            let node: LNodeR = self.cache.read(n_ptr)?;
            for op in get_prog_below::<V, LNodeR>(&node, key) {
                match op {
                    AboveAndBelow(_) => {
                        unreachable!();
                    }
                    Above(_) => {
                        unreachable!();
                    }
                    Below(idx) => {
                        if let Some((nk, nv)) =
                            node.get_value(idx).select_lt(node.get_key(idx), key)
                        {
                            results.push((nk, nv));
                        }
                    }
                    All(idx) => {
                        results.push((node.get_key(idx), node.get_value(idx)));
                    }
                }
            }
        }

        Ok(())
    }

    fn select_all(&self, n_ptr: NodePtr, results: &mut Vec<(Key, V)>) -> Result<()> {
        if self.cache.is_internal(n_ptr)? {
            let node: INodeR = self.cache.read(n_ptr)?;
            for i in 0..node.nr_entries() {
                self.select_all(node.get_value(i), results)?;
            }
        } else {
            let node: LNodeR = self.cache.read(n_ptr)?;
            for i in 0..node.nr_entries() {
                results.push((node.get_key(i), node.get_value(i)));
            }
        }
        Ok(())
    }

    fn select_above_below(
        &self,
        n_ptr: NodePtr,
        key_begin: Key,
        key_end: Key,
        results: &mut Vec<(Key, V)>,
    ) -> Result<()> {
        use NodeOp::*;

        if self.cache.is_internal(n_ptr)? {
            let node: INodeR = self.cache.read(n_ptr)?;
            for op in get_prog(&node, key_begin, key_end) {
                match op {
                    AboveAndBelow(idx) => {
                        self.select_above_below(node.get_value(idx), key_begin, key_end, results)?;
                    }
                    Above(idx) => {
                        self.select_above(node.get_value(idx), key_begin, results)?;
                    }
                    Below(idx) => {
                        self.select_below(node.get_value(idx), key_end, results)?;
                    }
                    All(idx) => {
                        self.select_all(node.get_value(idx), results)?;
                    }
                }
            }
        } else {
            let node: LNodeR = self.cache.read(n_ptr)?;
            for op in get_prog::<V, LNodeR>(&node, key_begin, key_end) {
                match op {
                    AboveAndBelow(idx) => {
                        // we need to use both trim functions
                        if let Some((nk, nv)) =
                            node.get_value(idx).select_geq(node.get_key(idx), key_begin)
                        {
                            if let Some((nk, nv)) = nv.select_lt(nk, key_end) {
                                results.push((nk, nv));
                            }
                        }
                    }
                    Above(idx) => {
                        if let Some((nk, nv)) =
                            node.get_value(idx).select_geq(node.get_key(idx), key_begin)
                        {
                            results.push((nk, nv));
                        }
                    }
                    Below(idx) => {
                        if let Some((nk, nv)) =
                            node.get_value(idx).select_lt(node.get_key(idx), key_end)
                        {
                            results.push((nk, nv));
                        }
                    }
                    All(idx) => {
                        results.push((node.get_key(idx), node.get_value(idx)));
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns a vec of key, value pairs
    pub fn lookup_range(&self, key_begin: Key, key_end: Key) -> Result<Vec<(Key, V)>> {
        let mut results = Vec::with_capacity(16);

        // FIXME: order of select_* params changes?
        self.select_above_below(self.root, key_begin, key_end, &mut results)?;

        Ok(results)
    }
}

//-------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    struct MockNode {
        loc: MetadataBlock,
        keys: Vec<Key>,
        values: Vec<u32>,
        flags: BTreeFlags,
    }

    impl MockNode {
        fn new(loc: MetadataBlock, keys: Vec<Key>) -> Self {
            let values = keys.iter().map(|k| *k as u32).collect();
            MockNode {
                loc,
                keys,
                values,
                flags: BTreeFlags::Internal,
            }
        }
    }

    impl NodeR<u32, SharedProxy> for MockNode {
        fn open(_loc: MetadataBlock, _data: SharedProxy) -> Result<Self> {
            unimplemented!();
        }

        fn n_ptr(&self) -> NodePtr {
            NodePtr {
                loc: self.loc,
                seq_nr: 0,
            }
        }

        fn nr_entries(&self) -> usize {
            self.keys.len()
        }

        fn is_empty(&self) -> bool {
            self.keys.is_empty()
        }

        fn get_key(&self, idx: usize) -> Key {
            self.keys[idx]
        }

        fn get_key_safe(&self, idx: usize) -> Option<Key> {
            self.keys.get(idx).cloned()
        }

        fn get_value(&self, idx: usize) -> u32 {
            self.values[idx]
        }

        fn get_value_safe(&self, idx: usize) -> Option<u32> {
            self.values.get(idx).cloned()
        }

        fn lower_bound(&self, key: Key) -> isize {
            if self.is_empty() {
                return -1;
            }

            let mut lo = -1;
            let mut hi = self.nr_entries() as isize;
            while (hi - lo) > 1 {
                let mid = lo + ((hi - lo) / 2);
                let mid_key = self.keys[mid as usize];

                if mid_key == key {
                    return mid;
                }

                if mid_key < key {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }

            lo
        }

        fn get_entries(&self, b_idx: usize, e_idx: usize) -> (Vec<Key>, Vec<u32>) {
            (
                self.keys[b_idx..e_idx].to_vec(),
                self.values[b_idx..e_idx].to_vec(),
            )
        }

        fn get_flags(&self) -> BTreeFlags {
            self.flags
        }
    }

    #[test]
    fn test_get_prog() {
        use NodeOp::*;

        let tests = [
            (
                vec![10, 20, 30, 40, 50],
                15,
                35,
                vec![Above(0), All(1), Below(2)],
            ),
            (vec![10], 15, 35, vec![AboveAndBelow(0)]),
            (vec![10, 40], 15, 35, vec![AboveAndBelow(0)]),
            (vec![50, 60, 70], 15, 35, vec![]),
            (vec![50, 60, 70], 35, 100, vec![All(0), All(1), Below(2)]),
            (vec![50, 60, 70], 100, 150, vec![AboveAndBelow(2)]),
        ];

        for t in tests {
            let node = MockNode::new(0, t.0);
            let key_begin = t.1;
            let key_end = t.2;
            let prog = get_prog(&node, key_begin, key_end);
            assert_eq!(prog, t.3);
        }
    }
}
