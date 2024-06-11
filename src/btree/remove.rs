use anyhow::anyhow;
use anyhow::Result;

use crate::block_cache::*;
use crate::btree::node::*;
use crate::btree::node_cache::*;
use crate::packed_array::*;

use crate::btree::BTree;

//-------------------------------------------------------------------------

impl<
        V: Serializable + Copy,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    fn remove_internal(&mut self, n_ptr: NodePtr, key: u32) -> Result<NodeResult> {
        let mut node = self
            .cache
            .shadow::<NodePtr, INodeW>(n_ptr, self.snap_time)?;

        let mut idx = node.lower_bound(key);
        if idx < 0 {
            return Ok(NodeResult::single(&node));
        }

        if idx as usize == node.nr_entries() {
            idx -= 1;
        }

        let idx = idx as usize;

        let child = node.get_value(idx);
        let res = self.remove_recurse(child, key)?;
        self.node_insert_result(&mut node, idx, &res)
    }

    fn remove_leaf(&mut self, n_ptr: NodePtr, key: u32) -> Result<NodeResult> {
        let mut node = self.cache.shadow::<V, LNodeW>(n_ptr, 0)?;

        let idx = node.lower_bound(key);
        if (idx >= 0) && ((idx as usize) < node.nr_entries()) {
            let idx = idx as usize;
            if node.get_key(idx) == key {
                node.remove_at(idx);
            }
        }
        Ok(NodeResult::single(&node))
    }

    fn remove_recurse(&mut self, n_ptr: NodePtr, key: u32) -> Result<NodeResult> {
        if self.cache.is_internal(n_ptr)? {
            self.remove_internal(n_ptr, key)
        } else {
            self.remove_leaf(n_ptr, key)
        }
    }

    pub fn remove_(&mut self, key: u32) -> Result<NodePtr> {
        use NodeResult::*;

        match self.remove_recurse(self.root, key)? {
            Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
            Pair(left, right) => {
                let mut parent: INodeW = self.cache.new_node(false)?;
                parent.append(
                    &[left.key_min.unwrap(), right.key_min.unwrap()],
                    &[left.n_ptr, right.n_ptr],
                );
                Ok(parent.n_ptr())
            }
        }
    }

    pub fn remove(&mut self, key: u32) -> Result<()> {
        let root = self.remove_(key)?;
        self.root = root;
        Ok(())
    }
}

//-------------------------------------------------------------------------

// All usizes are indexes
// FIXME: Trim ops should hold the key they're trimming against too
enum NodeOp {
    Recurse(usize),
    TrimLt(usize),
    TrimGeq(usize),
    Erase(usize, usize),
}

type NodeProgram = Vec<NodeOp>;

fn lt_prog<V: Serializable, N: NodeW<V, ExclusiveProxy>>(node: &N, key: u32) -> NodeProgram {
    use NodeOp::*;

    if node.nr_entries() == 0 {
        return vec![];
    }

    match node.lower_bound(key) {
        idx if idx < 0 => {
            vec![]
        }
        idx if node.get_key(idx as usize) == key => {
            vec![Erase(0, idx as usize)]
        }
        idx => {
            vec![Erase(0, idx as usize), TrimLt(idx as usize)]
        }
    }
}

impl<
        V: Serializable + Copy,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    fn remove_lt_internal(
        &mut self,
        n_ptr: NodePtr,
        key: u32,
        split_fn: &ValFn<V>,
    ) -> Result<NodeResult> {
        use NodeOp::*;

        let mut node = self
            .cache
            .shadow::<NodePtr, INodeW>(n_ptr, self.snap_time)?;
        let prog = lt_prog(&node, key);

        let mut delta = 0;
        for op in prog {
            match op {
                Recurse(_) => {
                    panic!("unexpected recurse");
                }
                TrimLt(idx) => {
                    let idx = idx - delta;
                    let res = self.remove_lt_recurse(node.get_value(idx), key, split_fn)?;

                    // remove_lt cannot cause a Pair result, so we don't need to preserve the result
                    self.node_insert_result(&mut node, idx, &res)?;
                }
                TrimGeq(_) => {
                    panic!("unexpected trim geq");
                }
                Erase(idx_b, idx_e) => {
                    node.erase(idx_b - delta, idx_e - delta);
                    delta += idx_e - idx_b;
                }
            }
        }

        Ok(NodeResult::single(&node))
    }

    fn remove_lt_leaf(
        &mut self,
        n_ptr: NodePtr,
        key: u32,
        split_fn: &ValFn<V>,
    ) -> Result<NodeResult> {
        use NodeOp::*;

        let mut node = self.cache.shadow::<V, LNodeW>(n_ptr, self.snap_time)?;
        let prog = lt_prog(&node, key);

        let mut delta = 0;
        for op in prog {
            match op {
                Recurse(_) => {
                    panic!("unexpected recurse");
                }
                TrimLt(idx) => match split_fn(node.get_key(idx), node.get_value(idx)) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, new_value)) => {
                        node.overwrite(idx, new_key, &new_value);
                    }
                },
                TrimGeq(_) => {
                    panic!("unexpected trim geq");
                }
                Erase(idx_b, idx_e) => {
                    node.erase(idx_b - delta, idx_e - delta);
                    delta += idx_e - idx_b;
                }
            }
        }

        Ok(NodeResult::single(&node))
    }

    pub fn remove_lt_recurse(
        &mut self,
        n_ptr: NodePtr,
        key: u32,
        split_fn: &ValFn<V>,
    ) -> Result<NodeResult> {
        if self.cache.is_internal(n_ptr)? {
            self.remove_lt_internal(n_ptr, key, split_fn)
        } else {
            self.remove_lt_leaf(n_ptr, key, split_fn)
        }
    }

    fn remove_lt_(&mut self, root: NodePtr, key: u32, split_fn: &ValFn<V>) -> Result<NodePtr> {
        match self.remove_lt_recurse(root, key, split_fn)? {
            NodeResult::Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
            NodeResult::Pair(_, _) => Err(anyhow!("remove_lt increase nr entries somehow")),
        }
    }

    pub fn remove_lt(&mut self, key: u32, val_fn: &ValFn<V>) -> Result<()> {
        self.root = self.remove_lt_(self.root, key, val_fn)?;
        Ok(())
    }
}
//-------------------------------------------------------------------------

fn geq_prog<V: Serializable, N: NodeW<V, ExclusiveProxy>>(node: &N, key: u32) -> NodeProgram {
    use NodeOp::*;

    let nr_entries = node.nr_entries();
    if nr_entries == 0 {
        return vec![];
    }

    match node.lower_bound(key) {
        idx if idx < 0 => {
            vec![Erase(0, node.nr_entries())]
        }
        idx if node.get_key(idx as usize) == key => {
            vec![Erase(idx as usize, nr_entries)]
        }
        idx => {
            let idx = idx as usize;
            if idx + 1 < nr_entries {
                vec![TrimGeq(idx), Erase(idx + 1, nr_entries)]
            } else {
                vec![TrimGeq(idx)]
            }
        }
    }
}

impl<
        V: Serializable + Copy,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    fn remove_geq_internal(
        &mut self,
        n_ptr: NodePtr,
        key: u32,
        split_fn: &ValFn<V>,
    ) -> Result<NodeResult> {
        use NodeOp::*;

        let mut node = self
            .cache
            .shadow::<NodePtr, INodeW>(n_ptr, self.snap_time)?;
        let prog = geq_prog(&node, key);

        let mut delta = 0;
        for op in prog {
            match op {
                Recurse(_) => {
                    panic!("unexpected recurse");
                }
                TrimLt(_) => {
                    panic!("unexpected thin lt");
                }
                TrimGeq(idx) => {
                    let idx = idx - delta;
                    let res = self.remove_geq_recurse(node.get_value(idx), key, split_fn)?;

                    // remove_geq cannot cause a Pair result, so this can't split node.
                    self.node_insert_result(&mut node, idx, &res)?;
                }
                Erase(idx_b, idx_e) => {
                    node.erase(idx_b - delta, idx_e - delta);
                    delta += idx_e - idx_b;
                }
            }
        }

        Ok(NodeResult::single(&node))
    }

    fn remove_geq_leaf(
        &mut self,
        n_ptr: NodePtr,
        key: u32,
        split_fn: &ValFn<V>,
    ) -> Result<NodeResult> {
        use NodeOp::*;

        let mut node = self.cache.shadow::<V, LNodeW>(n_ptr, self.snap_time)?;
        let prog = geq_prog(&node, key);

        let mut delta = 0;
        for op in prog {
            match op {
                Recurse(_) => {
                    panic!("unexpected recurse");
                }
                TrimLt(_) => {
                    panic!("unexpected trim lt");
                }
                TrimGeq(idx) => match split_fn(node.get_key(idx), node.get_value(idx)) {
                    None => {
                        node.remove_at(idx);
                    }
                    Some((new_key, new_value)) => {
                        node.overwrite(idx, new_key, &new_value);
                    }
                },
                Erase(idx_b, idx_e) => {
                    node.erase(idx_b - delta, idx_e - delta);
                    delta += idx_e - idx_b;
                }
            }
        }

        Ok(NodeResult::single(&node))
    }

    fn remove_geq_recurse(
        &mut self,
        n_ptr: NodePtr,
        key: u32,
        split_fn: &ValFn<V>,
    ) -> Result<NodeResult> {
        if self.cache.is_internal(n_ptr)? {
            self.remove_geq_internal(n_ptr, key, split_fn)
        } else {
            self.remove_geq_leaf(n_ptr, key, split_fn)
        }
    }

    fn remove_geq_(&mut self, root: NodePtr, key: u32, split_fn: &ValFn<V>) -> Result<NodePtr> {
        match self.remove_geq_recurse(root, key, split_fn)? {
            NodeResult::Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
            NodeResult::Pair(_, _) => Err(anyhow!("remove_geq increased nr of entries")),
        }
    }

    pub fn remove_geq(&mut self, key: u32, val_fn: &ValFn<V>) -> Result<()> {
        self.root = self.remove_geq_(self.root, key, val_fn)?;
        Ok(())
    }
}
//-------------------------------------------------------------------------

// Categorises where a given key is to be found.  usizes are indexes into the
// key array.
enum KeyLoc {
    Within(usize),
    Exact(usize),
}

// The key must be >= to the first key in the node.
fn key_search<V: Serializable, N: NodeW<V, ExclusiveProxy>>(node: &N, k: u32) -> KeyLoc {
    let idx = node.lower_bound(k);

    assert!(idx >= 0);
    let idx = idx as usize;

    if node.get_key(idx) == k {
        KeyLoc::Exact(idx)
    } else {
        KeyLoc::Within(idx)
    }
}

// All indexes in the program are *before* any operations were executed
fn range_split<V: Serializable, N: NodeW<V, ExclusiveProxy>>(
    node: &N,
    key_begin: u32,
    key_end: u32,
) -> NodeProgram {
    use KeyLoc::*;
    use NodeOp::*;

    if node.is_empty() {
        // no entries
        return vec![];
    }

    if key_end <= node.get_key(0) {
        // remove range is before this node
        return vec![];
    }

    let b = key_search(node, key_begin);
    let e = key_search(node, key_end);

    match (b, e) {
        // Recurse special cases:
        (Exact(idx1), Within(idx2)) if idx1 == idx2 => {
            vec![Recurse(idx1)]
        }
        (Within(idx1), Within(idx2)) if idx1 == idx2 => {
            vec![Recurse(idx1)]
        }
        (Within(idx1), Exact(idx2)) if (idx2 - idx1) == 1 => {
            vec![Recurse(idx1)]
        }

        // General cases:
        (Exact(idx1), Exact(idx2)) => {
            vec![Erase(idx1, idx2)]
        }
        (Exact(idx1), Within(idx2)) => {
            if idx2 == idx1 {
                vec![TrimGeq(idx1)]
            } else {
                vec![Erase(idx1, idx2), TrimLt(idx2)]
            }
        }
        (Within(idx1), Exact(idx2)) => {
            if idx2 - idx1 == 1 {
                vec![TrimGeq(idx1)]
            } else {
                vec![TrimGeq(idx1), Erase(idx1 + 1, idx2)]
            }
        }
        (Within(idx1), Within(idx2)) => {
            if idx2 - idx1 == 1 {
                vec![TrimGeq(idx1), TrimLt(idx2)]
            } else {
                vec![TrimGeq(idx1), Erase(idx1 + 1, idx2), TrimLt(idx2)]
            }
        }
    }
}

impl<
        V: Serializable + Copy,
        INodeR: NodeR<NodePtr, SharedProxy>,
        INodeW: NodeW<NodePtr, ExclusiveProxy>,
        LNodeR: NodeR<V, SharedProxy>,
        LNodeW: NodeW<V, ExclusiveProxy>,
    > BTree<V, INodeR, INodeW, LNodeR, LNodeW>
{
    fn remove_range_internal(
        &mut self,
        n_ptr: NodePtr,
        key_begin: u32,
        key_end: u32,
        split_lt: &ValFn<V>,
        split_geq: &ValFn<V>,
    ) -> Result<NodeResult> {
        use NodeOp::*;

        let mut node = self
            .cache
            .shadow::<NodePtr, INodeW>(n_ptr, self.snap_time)?;
        let prog = range_split(&node, key_begin, key_end);
        let prog_len = prog.len();

        let mut delta = 0;
        for op in prog {
            match op {
                Recurse(idx) => {
                    assert!(prog_len == 1);
                    return self.remove_range_recurse(
                        node.get_value(idx),
                        key_begin,
                        key_end,
                        split_lt,
                        split_geq,
                    );
                }

                // The rest of the ops are guaranteed to return a Single, so we don't need
                // to do anything fancy aggregating them.
                TrimLt(idx) => {
                    let idx = idx - delta;

                    let res = self.remove_lt_recurse(node.get_value(idx), key_end, split_lt)?;
                    self.node_insert_result(&mut node, idx, &res)?;
                }
                TrimGeq(idx) => {
                    let idx = idx - delta;
                    let res = self.remove_geq_recurse(node.get_value(idx), key_begin, split_geq)?;
                    self.node_insert_result(&mut node, idx, &res)?;
                }
                Erase(idx_b, idx_e) => {
                    node.erase(idx_b - delta, idx_e - delta);
                    delta += idx_e - idx_b;
                }
            }
        }
        Ok(NodeResult::single(&node))
    }

    fn remove_range_leaf(
        &mut self,
        n_ptr: NodePtr,
        key_begin: u32,
        key_end: u32,
        split_lt: &ValFn<V>,
        split_geq: &ValFn<V>,
    ) -> Result<NodeResult> {
        use NodeOp::*;

        let mut node = self.cache.shadow::<V, LNodeW>(n_ptr, self.snap_time)?;
        let prog = range_split(&node, key_begin, key_end);
        let prog_len = prog.len();

        let mut delta = 0;
        for op in prog {
            match op {
                Recurse(idx) => {
                    assert!(prog_len == 1);

                    // This means the range hits the middle of an entry.
                    // So we'll have to split it in two.
                    let k = node.get_key(idx);
                    let v = node.get_value(idx);
                    match (split_geq(k, v), split_lt(k, v)) {
                        (None, None) => {
                            node.remove_at(idx);
                            return Ok(NodeResult::single(&node));
                        }
                        (Some((k, v)), None) => {
                            node.overwrite(idx, k, &v);
                            return Ok(NodeResult::single(&node));
                        }
                        (None, Some((k, v))) => {
                            node.overwrite(idx, k, &v);
                            return Ok(NodeResult::single(&node));
                        }
                        (Some((k1, v1)), Some((k2, v2))) => {
                            eprintln!("k1 = {:?}, k2 = {:?}", k1, k2);
                            node.overwrite(idx, k1, &v1);
                            return ensure_space(
                                self.cache.as_ref(),
                                &mut node,
                                idx,
                                |node, idx| node.insert(idx + 1, k2, &v2),
                            );
                        }
                    }
                }
                TrimLt(idx) => {
                    let idx = idx - delta;
                    match split_lt(node.get_key(idx), node.get_value(idx)) {
                        None => {
                            node.remove_at(idx);
                        }
                        Some((new_key, v)) => {
                            node.overwrite(idx, new_key, &v);
                        }
                    }
                }
                TrimGeq(idx) => {
                    let idx = idx - delta;
                    match split_geq(node.get_key(idx), node.get_value(idx)) {
                        None => {
                            node.remove_at(idx);
                        }
                        Some((new_key, v)) => {
                            node.overwrite(idx, new_key, &v);
                        }
                    }
                }
                Erase(idx_b, idx_e) => {
                    node.erase(idx_b - delta, idx_e - delta);
                    delta += idx_e - idx_b;
                }
            }
        }

        Ok(NodeResult::single(&node))
    }

    fn remove_range_recurse(
        &mut self,
        n_ptr: NodePtr,
        key_begin: u32,
        key_end: u32,
        split_lt: &ValFn<V>,
        split_geq: &ValFn<V>,
    ) -> Result<NodeResult> {
        if self.cache.is_internal(n_ptr)? {
            self.remove_range_internal(n_ptr, key_begin, key_end, split_lt, split_geq)
        } else {
            self.remove_range_leaf(n_ptr, key_begin, key_end, split_lt, split_geq)
        }
    }

    pub fn remove_range_(
        &mut self,
        root: NodePtr,
        key_begin: u32,
        key_end: u32,
        split_lt: &ValFn<V>,
        split_geq: &ValFn<V>,
    ) -> Result<NodePtr> {
        use NodeResult::*;

        match self.remove_range_recurse(root, key_begin, key_end, split_lt, split_geq)? {
            Single(NodeInfo { n_ptr, .. }) => Ok(n_ptr),
            Pair(left, right) => {
                let mut parent: INodeW = self.cache.new_node(false)?;
                parent.append(
                    &[left.key_min.unwrap(), right.key_min.unwrap()],
                    &[left.n_ptr, right.n_ptr],
                );
                Ok(parent.n_ptr())
            }
        }
    }

    pub fn remove_range(
        &mut self,
        key_begin: u32,
        key_end: u32,
        val_lt: &ValFn<V>,
        val_geq: &ValFn<V>,
    ) -> Result<()> {
        self.root = self.remove_range_(self.root, key_begin, key_end, val_lt, val_geq)?;
        Ok(())
    }
}

//-------------------------------------------------------------------------
