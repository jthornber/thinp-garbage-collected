// FIXME: I'm not sure we still need the cursor, keeping this code just in case

/*
struct Frame {
    is_leaf: bool,
    loc: MetadataBlock,

    // Index into the current node
    index: usize,

    // Nr entries in current node
    nr_entries: usize,
}

pub struct Cursor<
    'a,
    V: Serializable + Copy,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
> {
    tree: &'a BTree<V, INode, LNode>,

    // Holds pairs of (loc, index, nr_entries)
    stack: Option<Vec<Frame>>,
}

fn next_<
    V: Serializable + Copy,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<V, WriteProxy>,
>(
    tree: &BTree<V, INode, LNode>,
    stack: &mut Vec<Frame>,
) -> Result<bool> {
    if stack.is_empty() {
        return Ok(false);
    }

    let frame = stack.last_mut().unwrap();

    frame.index += 1;
    if frame.index >= frame.nr_entries {
        // We need to move to the next node.
        stack.pop();
        if !next_::<V, INode, LNode>(tree, stack)? {
            return Ok(false);
        }

        let frame = stack.last().unwrap();
        let node = INode::open(frame.loc,
        let n = tree.read_node::<MetadataBlock>(frame.loc)?;

        let loc = n.values.get(frame.index);
        let n = tree.read_node::<NV>(loc)?;

        stack.push(Frame {
            loc,
            index: 0,
            nr_entries: n.nr_entries.get() as usize,
        });
    }

    Ok(true)
}

fn prev_<
    TreeV: Serializable + Copy,
    NV: Serializable,
    INode: NodeW<MetadataBlock, WriteProxy>,
    LNode: NodeW<TreeV, WriteProxy>,
>(
    tree: &BTree<TreeV, INode, LNode>,
    stack: &mut Vec<Frame>,
) -> Result<bool> {
    if stack.is_empty() {
        return Ok(false);
    }
    let frame = stack.last_mut().unwrap();
    if frame.index == 0 {
        // We need to move to the previous node.
        stack.pop();
        if !prev_::<TreeV, MetadataBlock, INode, LNode>(tree, stack)? {
            return Ok(false);
        }
        let frame = stack.last().unwrap();
        let n = tree.read_node::<MetadataBlock>(frame.loc)?;
        let loc = n.values.get(frame.index);
        let n = tree.read_node::<NV>(loc)?;
        stack.push(Frame {
            loc,
            index: n.nr_entries.get() as usize - 1,
            nr_entries: n.nr_entries.get() as usize,
        });
    } else {
        frame.index -= 1;
    }

    Ok(true)
}

impl<
        'a,
        V: Serializable + Copy,
        INode: NodeW<MetadataBlock, WriteProxy>,
        LNode: NodeW<V, WriteProxy>,
    > Cursor<'a, V, INode, LNode>
{
    fn new(tree: &'a BTree<V, INode, LNode>, key: u32) -> Result<Self> {
        let mut stack = Vec::new();
        let mut loc = tree.root();

        loop {
            if tree.is_leaf(loc)? {
                let n = tree.read_node::<V>(loc)?;
                let nr_entries = n.nr_entries.get() as usize;
                if nr_entries == 0 {
                    eprintln!("empty cursor");
                    return Ok(Self { tree, stack: None });
                }

                let mut idx = n.keys.bsearch(&key);
                if idx < 0 {
                    idx = 0;
                }

                stack.push(Frame {
                    loc,
                    index: idx as usize,
                    nr_entries,
                });

                return Ok(Self {
                    tree,
                    stack: Some(stack),
                });
            }

            let n = tree.read_node::<MetadataBlock>(loc)?;
            let nr_entries = n.nr_entries.get() as usize;

            let mut idx = n.keys.bsearch(&key);
            if idx < 0 {
                idx = 0;
            }

            // we cannot have an internal node without entries
            stack.push(Frame {
                loc,
                index: idx as usize,
                nr_entries,
            });

            loc = n.values.get(idx as usize);
        }
    }

    /// Returns (key, value) for the current position.  Returns None
    /// if the cursor has run out of values.
    pub fn get(&self) -> Result<Option<(u32, V)>> {
        match &self.stack {
            None => Ok(None),
            Some(stack) => {
                let frame = stack.last().unwrap();

                // FIXME: cache nodes in frame
                let n = self.tree.read_node::<V>(frame.loc)?;
                let k = n.keys.get(frame.index);
                let v = n.values.get(frame.index);
                Ok(Some((k, v)))
            }
        }
    }

    // Move cursor to the next entry.  Returns false if there are no more, and
    // invalidates the cursor.
    pub fn next_entry(&mut self) -> Result<bool> {
        match &mut self.stack {
            None => Ok(false),
            Some(stack) => {
                if !next_::<V, V>(self.tree, stack)? {
                    self.stack = None;
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
    }

    // Move cursor to the previous entry.  Returns false if there are no more, and
    // invalidates the cursor.
    pub fn prev_entry(&mut self) -> Result<bool> {
        match &mut self.stack {
            None => Ok(false),
            Some(stack) => {
                if !prev_::<V, V>(self.tree, stack)? {
                    self.stack = None;
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
    }

    /// Returns true if the cursor is at the first entry.
    pub fn is_first(&self) -> bool {
        match &self.stack {
            None => false,
            Some(stack) => {
                for frame in stack.iter() {
                    if frame.index != 0 {
                        return false;
                    }
                }
                true
            }
        }
    }
}
*/
/*
#[test]
fn empty_cursor() -> Result<()> {
    let mut fix = Fixture::new(16, 1024)?;
    fix.commit()?;

    let c = fix.tree.cursor(0)?;
    ensure!(c.get()?.is_none());
    Ok(())
}

#[test]
fn populated_cursor() -> Result<()> {
    let mut fix = Fixture::new(1024, 102400)?;
    fix.commit()?;

    // build a big btree
    let count = 1000;
    for i in 0..count {
        fix.insert(i * 3, &mk_value(i * 3))?;
    }
    eprintln!("built tree");

    let first_key = 601;
    let mut c = fix.tree.cursor(first_key)?;

    let mut expected_key = (first_key / 3) * 3;
    loop {
        let (k, _v) = c.get()?.unwrap();
        ensure!(k == expected_key);
        expected_key += 3;

        if !c.next_entry()? {
            ensure!(expected_key == count * 3);
            break;
        }
    }

    Ok(())
}

#[test]
fn cursor_prev() -> Result<()> {
    let mut fix = Fixture::new(1024, 102400)?;
    fix.commit()?;

    // build a big btree
    let count = 1000;
    for i in 0..count {
        fix.insert(i * 3, &mk_value(i * 3))?;
    }
    eprintln!("built tree");

    let first_key = 601;
    let mut c = fix.tree.cursor(first_key)?;

    let mut expected_key = (first_key / 3) * 3;
    loop {
        let (k, _v) = c.get()?.unwrap();
        ensure!(k == expected_key);

        c.prev_entry()?;
        let (k, _v) = c.get()?.unwrap();
        ensure!(k == expected_key - 3);
        c.next_entry()?;

        expected_key += 3;

        if !c.next_entry()? {
            ensure!(expected_key == count * 3);
            break;
        }
    }

    Ok(())
}
*/
