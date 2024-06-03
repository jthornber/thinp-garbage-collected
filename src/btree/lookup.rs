use anyhow::Result;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub fn lookup<
    V: Serializable,
    INode: NodeR<MetadataBlock, ReadProxy>,
    LNode: NodeR<V, ReadProxy>,
>(
    tm: &TransactionManager,
    root: MetadataBlock,
    key: u32,
) -> Result<Option<V>> {
    let mut r_proxy = tm.read(root, &BNODE_KIND)?;

    loop {
        let flags = read_flags(r_proxy.r())?;

        match flags {
            BTreeFlags::Internal => {
                let node = INode::open(r_proxy.loc(), r_proxy)?;

                let idx = node.lower_bound(key);
                if idx < 0 || idx >= node.nr_entries() as isize {
                    return Ok(None);
                }

                let child = node.get_value(idx as usize).unwrap();
                r_proxy = tm.read(child, &BNODE_KIND)?;
            }
            BTreeFlags::Leaf => {
                let node = LNode::open(r_proxy.loc(), r_proxy)?;

                let idx = node.lower_bound(key);
                if idx < 0 || idx >= node.nr_entries() as isize {
                    return Ok(None);
                }

                return if node.get_key(idx as usize).unwrap() == key {
                    Ok(node.get_value(idx as usize))
                } else {
                    Ok(None)
                };
            }
        }
    }
}

//-------------------------------------------------------------------------
