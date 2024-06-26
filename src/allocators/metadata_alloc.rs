use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::allocators::buddy_alloc::*;
use crate::allocators::*;
use crate::block_cache::MetadataBlock;

//-------------------------------------

/// A sub allocator that wraps the global metadata allocator.
/// Each active thin volume will have one of these to improve
/// metadata locality.
pub struct MetadataAlloc {
    global_alloc: Arc<Mutex<dyn Allocator>>,
    prealloc_count: u64,
    free_list: VecDeque<MetadataBlock>,
}

impl Drop for MetadataAlloc {
    fn drop(&mut self) {
        let mut global_alloc = self.global_alloc.lock().unwrap();

        for b in &self.free_list {
            global_alloc
                .free(*b as u64, 1)
                .expect("freeing metadata block failed");
        }
    }
}

impl MetadataAlloc {
    pub fn new(global_alloc: Arc<Mutex<dyn Allocator>>, prealloc_size: u64) -> Self {
        Self {
            global_alloc,
            prealloc_count: prealloc_size,
            free_list: VecDeque::new(),
        }
    }

    pub fn alloc(&mut self) -> Result<MetadataBlock> {
        if self.free_list.is_empty() {
            self.prealloc()?;
        }

        let b = self.free_list.pop_front().unwrap();
        Ok(b)
    }

    // Succeeds if _any_ blocks were pre-allocated.
    fn prealloc(&mut self) -> Result<()> {
        let mut global_alloc = self.global_alloc.lock().unwrap();

        let (total, runs) = global_alloc.alloc_many(self.prealloc_count, 0)?;

        for (b, e) in runs {
            for block in b..e {
                self.free_list.push_back(block as MetadataBlock);
            }
        }

        Ok(())
    }
}

//-------------------------------------

#[test]
fn test_prealloc() -> Result<()> {
    let global_alloc = Arc::new(Mutex::new(BuddyAllocator::new(128)));
    let mut metadata_alloc = MetadataAlloc::new(global_alloc.clone(), 10);

    // Pre-allocate blocks
    metadata_alloc.prealloc()?;

    // Check that the free_list is populated
    assert_eq!(metadata_alloc.free_list.len(), 10);
    Ok(())
}

//-------------------------------------
