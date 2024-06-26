use std::sync::{Arc, Mutex};

use crate::allocators::buddy_alloc::*;
use crate::allocators::*;

//-------------------------------------

pub struct DataAlloc {
    global_alloc: Arc<Mutex<dyn Allocator>>,
    local_alloc: BuddyAllocator,
    prealloc_size: u64,
}

impl Drop for DataAlloc {
    fn drop(&mut self) {
        let mut global_alloc = self.global_alloc.lock().unwrap();
        for (order, blocks) in self.local_alloc.free_blocks.iter().enumerate() {
            for &block in blocks {
                let size = 1 << order;
                global_alloc
                    .free(block, size)
                    .expect("freeing data block failed");
            }
        }
    }
}

impl DataAlloc {
    pub fn new(global_alloc: Arc<Mutex<dyn Allocator>>, prealloc_size: u64) -> Self {
        let mut global_alloc_locked = global_alloc.lock().unwrap();
        let nr_blocks = global_alloc_locked.nr_blocks();
        drop(global_alloc_locked);

        let local_alloc = BuddyAllocator::new_empty(nr_blocks);
        Self {
            global_alloc,
            local_alloc,
            prealloc_size,
        }
    }

    /// Preallocate more space from the global allocator
    fn prealloc(&mut self) -> Result<()> {
        let (_total, runs) = {
            let mut global_alloc = self.global_alloc.lock().unwrap();
            global_alloc
                .alloc_many(self.prealloc_size, 0)
                .expect("Failed to preallocate additional space for DataAlloc")
        };

        // Add the new runs to the local allocator
        for (b, e) in runs {
            self.local_alloc
                .free(b, e - b)
                .expect("Failed to free new run into local allocator");
        }

        Ok(())
    }

    pub fn alloc(&mut self, nr_blocks: u64) -> Result<(u64, Vec<AllocRun>)> {
        match self.local_alloc.alloc_many(nr_blocks, 0) {
            Ok(result) => Ok(result),
            Err(MemErr::OutOfSpace) => {
                self.prealloc()?;

                // Retry the allocation
                self.local_alloc.alloc_many(nr_blocks, 0)
            }
            Err(e) => Err(e),
        }
    }

    pub fn free(&mut self, block: u64, nr_blocks: u64) -> Result<()> {
        self.local_alloc.free(block, nr_blocks)
    }
}

// -------------------------------------

#[test]
fn test_data_alloc() -> Result<()> {
    let global_alloc = Arc::new(Mutex::new(BuddyAllocator::new(1024 * 256))); // 1GB worth of 4k pages
    let mut data_alloc = DataAlloc::new(global_alloc.clone(), 1024); // Preallocate 4M

    // allocate from prealloc
    let (total, runs) = data_alloc.alloc(512)?;
    assert_eq!(total, 512);

    // still from prealloc
    let (total, runs) = data_alloc.alloc(312)?;
    assert_eq!(total, 312);

    // Allocate more than the initial preallocation to trigger additional preallocation
    let (total, runs) = data_alloc.alloc(512)?;
    assert_eq!(total, 512);

    Ok(())
}

// -------------------------------------
