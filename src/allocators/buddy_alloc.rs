use std::collections::{BTreeMap, BTreeSet};

use crate::allocators::bits::*;
use crate::allocators::*;

//-------------------------------------

#[derive(Clone)]
pub struct BuddyAllocator {
    // free_blocks[0] holds blocks of size 'block_size',
    // free_blocks[1]         "            2 * 'block_size' etc.
    //
    // If a block is not present in free_blocks, then it's been allocated
    pub free_blocks: Vec<BTreeSet<u64>>,
    pub total_blocks: u64,
}

fn get_buddy(index: u64, order: usize) -> u64 {
    index ^ (1 << order)
}

impl BuddyAllocator {
    pub fn new_empty(nr_blocks: u64) -> Self {
        let order = calc_order(nr_blocks);

        let mut free_blocks = Vec::new();
        for _ in 0..=order {
            free_blocks.push(BTreeSet::new());
        }

        BuddyAllocator {
            free_blocks,
            total_blocks: nr_blocks,
        }
    }

    // Create a new BuddyAllocator with a given pool size.
    // The pool size does not need to be a power of two.
    pub fn new(nr_blocks: u64) -> Self {
        let mut alloc = BuddyAllocator::new_empty(nr_blocks);
        alloc
            .free(0, nr_blocks)
            .expect("Failed to initialize allocator");
        alloc
    }

    pub fn from_runs(nr_blocks: u64, runs: Vec<AllocRun>) -> Self {
        let mut alloc = BuddyAllocator::new_empty(nr_blocks);

        // Free each run into the allocator
        for (start, end) in runs {
            let size = end - start;
            alloc
                .free(start, size)
                .expect("Failed to free run during initialization");
        }
        alloc
    }

    // FIXME: the next three functions can be implemented with bit banging.  See Hacker's Delight.

    /// Succeeds if _any_ blocks were pre-allocated.
    pub fn alloc_many(&mut self, nr_blocks: u64, min_order: usize) -> Result<(u64, Vec<AllocRun>)> {
        let mut total_allocated = 0;
        let mut runs = Vec::new();
        let mut order = calc_order(nr_blocks);

        let mut remaining = nr_blocks;
        while remaining > 0 {
            if let Ok(block) = self.alloc_order(order) {
                let size = (1 << order).min(remaining);
                runs.push((block, block + size));
                total_allocated += size;
                remaining -= size;
            } else {
                if order <= min_order {
                    break;
                }

                // Look for a smaller block
                order -= 1;
            }
        }

        // Sort the runs
        runs.sort_by_key(|&(start, _)| start);

        if total_allocated > 0 {
            Ok((total_allocated, runs))
        } else {
            Err(MemErr::OutOfSpace)
        }
    }

    // Allocate a block of the given size (in number of blocks).
    pub fn alloc(&mut self, nr_blocks: u64) -> Result<u64> {
        if nr_blocks == 0 {
            return Err(MemErr::BadParams("cannot allocate zero blocks".to_string()));
        }

        let order = calc_order(nr_blocks);
        let index = self.alloc_order(order)?;

        // If the allocated block is larger than needed, free the unused tail
        let allocated_size = 1 << order;
        if allocated_size > nr_blocks {
            let unused_start = index + nr_blocks;
            let unused_size = allocated_size - nr_blocks;
            self.free(unused_start, unused_size)?;
        }

        Ok(index)
    }

    pub fn alloc_order(&mut self, order: usize) -> Result<u64> {
        // We search up through the orders looking for one that
        // contains some free blocks.  We then split this block
        // back down through the orders, until we have one of the
        // desired size.
        let mut high_order = order;
        loop {
            if high_order >= self.free_blocks.len() {
                return Err(MemErr::OutOfSpace);
            }
            if !self.free_blocks[high_order].is_empty() {
                break;
            }
            high_order += 1;
        }

        let index = self.free_blocks[high_order].pop_first().unwrap();

        // Split back down
        while high_order != order {
            high_order -= 1;
            self.free_blocks[high_order].insert(get_buddy(index, high_order));
        }

        Ok(index)
    }

    // Free a previously allocated block.
    pub fn free(&mut self, block: u64, nr_blocks: u64) -> Result<()> {
        if nr_blocks == 0 {
            return Err(MemErr::BadParams("cannot free zero blocks".to_string()));
        }

        let mut b = block;
        let e = b + nr_blocks;

        while b != e {
            let order = calc_min_order(b, e - b);
            eprintln!("b = {}, order = {}", b, order);
            self.free_order(b, order);
            b += 1 << order;
        }

        Ok(())
    }

    pub fn free_order(&mut self, mut block: u64, mut order: usize) -> Result<()> {
        loop {
            let buddy = get_buddy(block, order);

            // Is the buddy free at this order?
            if !self.free_blocks[order].contains(&buddy) {
                break;
            }
            self.free_blocks[order].remove(&buddy);
            order += 1;

            if buddy < block {
                block = buddy;
            }

            if order == self.free_blocks.len() {
                break;
            }
        }

        self.free_blocks[order].insert(block);
        Ok(())
    }

    // Grow the pool by adding extra blocks.
    pub fn grow(&mut self, nr_extra_blocks: u64) -> Result<()> {
        if nr_extra_blocks == 0 {
            return Err(MemErr::BadParams("Cannot grow by zero blocks".to_string()));
        }

        let new_total_blocks = self.total_blocks + nr_extra_blocks;
        let order = calc_order(new_total_blocks);

        // Ensure the free_blocks vector is large enough
        while self.free_blocks.len() <= order {
            self.free_blocks.push(BTreeSet::new());
        }
        let old_total = self.total_blocks;
        self.total_blocks = new_total_blocks;

        self.free(old_total, nr_extra_blocks);
        Ok(())
    }
}

#[test]
fn test_create_allocator() -> Result<()> {
    let _buddy = BuddyAllocator::new(1024);
    Ok(())
}

#[test]
fn test_alloc_small() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);
    // Allocate 1 block
    let index = buddy.alloc(1)?;
    assert_eq!(index, 0);
    buddy.free(index, 1)?;
    let index = buddy.alloc(1)?;
    assert_eq!(index, 0);
    Ok(())
}

#[test]
fn test_alloc_and_free() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);
    let index1 = buddy.alloc(2)?;
    let index2 = buddy.alloc(2)?;
    assert_ne!(index1, index2);
    buddy.free(index1, 2)?;
    buddy.free(index2, 2)?;
    let index3 = buddy.alloc(2)?;
    assert!(index3 == index1 || index3 == index2);
    Ok(())
}

#[test]
fn test_alloc_large() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);
    let index = buddy.alloc(512)?;
    assert_eq!(index, 0);
    buddy.free(index, 512)?;
    Ok(())
}

#[test]
fn test_grow_allocator() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);
    // Grow the allocator by 512 blocks
    buddy.grow(512)?;
    // Allocate a block of 512 blocks
    let index = buddy.alloc(512)?;
    assert_eq!(index, 1024);
    buddy.free(index, 512)?;
    Ok(())
}

#[test]
fn test_alloc_non_power_of_two() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);
    // Allocate 3 blocks (non-power of two)
    let index = buddy.alloc(3)?;
    assert_eq!(index, 0);
    // Free the allocated 3 blocks
    buddy.free(index, 3)?;
    Ok(())
}

#[test]
fn test_grow_allocator_correctly() -> Result<()> {
    let mut buddy = BuddyAllocator::new(53);
    buddy.grow(128 - 53)?;

    // FIXME: remove
    for i in 0..buddy.free_blocks.len() {
        eprintln!("slot {} has {} entries", i, buddy.free_blocks[i].len());
    }

    for i in 0..7 {
        assert!(buddy.free_blocks[i].is_empty());
    }
    assert!(buddy.free_blocks[7].len() == 1);
    Ok(())
}

//-------------------------------------
