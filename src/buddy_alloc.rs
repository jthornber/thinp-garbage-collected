use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, BTreeSet};

//-------------------------------------

#[derive(Clone)]
pub struct BuddyAllocator {
    // free_blocks[0] holds blocks of size 'block_size',
    // free_blocks[1]         "            2 * 'block_size' etc.
    //
    // If a block is not present in free_blocks, then it's been allocated
    free_blocks: Vec<BTreeSet<u64>>,
    total_blocks: u64,
}

fn get_buddy(index: u64, order: usize) -> u64 {
    index ^ (1 << order)
}

impl BuddyAllocator {
    // Create a new BuddyAllocator with a given pool size.
    // The pool size does not need to be a power of two.
    pub fn new(pool_size: u64) -> Self {
        let mut order = 0;
        let mut size = 1;
        while size < pool_size {
            size <<= 1;
            order += 1;
        }

        let mut free_blocks = Vec::new();
        for _ in 0..=order {
            free_blocks.push(BTreeSet::new());
        }

        // Initialize free blocks
        let mut remaining_size = pool_size;
        let mut block_index = 0;
        while remaining_size > 0 {
            let mut block_size = 1;
            let mut block_order = 0;
            while block_size * 2 <= remaining_size {
                block_size *= 2;
                block_order += 1;
            }
            free_blocks[block_order].insert(block_index);
            block_index += block_size;
            remaining_size -= block_size;
        }

        BuddyAllocator {
            free_blocks,
            total_blocks: pool_size,
        }
    }

    // Helper function to calculate the order based on the number of blocks
    fn calculate_order(nr_blocks: u64) -> usize {
        let mut order = 0;
        let mut size = 1;
        while size < nr_blocks {
            size <<= 1;
            order += 1;
        }
        order
    }

    // Allocate a block of the given size (in number of blocks).
    pub fn alloc(&mut self, nr_blocks: u64) -> Result<u64> {
        if nr_blocks == 0 {
            return Err(anyhow!("Cannot allocate zero blocks"));
        }

        let order = Self::calculate_order(nr_blocks);

        // We search up through the orders looking for one that
        // contains some free blocks.  We then split this block
        // back down through the orders, until we have one of the
        // desired size.
        let mut high_order = order;
        loop {
            if high_order >= self.free_blocks.len() {
                return Err(anyhow!("Not enough memory to allocate"));
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

        // If the allocated block is larger than needed, free the unused tail
        let allocated_size = 1 << order;
        if allocated_size > nr_blocks {
            let unused_start = index + nr_blocks;
            let unused_size = allocated_size - nr_blocks;
            self.free(unused_start, unused_size)?;
        }
        Ok(index)
    }

    // Free a previously allocated block.
    pub fn free(&mut self, block: u64, nr_blocks: u64) -> Result<()> {
        if nr_blocks == 0 {
            return Err(anyhow!("Cannot free zero blocks"));
        }

        let order = Self::calculate_order(nr_blocks);
        let mut current_order = order;
        let mut current_block = block;
        loop {
            let buddy = get_buddy(current_block, current_order);
            // Is the buddy free at this order?
            if !self.free_blocks[current_order].contains(&buddy) {
                break;
            }
            self.free_blocks[current_order].remove(&buddy);
            current_order += 1;
            if buddy < current_block {
                current_block = buddy;
            }
            if current_order == self.free_blocks.len() {
                break;
            }
        }

        self.free_blocks[current_order].insert(current_block);
        Ok(())
    }

    // Grow the pool by adding extra blocks.
    pub fn grow(&mut self, nr_extra_blocks: u64) -> Result<()> {
        if nr_extra_blocks == 0 {
            return Err(anyhow!("Cannot grow by zero blocks"));
        }

        let new_total_blocks = self.total_blocks + nr_extra_blocks;
        let order = Self::calculate_order(new_total_blocks);

        // Ensure the free_blocks vector is large enough
        while self.free_blocks.len() <= order {
            self.free_blocks.push(BTreeSet::new());
        }

        // Initialize new free blocks
        let mut remaining_size = nr_extra_blocks;
        let mut block_index = self.total_blocks;
        while remaining_size > 0 {
            let mut block_size = 1;
            let mut block_order = 0;
            // Find the largest power of two block that fits in the remaining size
            while block_size * 2 <= remaining_size && (block_index % (block_size * 2) == 0) {
                block_size *= 2;
                block_order += 1;
            }

            // Call free() so the new blocks can be merged with the existing ones.
            self.free(block_index, block_size)?;

            block_index += block_size;
            remaining_size -= block_size;
        }

        self.total_blocks = new_total_blocks;
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

    for i in 0..7 {
        assert!(buddy.free_blocks[i].is_empty());
    }
    assert!(buddy.free_blocks[7].len() == 1);
    Ok(())
}

//-------------------------------------
