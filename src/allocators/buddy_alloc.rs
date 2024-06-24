use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};

use crate::allocators::bits::*;
use crate::allocators::*;
use crate::varint::*;

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

// Testing shows this value has v. similar performance between 0.05 and 0.5.
const DENSITY_THRESHOLD: f64 = 0.1;

impl BuddyAllocator {
    pub fn pack(&self) -> io::Result<Vec<u8>> {
        let mut packed = Vec::new();
        packed.write_u64::<LittleEndian>(self.total_blocks)?;

        for (order, blocks) in self.free_blocks.iter().enumerate() {
            if !blocks.is_empty() {
                packed.write_u8(order as u8)?;
                let blocks_at_this_order = self.total_blocks >> order;
                let density = blocks.len() as f64 / blocks_at_this_order as f64;

                if density > DENSITY_THRESHOLD {
                    // Use bitmap
                    packed.write_u8(1)?; // 1 indicates bitmap
                    write_varint(&mut packed, blocks_at_this_order)?;
                    let mut bitmap = vec![0u8; (blocks_at_this_order as usize + 7) / 8];
                    for &block in blocks {
                        let index = (block >> order) as usize;
                        bitmap[index / 8] |= 1 << (index % 8);
                    }
                    packed.extend_from_slice(&bitmap);
                } else {
                    // Use delta encoding
                    packed.write_u8(0)?; // 0 indicates delta encoding
                    write_varint(&mut packed, blocks.len() as u64)?;
                    let mut prev_block = 0;
                    for &block in blocks {
                        let delta = block - prev_block;
                        write_varint(&mut packed, delta)?;
                        prev_block = block;
                    }
                }
            }
        }
        Ok(packed)
    }

    pub fn unpack(mut data: &[u8]) -> io::Result<Self> {
        let total_blocks = data.read_u64::<LittleEndian>()?;
        let mut alloc = BuddyAllocator::new_empty(total_blocks);

        while !data.is_empty() {
            let order = data.read_u8()? as usize;
            let encoding_type = data.read_u8()?;

            if encoding_type == 1 {
                // bitmap
                let blocks_at_this_order = read_varint(&mut data)?;
                let bitmap_size = ((blocks_at_this_order + 7) / 8) as usize;
                let bitmap = &data[..bitmap_size];
                data = &data[bitmap_size..];

                for i in 0..blocks_at_this_order {
                    if bitmap[i as usize / 8] & (1 << (i % 8)) != 0 {
                        alloc.free_blocks[order].insert(i << order);
                    }
                }
            } else {
                // delta encoding
                let count = read_varint(&mut data)?;
                let mut block = 0;
                for _ in 0..count {
                    let delta = read_varint(&mut data)?;
                    block += delta;
                    alloc.free_blocks[order].insert(block);
                }
            }
        }
        Ok(alloc)
    }

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

    fn alloc_order(&mut self, order: usize) -> Result<u64> {
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

    pub fn get_containing_block(&self, block: u64, order: usize) -> u64 {
        // Mask off the lower bits to find the containing block
        block & !((1 << order) - 1)
    }

    pub fn alloc_at(&mut self, block: u64, order: usize) -> Result<()> {
        let max_order = self.free_blocks.len();

        if order >= self.free_blocks.len() {
            return Err(MemErr::BadParams("Order too large".to_string()));
        }

        let size = 1 << order;
        if block + size > self.total_blocks {
            return Err(MemErr::BadParams("Block out of range".to_string()));
        }

        let mut current_order = order;
        let mut current_block = block;

        // Go up looking for the superblock that contains this block
        while current_order < max_order {
            current_block = self.get_containing_block(block, current_order);

            if self.free_blocks[current_order].remove(&current_block) {
                // We found it.
                break;
            }

            current_order += 1;
        }

        if current_order == max_order {
            return Err(MemErr::OutOfSpace);
        }

        // Now we go down the levels splitting
        while current_order > order {
            current_order -= 1;
            let buddy = get_buddy(current_block, current_order);

            if self.get_containing_block(block, current_order) == current_block {
                self.free_blocks[current_order].insert(buddy);
            } else {
                self.free_blocks[current_order].insert(current_block);
                current_block = buddy;
            }
        }

        assert!(current_block == block);
        Ok(())
    }

    fn free_order(&mut self, mut block: u64, mut order: usize) -> Result<()> {
        loop {
            let buddy = get_buddy(block, order);

            // Is the buddy free at this order?
            if !self.free_blocks[order].contains(&buddy) {
                break;
            }
            self.free_blocks[order].remove(&buddy);
            order += 1;

            block = block.min(buddy);

            if order == self.free_blocks.len() {
                break;
            }
        }

        self.free_blocks[order].insert(block);
        Ok(())
    }
}

impl Allocator for BuddyAllocator {
    /// Succeeds if _any_ blocks were pre-allocated.
    fn alloc_many(&mut self, nr_blocks: u64, min_order: usize) -> Result<(u64, Vec<AllocRun>)> {
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
    fn alloc(&mut self, nr_blocks: u64) -> Result<u64> {
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

    // Free a previously allocated block.
    fn free(&mut self, block: u64, nr_blocks: u64) -> Result<()> {
        if nr_blocks == 0 {
            return Err(MemErr::BadParams("cannot free zero blocks".to_string()));
        }

        let mut b = block;
        let e = b + nr_blocks;

        while b < e {
            let order = calc_min_order(b, e - b);
            self.free_order(b, order);
            b += 1 << order;
        }

        Ok(())
    }

    // Grow the pool by adding extra blocks.
    fn grow(&mut self, nr_extra_blocks: u64) -> Result<()> {
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

//-------------------------------------

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

#[test]
fn test_alloc_at() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);

    // Test 1: Allocate a block at order 3 (8 blocks) starting at block 8
    buddy.alloc_at(8, 3)?;

    // Try to allocate the same block again, should fail
    assert!(
        buddy.alloc_at(8, 3).is_err(),
        "Should not be able to allocate the same block twice"
    );

    // Allocate adjacent blocks
    buddy.alloc_at(0, 3)?;
    buddy.alloc_at(16, 3)?;

    // Try to allocate a block that overlaps with existing allocations, should fail
    assert!(
        buddy.alloc_at(4, 3).is_err(),
        "Should not be able to allocate overlapping block"
    );

    // Allocate a larger block
    buddy.alloc_at(32, 5)?;

    // Free the allocated blocks
    buddy.free(8, 8)?;
    buddy.free(0, 8)?;
    buddy.free(16, 8)?;
    buddy.free(32, 32)?;

    // After freeing, we should be able to allocate a large block again
    buddy.alloc_at(0, 9)?;

    // Free the large block
    buddy.free(0, 512)?;

    // Allocate at the end of the memory
    buddy.alloc_at(1016, 3)?;

    // Try to allocate a block that goes beyond the total size, should fail
    assert!(
        buddy.alloc_at(1020, 3).is_err(),
        "Should not be able to allocate beyond total size"
    );

    // Free the last allocation
    buddy.free(1016, 8)?;

    // Final test: allocate the entire memory
    println!("State before attempted full allocation:");
    for (i, set) in buddy.free_blocks.iter().enumerate() {
        println!("Order {}: {:?}", i, set);
    }

    println!("Attempting to allocate entire memory:");
    match buddy.alloc_at(0, 10) {
        Ok(_) => println!("Successfully allocated entire memory"),
        Err(e) => println!("Failed to allocate entire memory: {:?}", e),
    }

    // Print the state of free_blocks after attempted full allocation
    println!("State after attempted full allocation:");
    for (i, set) in buddy.free_blocks.iter().enumerate() {
        println!("Order {}: {:?}", i, set);
    }

    // Try to allocate smaller chunks to see what's available
    for order in (0..10).rev() {
        match buddy.alloc_at(0, order) {
            Ok(_) => println!("Successfully allocated at order {}", order),
            Err(e) => println!("Failed to allocate at order {}: {:?}", order, e),
        }
    }

    Ok(())
}

#[test]
fn test_alloc_at_debug() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);

    println!("Initial state:");
    for (i, set) in buddy.free_blocks.iter().enumerate() {
        println!("Order {}: {:?}", i, set);
    }

    // Test 1: Allocate a block at order 3 (8 blocks) starting at block 8
    buddy.alloc_at(8, 3)?;

    println!("State after allocation:");
    for (i, set) in buddy.free_blocks.iter().enumerate() {
        println!("Order {}: {:?}", i, set);
    }

    // Check that block 8 is not free at any order
    for order in 0..buddy.free_blocks.len() {
        assert!(
            !buddy.free_blocks[order].contains(&8),
            "Block 8 should not be free at order {}",
            order
        );
    }

    // Check that block 0 is free at order 3 (covers blocks 0-7)
    assert!(
        buddy.free_blocks[3].contains(&0),
        "Block 0 should be free at order 3"
    );

    // Check that blocks are free at the correct orders
    assert!(
        buddy.free_blocks[4].contains(&16),
        "Block 16 should be free at order 4"
    );
    assert!(
        buddy.free_blocks[5].contains(&32),
        "Block 32 should be free at order 5"
    );
    assert!(
        buddy.free_blocks[6].contains(&64),
        "Block 64 should be free at order 6"
    );
    assert!(
        buddy.free_blocks[7].contains(&128),
        "Block 128 should be free at order 7"
    );
    assert!(
        buddy.free_blocks[8].contains(&256),
        "Block 256 should be free at order 8"
    );
    assert!(
        buddy.free_blocks[9].contains(&512),
        "Block 512 should be free at order 9"
    );

    // Ensure each order has exactly one free block (except 0, 1, 2, and 10)
    for order in 3..10 {
        assert_eq!(
            buddy.free_blocks[order].len(),
            1,
            "There should be exactly one free block at order {}",
            order
        );
    }

    // Orders 0, 1, 2, and 10 should be empty
    assert!(buddy.free_blocks[0].is_empty(), "Order 0 should be empty");
    assert!(buddy.free_blocks[1].is_empty(), "Order 1 should be empty");
    assert!(buddy.free_blocks[2].is_empty(), "Order 2 should be empty");
    assert!(buddy.free_blocks[10].is_empty(), "Order 10 should be empty");

    Ok(())
}

#[test]
fn test_get_containing_block() {
    let buddy = BuddyAllocator::new(1024);

    assert_eq!(buddy.get_containing_block(2, 3), 0);
    assert_eq!(buddy.get_containing_block(7, 3), 0);
    assert_eq!(buddy.get_containing_block(8, 3), 8);
    assert_eq!(buddy.get_containing_block(15, 3), 8);

    assert_eq!(buddy.get_containing_block(16, 4), 16);
    assert_eq!(buddy.get_containing_block(31, 4), 16);

    assert_eq!(buddy.get_containing_block(32, 5), 32);
    assert_eq!(buddy.get_containing_block(63, 5), 32);

    assert_eq!(buddy.get_containing_block(1023, 10), 0);
}

#[test]
fn test_free_small_blocks() -> Result<()> {
    let mut buddy = BuddyAllocator::new(1024);

    // Allocate all the space
    buddy.alloc(1024)?;

    println!("State before freeing:");
    for (i, set) in buddy.free_blocks.iter().enumerate() {
        println!("Order {}: {:?}", i, set);
    }

    // Free the first 8 blocks
    buddy.free(0, 8)?;

    println!("State after freeing 8 blocks:");
    for (i, set) in buddy.free_blocks.iter().enumerate() {
        println!("Order {}: {:?}", i, set);
    }

    // Check that the blocks are free at the correct order
    assert!(
        buddy.free_blocks[3].contains(&0),
        "Block 0 should be free at order 3"
    );

    // Check that the block is not free at lower orders
    assert!(
        !buddy.free_blocks[2].contains(&0),
        "Block 0 should not be free at order 2"
    );
    assert!(
        !buddy.free_blocks[1].contains(&0),
        "Block 0 should not be free at order 1"
    );
    assert!(
        !buddy.free_blocks[0].contains(&0),
        "Block 0 should not be free at order 0"
    );

    // Check that individual smaller blocks are not marked as free
    assert!(
        !buddy.free_blocks[0].contains(&1),
        "Block 1 should not be free at order 0"
    );
    assert!(
        !buddy.free_blocks[0].contains(&2),
        "Block 2 should not be free at order 0"
    );
    assert!(
        !buddy.free_blocks[0].contains(&3),
        "Block 3 should not be free at order 0"
    );
    assert!(
        !buddy.free_blocks[1].contains(&4),
        "Block 4 should not be free at order 1"
    );
    assert!(
        !buddy.free_blocks[2].contains(&4),
        "Block 4 should not be free at order 2"
    );

    Ok(())
}

#[test]
fn test_buddy_allocator_pack_unpack() -> anyhow::Result<()> {
    // Create a BuddyAllocator
    let mut allocator = BuddyAllocator::new(1024);

    // Perform some allocations and frees
    let block1 = allocator.alloc(10)?;
    let block2 = allocator.alloc(20)?;
    allocator.free(block1, 10)?;
    let block3 = allocator.alloc(5)?;

    // Pack the allocator
    let packed = allocator.pack()?;
    eprintln!("packed size = {}", packed.len());

    // Unpack to create a new allocator
    let mut unpacked_allocator = BuddyAllocator::unpack(&packed)?;

    // Verify that the unpacked allocator has the same state
    assert_eq!(allocator.total_blocks, unpacked_allocator.total_blocks);
    assert_eq!(
        allocator.free_blocks.len(),
        unpacked_allocator.free_blocks.len()
    );

    for (original, unpacked) in allocator
        .free_blocks
        .iter()
        .zip(unpacked_allocator.free_blocks.iter())
    {
        eprintln!("orig: {:?}, unpack: {:?}", original, unpacked);
        assert_eq!(original, unpacked);
    }

    // Perform the same allocations on the unpacked allocator to ensure it behaves identically
    let block2 = allocator.alloc(20)?;
    let block3 = allocator.alloc(5)?;
    let unpacked_block2 = unpacked_allocator.alloc(20)?;
    let unpacked_block3 = unpacked_allocator.alloc(5)?;

    assert_eq!(block2, unpacked_block2);
    assert_eq!(block3, unpacked_block3);

    Ok(())
}

#[test]
fn test_buddy_allocator_pack_pathological() -> anyhow::Result<()> {
    // Create a BuddyAllocator with 1024 * 1024 blocks
    let mut allocator = BuddyAllocator::new(1024 * 1024);

    // Allocate every other block
    for i in (0..1024 * 1024).step_by(2) {
        allocator.alloc_at(i, 0)?;
    }

    // Pack the allocator
    let packed = allocator.pack()?;

    // Print the size of the packed data
    println!("Packed size for pathological case: {} bytes", packed.len());

    // Unpack to create a new allocator
    let mut unpacked_allocator = BuddyAllocator::unpack(&packed)?;

    // Verify that the unpacked allocator has the same state
    assert_eq!(allocator.total_blocks, unpacked_allocator.total_blocks);
    assert_eq!(allocator.free_blocks, unpacked_allocator.free_blocks);

    // Try to allocate a free block in the unpacked allocator
    let block = unpacked_allocator.alloc(1)?;
    assert_eq!(block, 1, "First free block should be at index 1");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn create_allocator_with_density(total_blocks: u64, density: f64) -> BuddyAllocator {
        let mut allocator = BuddyAllocator::new(total_blocks);
        let free_blocks = (total_blocks as f64 * density) as u64;
        let mut rng = rand::thread_rng();

        // Allocate blocks to achieve the desired density of free blocks
        for _ in 0..(total_blocks - free_blocks) {
            loop {
                let block = rng.gen_range(0..total_blocks);
                if allocator.alloc_at(block, 0).is_ok() {
                    break;
                }
            }
        }

        allocator
    }

    #[test]
    fn test_packing_efficiency_vs_density() -> io::Result<()> {
        let total_blocks = 1_000_000; // 1 million blocks
        let densities = [
            0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99,
        ];

        println!("Density | Packed Size (bytes) | Bytes per Free Block");
        println!("--------|---------------------|---------------------");

        for &density in &densities {
            let allocator = create_allocator_with_density(total_blocks, density);
            let packed = allocator.pack()?;
            let free_blocks = (total_blocks as f64 * density) as u64;
            let bytes_per_free_block = packed.len() as f64 / free_blocks as f64;

            println!(
                "{:7.2} | {:19} | {:21.2}",
                density,
                packed.len(),
                bytes_per_free_block
            );
        }

        Ok(())
    }
}

//-------------------------------------
