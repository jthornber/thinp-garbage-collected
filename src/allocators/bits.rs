//-------------------------------------

// Helper function to calculate the order based on the number of blocks
pub fn calc_order(nr_blocks: u64) -> usize {
    if nr_blocks == 0 {
        return 0;
    }
    64 - (nr_blocks - 1).leading_zeros() as usize
}

pub fn calc_order_below(nr_blocks: u64) -> usize {
    if nr_blocks == 0 {
        return 0;
    }
    63 - nr_blocks.leading_zeros() as usize
}

/// Calculates the order of the lowest set bit in a given 64-bit unsigned integer.
/// If the 2^order will not exceed nr_blocks.
pub fn calc_min_order(n: u64, nr_blocks: u64) -> usize {
    if nr_blocks == 0 {
        return 0;
    }
    n.trailing_zeros().min(nr_blocks.ilog2()) as usize
}

//-------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_calc_order() {
        // Test case for nr_blocks = 0
        assert_eq!(calc_order(0), 0);
        // Test cases for powers of 2
        assert_eq!(calc_order(1), 0); // 2^0 = 1
        assert_eq!(calc_order(2), 1); // 2^1 = 2
        assert_eq!(calc_order(4), 2); // 2^2 = 4
        assert_eq!(calc_order(8), 3); // 2^3 = 8
        assert_eq!(calc_order(16), 4); // 2^4 = 16
        assert_eq!(calc_order(32), 5); // 2^5 = 32
        assert_eq!(calc_order(64), 6); // 2^6 = 64
        assert_eq!(calc_order(128), 7); // 2^7 = 128
        assert_eq!(calc_order(256), 8); // 2^8 = 256
        assert_eq!(calc_order(512), 9); // 2^9 = 512
        assert_eq!(calc_order(1024), 10); // 2^10 = 1024
                                          // Test cases for numbers that are not powers of 2
        assert_eq!(calc_order(3), 2); // 3 < 4 (2^2)
        assert_eq!(calc_order(5), 3); // 5 < 8 (2^3)
        assert_eq!(calc_order(9), 4); // 9 < 16 (2^4)
        assert_eq!(calc_order(17), 5); // 17 < 32 (2^5)
        assert_eq!(calc_order(33), 6); // 33 < 64 (2^6)
        assert_eq!(calc_order(65), 7); // 65 < 128 (2^7)
        assert_eq!(calc_order(129), 8); // 129 < 256 (2^8)
        assert_eq!(calc_order(257), 9); // 257 < 512 (2^9)
        assert_eq!(calc_order(513), 10); // 513 < 1024 (2^10)
                                         // Test cases for large numbers
        assert_eq!(calc_order(1_000_000), 20); // 1_000_000 < 2^20
        assert_eq!(calc_order(1_000_000_000), 30); // 1_000_000_000 < 2^30
        assert_eq!(calc_order(1_000_000_000_000), 40); // 1_000_000_000_000 < 2^40
        assert_eq!(calc_order(1_000_000_000_000_000), 50); // 1_000_000_000_000_000 < 2^50
        assert_eq!(calc_order(1_000_000_000_000_000_000), 60); // 1_000_000_000_000_000_000 < 2^60
    }

    #[test]
    fn test_calc_order_below() {
        // Test cases for powers of 2
        assert_eq!(calc_order_below(1), 0); // 2^0 = 1
        assert_eq!(calc_order_below(2), 1); // 2^1 = 2
        assert_eq!(calc_order_below(4), 2); // 2^2 = 4
        assert_eq!(calc_order_below(8), 3); // 2^3 = 8
        assert_eq!(calc_order_below(16), 4); // 2^4 = 16
        assert_eq!(calc_order_below(32), 5); // 2^5 = 32
        assert_eq!(calc_order_below(64), 6); // 2^6 = 64
        assert_eq!(calc_order_below(128), 7); // 2^7 = 128
        assert_eq!(calc_order_below(256), 8); // 2^8 = 256
        assert_eq!(calc_order_below(512), 9); // 2^9 = 512
        assert_eq!(calc_order_below(1024), 10); // 2^10 = 1024
                                                // Test cases for numbers that are not powers of 2
        assert_eq!(calc_order_below(3), 1); // 2^1 = 2
        assert_eq!(calc_order_below(5), 2); // 2^2 = 4
        assert_eq!(calc_order_below(9), 3); // 2^3 = 8
        assert_eq!(calc_order_below(17), 4); // 2^4 = 16
        assert_eq!(calc_order_below(33), 5); // 2^5 = 32
        assert_eq!(calc_order_below(65), 6); // 2^6 = 64
        assert_eq!(calc_order_below(129), 7); // 2^7 = 128
        assert_eq!(calc_order_below(257), 8); // 2^8 = 256
        assert_eq!(calc_order_below(513), 9); // 2^9 = 512
                                              // Test cases for large numbers
        assert_eq!(calc_order_below(1_000_000), 19); // 2^19 = 524288
        assert_eq!(calc_order_below(1_000_000_000), 29); // 2^29 = 536870912
        assert_eq!(calc_order_below(1_000_000_000_000), 39); // 2^39 = 549755813888
        assert_eq!(calc_order_below(1_000_000_000_000_000), 49); // 2^49 = 562949953421312
        assert_eq!(calc_order_below(1_000_000_000_000_000_000), 59); // 2^59 = 576460752303423488
    }

    #[test]
    fn test_calc_min_order() {
        // Test cases for n with a single set bit
        assert_eq!(calc_min_order(1, 1), 0); // 2^0 = 1
        assert_eq!(calc_min_order(2, 2), 1); // 2^1 = 2
        assert_eq!(calc_min_order(4, 4), 2); // 2^2 = 4
        assert_eq!(calc_min_order(8, 8), 3); // 2^3 = 8
        assert_eq!(calc_min_order(16, 16), 4); // 2^4 = 16
                                               // Test cases for n with multiple set bits
        assert_eq!(calc_min_order(3, 1), 0); // 2^0 = 1
        assert_eq!(calc_min_order(6, 2), 1); // 2^1 = 2
        assert_eq!(calc_min_order(12, 4), 2); // 2^2 = 4
        assert_eq!(calc_min_order(24, 8), 3); // 2^3 = 8
                                              // Test cases for nr_blocks limiting the order
        assert_eq!(calc_min_order(1, 0), 0); // 2^0 = 1, but nr_blocks = 0
        assert_eq!(calc_min_order(2, 1), 0); // 2^1 = 2, but nr_blocks = 1
        assert_eq!(calc_min_order(4, 3), 1); // 2^2 = 4, but nr_blocks = 3
        assert_eq!(calc_min_order(8, 7), 2); // 2^3 = 8, but nr_blocks = 7
                                             // Test cases for large numbers
        assert_eq!(calc_min_order(1 << 20, 1_000_000), 19); // 2^20 = 1048576
        assert_eq!(calc_min_order(1 << 30, 1_000_000_000), 29); // 2^30 = 1073741824
        assert_eq!(calc_min_order(1 << 40, 1_000_000_000_000), 39); // 2^40 = 1099511627776
    }
}

//-------------------------------------
