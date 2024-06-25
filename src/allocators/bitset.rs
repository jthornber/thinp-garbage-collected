use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::mem::size_of;

use crate::hash::*;
use crate::varint::*;

//----------------------------------------------------------------

fn div_up(n: u64, divisor: u64) -> u64 {
    (n + divisor - 1) / divisor
}

//----------------------------------------------------------------

pub struct Bitset {
    pub nr_bits: u64,
    bits: Vec<u64>,
}

#[derive(PartialEq, Eq)]
enum WordType {
    Ones,
    Zeroes,
    Mix,
}

impl Bitset {
    pub fn ones(nr_bits: u64) -> Self {
        let nr_u64s = div_up(nr_bits, 64);
        let bits = vec![u64::MAX; nr_u64s as usize];
        Bitset { nr_bits, bits }
    }

    pub fn zeroes(nr_bits: u64) -> Self {
        let nr_u64s = div_up(nr_bits, 64);
        let bits = vec![0; nr_u64s as usize];
        Bitset { nr_bits, bits }
    }

    /// Zero the last n bits of a word at idx
    fn zero_high_n_bits(&mut self, idx: usize, n: u8) {
        assert!(n <= 64, "n must be <= 64");
        if n == 0 {
            return;
        }

        let mask = ((1u64 << (64 - n)) - 1);
        self.bits[idx] &= mask;
    }

    /// Zero a run of words [begin, end)
    fn zero_word_range(&mut self, begin: usize, end: usize) {
        if begin < end {
            // Using `fill` is equivalent to `memset` in safe Rust
            self.bits[begin..end].fill(0);
        }
    }

    /// Zero the first n bits of a word at idx
    fn zero_low_n_bits(&mut self, idx: usize, n: u8) {
        assert!(n <= 64, "n must be <= 64");
        if n == 0 {
            return;
        }
        let mask = !0u64 << n;
        self.bits[idx] &= mask;
    }

    pub fn clear_range(&mut self, b: u64, e: u64) {
        assert!(b < e && e <= self.nr_bits, "Invalid range");

        let start_word = (b / 64) as usize;
        let end_word = (e / 64) as usize;
        let start_bit = (b % 64) as u8;
        let end_bit = (e % 64) as u8;

        if start_word == end_word {
            // Case 1: Range is within a single word
            let n = end_bit - start_bit;
            let mask = ((1u64 << n) - 1) << start_bit;
            self.bits[start_word] &= !mask;
        } else {
            // Case 2: Range spans multiple words
            // Handle first word
            self.zero_high_n_bits(start_word, 64 - start_bit);

            // Handle middle words
            self.zero_word_range(start_word + 1, end_word);

            // Handle last word
            if end_bit > 0 {
                self.zero_low_n_bits(end_word, end_bit);
            }
        }
    }

    /*
    pub fn clear_range(&mut self, b: u64, e: u64) {
        assert!(b < e && e <= self.nr_bits, "Invalid range");

        for bit in b..e {
            let word_index = (bit / 64) as usize;
            let bit_index = bit % 64;
            self.bits[word_index] &= !(1u64 << bit_index);
        }
    }
    */

    fn word_type(word: u64) -> WordType {
        use WordType::*;
        if word == u64::MAX {
            Ones
        } else if word == 0 {
            Zeroes
        } else {
            Mix
        }
    }

    pub fn pack(&self) -> io::Result<Vec<u8>> {
        use WordType::*;

        let mut packed = Vec::with_capacity(1024);
        write_varint(&mut packed, self.nr_bits);

        if self.nr_bits == 0 {
            return Ok(packed);
        }

        let mut current_type = Self::word_type(self.bits[0]);
        let mut run_length = 1u64;

        for (idx, &word) in self.bits.iter().enumerate().skip(1) {
            let word_type = Self::word_type(word);
            if word_type == current_type {
                run_length += 1;
            } else {
                self.write_run(
                    &mut packed,
                    idx - run_length as usize,
                    current_type,
                    run_length,
                )?;
                current_type = word_type;
                run_length = 1;
            }
        }

        if !self.bits.is_empty() && run_length > 0 {
            self.write_run(
                &mut packed,
                self.bits.len() - run_length as usize,
                current_type,
                run_length,
            )?;
        }

        Ok(packed)
    }

    fn write_run(
        &self,
        packed: &mut Vec<u8>,
        idx: usize,
        word_type: WordType,
        length: u64,
    ) -> io::Result<()> {
        use WordType::*;
        assert!(length > 0);

        match word_type {
            Ones => {
                packed.push(0);
                write_varint(packed, length);
            }
            Zeroes => {
                packed.push(1);
                write_varint(packed, length);
            }
            Mix => {
                packed.push(2);
                write_varint(packed, length);

                // Append the word data
                for i in idx..(idx + length as usize) {
                    packed.write_u64::<LittleEndian>(self.bits[i])?;
                }
            }
        }

        Ok(())
    }

    pub fn unpack(data: &[u8]) -> io::Result<Self> {
        let mut cursor = io::Cursor::new(data);
        let nr_bits = read_varint(&mut cursor)?;
        let nr_words = div_up(nr_bits, 64);
        let mut words = Vec::with_capacity(nr_words as usize);

        while (cursor.position() as usize) < data.len() {
            let word_type = cursor.read_u8()?;
            let run_length = read_varint(&mut cursor)?;

            match word_type {
                0 => {
                    // Ones
                    for _ in 0..run_length {
                        words.push(u64::MAX);
                    }
                }
                1 => {
                    // Zeroes
                    for _ in 0..run_length {
                        words.push(0);
                    }
                }
                2 => {
                    // Mix
                    for _ in 0..run_length {
                        let word = cursor.read_u64::<LittleEndian>()?;
                        words.push(word);
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid word type",
                    ))
                }
            }
        }

        Ok(Self {
            nr_bits,
            bits: words,
        })
    }

    pub fn is_set(&self, bit: u64) -> bool {
        if bit >= self.nr_bits {
            return false;
        }
        let word_index = (bit / 64) as usize;
        let bit_index = bit % 64;
        (self.bits[word_index] & (1u64 << bit_index)) != 0
    }

    pub fn zero_runs(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        ZeroRunIterator::new(self)
    }
}

//----------------------------------------------------------------

struct ZeroRunIterator<'a> {
    bitset: &'a Bitset,
    current_position: u64,
}

impl<'a> ZeroRunIterator<'a> {
    fn new(bitset: &'a Bitset) -> Self {
        ZeroRunIterator {
            bitset,
            current_position: 0,
        }
    }

    // FIXME: we can speed up by comparing with 0 and u64::MAX
    fn find_next_zero(&mut self) -> Option<u64> {
        while self.current_position < self.bitset.nr_bits {
            if !self.bitset.is_set(self.current_position) {
                return Some(self.current_position);
            }
            self.current_position += 1;
        }
        None
    }

    fn measure_zero_run(&mut self, begin: u64) -> u64 {
        let mut end = begin;
        while end < self.bitset.nr_bits && !self.bitset.is_set(end) {
            end += 1;
        }
        self.current_position = end;
        end
    }
}

impl<'a> Iterator for ZeroRunIterator<'a> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(begin) = self.find_next_zero() {
            let end = self.measure_zero_run(begin);
            Some((begin, end))
        } else {
            None
        }
    }
}

//----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_bitset_ones() {
        let bs = Bitset::ones(100);
        assert_eq!(bs.nr_bits, 100);
        assert_eq!(bs.bits.len(), 2);
        assert_eq!(bs.bits[0], u64::MAX);

        // Check that the first 36 bits of the second word are set
        assert_eq!(bs.bits[1] & ((1u64 << 36) - 1), (1u64 << 36) - 1);

        // We don't care about the state of bits 101-128 in the last word
    }

    #[test]
    fn test_clear_range() {
        let mut bs = Bitset::ones(128);
        bs.clear_range(60, 68);
        assert_eq!(bs.bits[0], u64::MAX ^ (0xFFu64 << 60));
        assert_eq!(bs.bits[1], u64::MAX ^ 0xF);
    }

    #[test]
    fn test_pack_unpack() -> anyhow::Result<()> {
        let mut bs = Bitset::ones(1000);
        bs.clear_range(100, 200);
        bs.clear_range(500, 600);
        let packed = bs.pack()?;
        let unpacked = Bitset::unpack(&packed)?;
        assert_eq!(bs.nr_bits, unpacked.nr_bits);
        assert_eq!(bs.bits, unpacked.bits);
        Ok(())
    }

    #[test]
    fn test_pack_efficiency() -> anyhow::Result<()> {
        let nr_bits = 1_000_000;
        let mut bs = Bitset::ones(nr_bits);

        // Clear every other bit
        for i in (0..nr_bits).step_by(2) {
            bs.clear_range(i, i + 1);
        }

        let packed = bs.pack()?;
        println!("Packed size for alternating bits: {} bytes", packed.len());

        // Clear half the bits
        let mut bs = Bitset::ones(nr_bits);
        bs.clear_range(0, nr_bits / 2);

        let packed = bs.pack()?;
        println!("Packed size for half cleared: {} bytes", packed.len());
        Ok(())
    }

    #[test]
    fn test_pack_size_vs_density() -> anyhow::Result<()> {
        let total_bits = 1_000_000; // 1 million bits
        let densities = [0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99];

        println!("Density | Packed Size (bytes) | Bytes per 1000 bits");
        println!("--------|---------------------|--------------------");

        for &density in &densities {
            let mut bitset = Bitset::ones(total_bits);
            let bits_to_clear = (total_bits as f64 * (1.0 - density)) as u64;

            let mut rng = rand::thread_rng();
            let mut cleared = 0;
            while cleared < bits_to_clear {
                let bit = rng.gen_range(0..total_bits);
                if bitset.is_set(bit) {
                    bitset.clear_range(bit, bit + 1);
                    cleared += 1;
                }
            }

            let packed = bitset.pack()?;
            let bytes_per_thousand = packed.len() as f64 * 1000.0 / total_bits as f64;

            println!(
                "{:7.2} | {:19} | {:20.2}",
                density,
                packed.len(),
                bytes_per_thousand
            );
        }

        Ok(())
    }

    #[test]
    fn test_zero_runs() {
        let mut bitset = Bitset::ones(128);

        // Clear some ranges to create zero runs
        bitset.clear_range(10, 20);
        bitset.clear_range(50, 60);
        bitset.clear_range(64, 96);
        bitset.clear_range(127, 128);

        let zero_runs: Vec<(u64, u64)> = bitset.zero_runs().collect();

        assert_eq!(zero_runs, vec![(10, 20), (50, 60), (64, 96), (127, 128)]);
    }

    #[test]
    fn test_zero_low_n_bits() {
        let mut bitset = Bitset::ones(64);
        bitset.zero_low_n_bits(0, 4);
        assert_eq!(bitset.bits[0], 0xFFFFFFFFFFFFFFF0);
    }

    #[test]
    fn test_zero_word_range() {
        let mut bitset = Bitset::ones(256);
        bitset.zero_word_range(1, 3);
        assert_eq!(bitset.bits, vec![u64::MAX, 0, 0, u64::MAX]);
    }

    #[test]
    fn test_zero_high_n_bits() {
        let mut bitset = Bitset::ones(64);
        bitset.zero_high_n_bits(0, 4);
        assert_eq!(bitset.bits[0], 0x0FFFFFFFFFFFFFFF);
    }
}

//----------------------------------------------------------------
