# Thinp1

## space maps

Hold reference count for every single metadata block and data block.  If the count
is below 3 it is stored in a couple of bits.  Above this we overflow into a btree, so each
count takes about 32bytes.  Aged pools with a lot of snapshots can have a large percentage
of the metadata space used up with the space maps alone.

## mapping trees

- Entry: thin block (64bit), data block (40bit), snap time (24 bit)
- ~240 entries per node max.

# Thinp 2

## No space maps

We do have to store the free blocks in the checkpoint.  But this is stored at much better than
1 bit per block.

## Mapping trees

- Entry: thin_begin (64bit), data_begin (64bit), snap time (64 bit), len (64bit)
- ~120 entries per node max

# Node compression

- BTrees not needed for fast path since kernel will have it's own mapping cache.
- Space is more important, so we should look at compressing.

- compile to program for tiny abstract machine (currently 12 instructions).  Example
  program (first column is the nr bits the instruction consumes):

  ```
      85    keyframe 29232, 25248200, 4559
      9     len 8
      1     emit
      11    data +80
      1     emit              ;; thin=29240, data=25248288, time=4559, len=8

      11    shift 3
      7     data +1
      1     emit              ;; thin=29248, data=25248304, time=4559, len=8

      11    data +102
      6     len 2
      1     emit              ;; thin=29256, data=25249128, time=4559, len=16

      7     data +13
      9     len 4
      1     emit              ;; thin=29272, data=25249248, time=4559, len=32

      11    data +46
      6     len 1
      1     emit              ;; thin=29304, data=25249648, time=4559, len=8

      14    data -47
      6     len 3
      1     emit              ;; thin=29312, data=25249280, time=4559, len=24

      11    data +51
      6     len 1
      1     emit              ;; thin=29336, data=25249712, time=4559, len=8

      14    data -52
      6     len 2
      1     emit              ;; thin=29344, data=25249304, time=4559, len=16

      11    data +59
      6     len 1
      1     emit              ;; thin=29360, data=25249792, time=4559, len=8

      7     data +13
      1     emit              ;; thin=29368, data=25249904, time=4559, len=8

      7     data +1
      6     len 2
      1     emit              ;; thin=29376, data=25249920, time=4559, len=16

      7     data +4
      6     len 1
      1     emit              ;; thin=29392, data=25249968, time=4559, len=8

      11    data +47
      1     emit              ;; thin=29400, data=25250352, time=4559, len=8

      7     data +2
      1     emit              ;; thin=29408, data=25250376, time=4559, len=8

      7     data +10
      1     emit              ;; thin=29416, data=25250464, time=4559, len=8

      7     data +1
      1     emit              ;; thin=29424, data=25250480, time=4559, len=8

      11    data +18
      1     emit              ;; thin=29432, data=25250632, time=4559, len=8

      7     data +8
      1     emit              ;; thin=29440, data=25250704, time=4559, len=8
        ```

- Instructions get huffman encoded and written to a bitstream.

- Running a program generates a set of mappings.

- multiple programs per node means lookups can select the appropriate sub program for the query.

- Node modifications (insert, remove_range etc) get appended to back of node and executed in batches.  This amortises the cost of recompiling the programs.

- Running with example thinp1 metadata from customers, we now average ~1500 entries per node.  ~2.7 bytes per range mapping.

- Experiments with zstd suggest we can get a chunk more compression.   But I think we're in danger of over optimising for thinp1 fragmented metadata, which we're going to try and avoid in thinp2.

# Packing fragmented thinp1 metadata

bz_1763895
mean run length: 2.2
thinp1: 14092m
thinp2:   518m
ratio: 27:1 

bz_2039978
mean run length: 1.68
thinp1: 4102m  !!!???
thinp2:   38.5m
ratio: 100:1

bz_1806798c32
mean run length: 5.1
thinp1: 151m
thinp2:   2.5m
ratio: 56:1
 
Rule of thumb: 2 (no space maps) * 6 (thin2 mappings / thin1 mappings) * (average run length)


# Improving average run length

- Per thin allocators
- Ephemeral snapshots (Allow snapshots to be reversed)
- metadata only snapshots that don't trigger copy-on-write
- Integrate blk-archive for the very common 'time-machine' use case
- Alternative method for tracking deltas.
- Data defrag

Aiming for 1000:1 ratio.  ie. A big enterprise installation to have 16m of metadata rather than 16g.

