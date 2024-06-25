# Overview

Experiment with a thin provisioning metadata format that
doesn't use reference counting space maps, but instead does
garbage collection.  Instead of having a separate GC thread
(stop the world is obviously out of the question), we push
the GC process forward as part of block allocation.


# Goals

- Metadata must use much less space that thinp1.
  - store ranges
  - compress mapping nodes, sacrifice cpu.  There will be a live mapping cache
    in front of the metadata, so performance isn't so critical.

- More resilience; in thinp1, if a node high up in a btree gets damaged it can be
  difficult to repair.

- Live recovery rather than offline thin_repair.

- Support 4k block size.  Just to confound making metadata take up less space.

- Isolate thin transactions from each other.  In thinp1 there is only a global
  transaction.  As you get more active thins you are going to naturally get more
  commits triggered by REQ_FLUSH/FUA.

- Support short lived snapshots that don't have a permanent performance hit on the origin.

- Integrate with blk_archive to make it easy to migrate volumes out of the live pool.
