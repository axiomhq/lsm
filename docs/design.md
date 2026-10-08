# Design

How lsm works and what it promises. For usage, see [guide.md](guide.md).

## Comparisons

[Pebble](https://github.com/cockroachdb/pebble),
[RocksDB](https://github.com/facebook/rocksdb), and
[goleveldb](https://github.com/syndtr/goleveldb) are complete storage engines.
They own a directory: a write-ahead log, a memtable, and a manifest.

lsm has none of those. You buffer and sort writes, choose where the bytes live,
and publish versions. In return, every file is immutable and read by byte
range, and readers and compactors can run anywhere that can reach the objects.

## Tiered and leveled compaction

`Pick` uses tiered compaction by default. It rewrites each byte fewer times
than leveled compaction does when level 0 fills faster than jobs finish. On one
ingest-heavy workload, tiered took 4,355 docs/s to leveled's 3,596. In
simulation, it reads and writes 9.5x the ingested bytes, where leveled reads
and writes 25x. [docs/tiered.md](docs/tiered.md) has the details, but the
rules are short.

- Every level below 0 is one sorted run, newest first, and each key space has
  its own stack of runs.
- Level 0 becomes a new run.
- A space's runs merge by size. Equal runs pair up like the bits of a binary
  counter.
- Past `Options.MaxRuns` (8) runs, a space merges by count.
- A write-once space (`Options.Spaces`, `SpacePolicy{WriteOnce: true}`)
  never overwrites a key, so merging it reclaims only deletes. Its runs merge
  `Fanout` (8) at a time instead of in pairs, up to its own `MaxRuns`. With
  `DeadRatio` set, all its runs merge into the oldest once its tombstones
  reach that share of its puts (`TableMeta.Deletes`).

So a byte is rewritten about log2(space bytes / level-0 bytes) times, rather
than once per level-0 job. A new-run job (`Job.NewRun`) moves the runs it
displaces one level deeper. Its `Edit` lists those moved files in `Del`, and
in `Add` below level 1 under their own keys, so `Add[Level+1]` is still
exactly the new outputs.

Set `Options.Leveled` to get the leveled picker from v0.9.0 instead. Each
level has a budget of `BaseBytes` × `LevelRatio`^(n-1), and the picker drains
before it fills.

- A level over budget moves the contiguous run of files that overlaps the
  level below the least, per byte. It moves at least its excess, and at most
  `BaseBytes`. The level furthest over budget goes first.
- Level 0 compacts at `L0Trigger` files, but only when every deeper level is
  within budget. That way each level-0 job is followed by the deeper work it
  causes, and level 1 stays near `BaseBytes`.
- A level-0 job takes the oldest file's key space and every level-0 file that
  overlaps it. If you write each level-0 table to one space, a job rewrites
  only that space's share of level 1.

The leveled picker doesn't hold back your flushes. If you need to bound level
0 while deeper levels drain, that's up to you.

## Table format

| part | bytes |
| --- | --- |
| block | crc32c u32, mode byte (0 zstd, 1 stored), payload; raw is uvarint(count), then per entry uvarint(len key) key, kind byte, uvarint(len value) value |
| index | uvarint(blocks), then per block offset, length, raw length, first key, last key (all uvarint-framed), then the key filter: uvarint(length), bloom bits |
| footer | 32 bytes: index offset u64, index length u64, entry count u64, crc32c(index) u32, magic `LSM2` (`LSM1` through v0.6.2 and `DWL1` through v0.3.0: no key filter, still read) |

Blocks close at 64 KiB of raw bytes (`DefaultBlockBytes`). A value of 4 KiB or
more (`LargeValueBytes`) gets a block of its own, so a point read of it
decodes only that block. If zstd can't shrink a block by at least an eighth,
the block is stored raw.
A block that is one large value is stored raw unless zstd at least halves
it, so dense vectors stay readable by extent (`Table.Single`).

The key filter is a bloom filter at 10 bits per key, which gives about 1%
false positives. A point lookup of a key that a table doesn't hold reads no
blocks from it, so a key written once costs one block read, however deep
level 0 gets. Below level 0, a point lookup binary-searches each level for its
one candidate file, so its cost grows with the number of levels, not the
number of files.

Every read verifies checksums. Corrupt bytes return an error that wraps
`lsm.ErrCorrupt`. `lsm.DecompressBounded(data, limit)` exposes the
size-capped zstd decode that every block read uses: it refuses a frame that
claims, or decodes to, more than `limit` bytes.

## Compatibility

`OpenTableAt` fetches the index and the footer behind it in one range read.
It checks, in order, the footer's index position against the manifest, the
magic, and the index crc32c. The 32-byte footer frame, with index offset and
length first and magic last, is the same in every format. So a footer that
disagrees with the manifest is `ErrCorrupt`, but a magic the reader doesn't
know is `lsm.ErrUnsupportedFormat`: those are bytes from a newer format, not
corruption.

The magic is the format's version. While the module is v0, a format change
bumps the minor version, and readers keep accepting the previous magic when
the layout didn't change.

The manifest encoding is the JSON field names on `Version`, `FileRef`,
`TableMeta`, and `SpaceRange`. They're stable: a manifest written by any
earlier version decodes with every field in place.

### Entry size

An entry has to fit in a table of its own, so a compaction can always start a
new file with whatever entry it reads next. `Add` returns
`lsm.ErrEntryTooLarge` past `lsm.MaxEntryBytes`, which is the value plus three
times the key, 202 bytes under `MaxTableBytes`. A block's raw bytes never
exceed `MaxTableBytes`, whatever `BlockBytes` says, and a compaction closes
an output file before the entry that would push it past `FileBytes`.

Writers before v0.4.0 checked only the finished table, so their tables can hold
an entry past that limit: a key over about a third of `MaxTableBytes` with a
small value, or a value plus three times its key within about a hundred bytes
of `MaxTableBytes`, wherever it sits in its block. Such a table reads fine, but
a compaction over it fails with `ErrEntryTooLarge`.

