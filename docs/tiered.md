# Tiered compaction

Since v0.10.0 `Pick` tiers by default. `Options.Leveled` selects the leveled
picker of v0.9.0, which drains deeper levels before level 0.

## Why it is the default

On one ingest-heavy workload (same data, steady state), tiered took 4,355
docs/s and leveled 3,596. Neither stalled a flush on level 0. Tiered lost 1
manifest write to a concurrent writer per 12 flushes, leveled 3. The
simulations below agree: tiered reads and writes half or less of what
leveled does.

## Why tiered

Leveled compaction merges level 0 into level 1. A level-0 file whose keys are
random within its key space overlaps every level-1 file of that space. So each
level-0 job reads and rewrites all of level 1. When level 0 always holds
`L0Trigger` files (a writer flushing faster than jobs finish), the v0.8 picker
always preferred level 0: level 1 never drained to level 2, and grew without
bound, and so did the cost of every level-0 job. The v0.9.0 picker drains
deeper levels first, which bounds level 1, but every level-0 job still
rewrites its spaces' share of level 1, and each byte then moves down every
level.

## Shape

- Level 0 is unchanged: flushed files, newest first, overlapping.
- Every level below 0 is one sorted run: disjoint files sorted by key, newest
  level first. `Version.Levels` is still the persisted form: a run is a level.
- Each key space (the key's first byte) has its own stack of runs. A file below
  level 0 holds one space, because compaction starts a new file per space. So the
  runs of one space merge without reading another's. A level may hold some
  spaces and not others.

## Picks

1. Level 0 at `L0Trigger` files becomes a new run (`Job.NewRun`). It merges with
   nothing. The spaces level 1 holds shift one level deeper, each down to its
   first free level, so level 1 is free for the outputs. The shift is part of the
   job's `Edit`: moved files are in `Del` and in `Add` below level 1, under their
   own keys. `Add[1]` holds only new outputs.
2. Size: within a space, the newest window of adjacent runs in which every run
   is no bigger than the runs before it put together merges into its oldest run.
   Equal runs pair up like the bits of a binary counter.
3. Count: past `MaxRuns` (8) runs in a space, the adjacent window of
   `runs - MaxRuns + 1` runs with the fewest bytes merges.
   A write-once space (`Options.Spaces`, `SpacePolicy.WriteOnce`) skips rule 2:
   its newest `Fanout` (8) adjacent runs within twice the newest's bytes merge
   into the oldest of them, it counts to its own `MaxRuns`, and, with
   `DeadRatio` set, every run merges into the oldest once its tombstones reach
   that share of its puts. Its keys never repeat across runs, so a pair merge buys fewer runs
   and reclaims nothing; an 8-way merge buys the same for fewer rewrites.
4. Age (`MaxTableAge`): an aged file in any run but a space's oldest merges that
   run and every older run of the space into the oldest. An aged level-0 file
   merges level 0 and every level into the deepest. That only happens to an idle
   version, because an active one makes level 0 a new run first. Either way, the
   write's tombstones and superseded versions drop in one job.

Among the merges that rules 2 and 3 owe, the one with the fewest bytes runs first.
A merge is an ordinary `Job`: `Inputs` are the newer runs' files, newest first.
`Overlap` is the oldest run's files. `Compact` partitions by `Overlap`, and a
file no input key falls in stays where it is. A partition scans only the input
files that meet its range. A job with no overlap (a new run) is partitioned by
key space.

`Rebase` works as before: a concurrent `Flush` adds only level-0 files, and
every job's edit is computed against levels 1 and deeper as they were.

## Cost

With `N` = a space's bytes / its level-0 bytes per new run:

- Writes: a byte is written once by its flush and once into a new run. Then it
  is merged at most about log2(N) times. Merges that cascade (1+1+2+4 into 8)
  rewrite a byte less often than that.
- Reads: a point lookup probes a space's level-0 files and its runs, about
  log2(N) and at most `MaxRuns` once merges catch up. Each probe is a binary search
  of an in-memory file list and a key filter check. A range scan merges one
  iterator per run.
- Space: an overwritten or deleted key keeps its old versions until a merge
  reaches the run that holds them. With overwrite-heavy data, a space holds up to
  about twice its live bytes before the largest merge.

## Simulations

Both replay 300 flushes through `Pick`, on file sizes alone. Each flush is
0.7 GB: 24 level-0 files, 2 per space over 12 spaces, one every 35 s. One job
runs at a time at 270 MB/s. No key is overwritten.

`TestFoldPatternSimulation` (tiered_sim_test.go): level-0 files span their
space. A flush waits while level 0 holds more than two flushes' bytes and a
level-0 job is owed.

| picker | compaction read+written per ingested byte | write amp | max L0 files | flushes that waited | ingest MB/s | biggest job read |
| --- | --- | --- | --- | --- | --- | --- |
| leveled | 25.0× | 13.5× | 60 | 4 (1 s) | 20.0 | 1.7 GB |
| tiered | 9.5× | 5.8× | 48 | 0 | 20.0 (the flush rate) | 15.3 GB |
| leveled, one space half the bytes | 25.2× | 13.6× | 76 | 11 (10 s) | 20.0 | 2.7 GB |
| tiered, one space half the bytes | 9.5× | 5.8× | 72 | 4 (159 s) | 19.7 | 46.2 GB |

The v0.8 leveled picker read+wrote 34.7× here, with 42 flushes waiting.
Tiered holds up to 13 runs in one space while merges catch up after new runs.
Level-0 jobs go first. Once the merges catch up, a space settles at 9 runs or
fewer.

`TestFoldPatternWriteAmp` (sim_test.go): each level-0 file holds half its
space. A job costs its read and written bytes at 270 MB/s, and a flush waits
while level 0 holds more than 2 GiB at `L0Trigger`.

| picker | compaction written per ingested byte | read+written | flushes that waited | stalled | ingest MB/s |
| --- | --- | --- | --- | --- | --- |
| leveled | 8.7× | 17.4× | 224 | 3,202 s (23%) | 15 |
| tiered | 4.5× | 8.9× | 2 | 15 s (0%) | 20 |

Tiered pays for this in read cost and job size: a lookup probes a space's
runs, and a merge of a space's largest runs reads tens of GB in one job.

`TestWriteOnceSimulation` (tiered_sim_test.go): the same flushes, split by a
vector index's shares: one write-once space of full vectors 50%, one of
document blocks 20%, four tiered spaces 7.5% each. Written is level 0 plus
compaction, per ingested byte.

| policy | written, 300 flushes | vectors space | max runs in a space | written, 60 flushes |
| --- | --- | --- | --- | --- |
| tiered | 5.54× | 5.73× | 10 | 4.56× |
| write-once, count rule alone, MaxRuns 16 | 8.50× | 9.48× | 20 | |
| write-once, count rule alone, MaxRuns 32 | 6.25× | 6.35× | 36 | |
| write-once, count rule alone, MaxRuns 64 | 4.94× | 4.49× | 68 | |
| write-once, fanout 4, MaxRuns 64 | 5.65× | 5.68× | 13 | 3.97× |
| write-once, fanout 8, MaxRuns 32 | 4.41× | 3.83× | 18 | 3.41× |
| write-once, fanout 16, MaxRuns 64 | 4.42× | 3.81× | 31 | 3.31× |

The count rule alone (a picker draft) merges the cheapest adjacent pair past
`MaxRuns`, which keeps rewriting the newest run: worse than pairs below 64
runs. Fanout 8 writes a fifth less than tiered at 18 runs; 16 adds runs and
saves nothing more.

`BenchmarkRuns` (bench_test.go) is the read cost of those runs, in memory:
8,192 4 KiB values striped over the runs.

| runs | Get | Locate | 64-key Iter |
| --- | --- | --- | --- |
| 8 | 1.11 µs | 0.57 µs | 74 µs |
| 18 | 1.67 µs | 1.00 µs | 91 µs |
| 32 | 2.39 µs | 1.54 µs | 119 µs |
| 64 | 3.89 µs | 2.60 µs | 170 µs |

A key filter keeps a lookup at one block read whatever the runs; the cost is
CPU: about 50 ns a run for Get, 36 ns for Locate.

`DeadRatio` on a growing vector index (2M rows streamed, 128-d, three LSMs,
splits deleting a quarter to a half of a round's blocks; bytes written per
row, the store's total, n = 3 against tiered's 2,271, n = 4):

| write-once F, V, R, D, E | bytes written per row |
| --- | --- |
| DeadRatio 0.25 | 2,824 (+24%) |
| DeadRatio 0.5 | 2,375 (+5%) |
| DeadRatio 1 | 1,927 (−15%) |
| DeadRatio 0 (never) | 1,817 (−20%) |

Every reclaim rewrote the whole space; under that churn the reclaims cost
more than the pairs they replace. A low ratio pays where deletes are rare.
