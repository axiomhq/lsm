# lsm

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/axiomhq/lsm)
[![Latest Release](https://img.shields.io/github/v/release/axiomhq/lsm?style=flat-square)](https://github.com/axiomhq/lsm/releases/latest)

lsm is a log-structured merge tree for object storage.

The basic idea is that an LSM tree is mostly a file format and a set of rules
for merging files, and neither one needs to own a disk. So this package doesn't
do any I/O. You give it sorted entries, and it gives you table bytes to store.
You give it byte ranges back, and it answers reads. You keep the `Version`,
which lists the live tables, in a manifest of your own, and you update it with
a compare-and-swap. Every table is written once and never modified, which is
exactly what object stores like S3 are good at.

A key's history is a stack of versions: `Put`, `Delete`, and `Merge`. Reads
fold that history into one value per key. Compaction is that same fold,
written back out one level deeper.

```sh
go get github.com/axiomhq/lsm
```

## Usage

Here's the whole lifecycle: flush, compact, read. A map stands in for the
object store. This is `ExampleCompact` in [example_test.go](example_test.go),
so `go test` checks it; the file has more examples, including merge
operands.

```go
ctx := context.Background()

// A map stands in for your object store.
objects := map[string][]byte{}

// put stores a table and returns the object key it lives under.
put := func(ctx context.Context, level int, seq uint64, data []byte) (string, error) {
	key := fmt.Sprintf("%d-%d", level, seq)
	objects[key] = data
	return key, nil
}

// open reads a table back. A real Source reads byte ranges of an object.
open := func(ctx context.Context, f lsm.FileRef) (*lsm.Table, error) {
	return lsm.OpenTableAt(ctx, lsm.BytesSource(objects[f.Key]), f.TableMeta)
}

opts := lsm.DefaultOptions()
opts.L0Trigger = 2 // compact early, for the demo
opts.Workers = 1   // put isn't safe for concurrent use

// Two flushes, each a sorted batch with one entry per key.
var v lsm.Version
v, _, err := lsm.Flush(ctx, v, []lsm.Entry{
	{Key: []byte("a"), Kind: lsm.KindPut, Value: []byte("1")},
	{Key: []byte("b"), Kind: lsm.KindPut, Value: []byte("2")},
}, opts, put)
if err != nil {
	log.Fatal(err)
}
v, _, err = lsm.Flush(ctx, v, []lsm.Entry{
	{Key: []byte("a"), Kind: lsm.KindDelete},
	{Key: []byte("c"), Kind: lsm.KindPut, Value: []byte("3")},
}, opts, put)
if err != nil {
	log.Fatal(err)
}

// Compact until Pick has nothing left to do.
r := lsm.Reader{Open: open}
for job, ok := lsm.Pick(v, opts); ok; job, ok = lsm.Pick(v, opts) {
	if v, _, err = lsm.Compact(ctx, v, job, opts, r, put); err != nil {
		log.Fatal(err)
	}
}
fmt.Printf("level 0: %d files, level 1: %d files\n", len(v.Levels[0]), len(v.Levels[1]))

// Read it all back.
it, err := r.Iter(ctx, v, nil, nil)
if err != nil {
	log.Fatal(err)
}
for ok := it.SeekGE(nil); ok; ok = it.Next() {
	fmt.Printf("%s=%s\n", it.Key(), it.Value())
}
if err := it.Err(); err != nil {
	log.Fatal(err)
}
```

```
level 0: 0 files, level 1: 2 files
b=2
c=3
```

The delete hid `a`, and compaction into the bottom level dropped it. Level 1
ended up with two files, not one, because of key spaces.

### Key spaces

The first byte of every key is its space. This is a format rule, not a
suggestion: compaction never writes a file that spans two spaces, it tracks
each file's per-space ranges (`FileRef.Spaces`), and it leaves alone any file
that no input writes to. The package doesn't interpret the space byte beyond
that.

### Writing

Sort your entries by key, one entry per key, and call `lsm.Flush`. It writes
one level-0 table through your `Putter` and returns the next `Version`. Save
that version with a conditional write. If you lose the race, reload and try
again.

A `Putter` stores `data` and returns its object key. `seq` is unique within
one level of a version, and no more. Two writers that start from the same
version hand out the same seqs, so if your writers can race, don't name
objects by `seq` alone: add the level and a writer ID, or let the store pick
the name. A file's identity is `FileRef.Key`.

If you only want table bytes, without levels, use
`lsm.BuildTable(entries, lsm.DefaultBlockBytes)`, or stream with
`lsm.NewTableWriter(blockBytes)`, `Add`, and `Finish`.

### Reading

A `Reader` needs an `Opener`, which turns a `FileRef` into a `*Table`. Call
`lsm.OpenTableAt` with a `Source`, which is anything with
`ReadAt(ctx, off, length int64) ([]byte, error)`. `lsm.BytesSource` wraps
bytes in memory.

- `r.Get(ctx, v, key)` is a point read.
- `r.Iter(ctx, v, lo, hi)` is a range read over `[lo, hi)`. A nil `SeekGE`
  lands on `lo`. Check `it.Err()` when you're done.
- `r.Locate(ctx, v, key)` is for large put-only values. When the value sits
  alone in a stored block, you get its extent in the table (`Table.Single`),
  and you can read exactly those bytes with `l.Table.ReadAt`. `l.File.Key`
  names the object it came from.
- `t.Span(lo, hi)` returns the block indexes of a table that may hold keys in
  `[lo, hi)`, from the in-memory index alone. That's enough to build a cache
  key for a range without reading any blocks.

### Merging

`KindMerge` entries are operands, and a `Merger` folds them onto the value
beneath them. Leave `Reader.Merger` nil if you never write `KindMerge`.

The module ships one Merger, `setmerge.Merger`, for roaring-bitmap sets with
add and remove operands. Build values with `setmerge.Value`, operands with
`setmerge.Operand`, and read them back with `setmerge.Decode`.

### Compacting

Compaction is three steps.

1. `job, ok := lsm.Pick(v, opts)` picks the next job. `ok` is false when
   every level is in shape.
2. `next, edit, err := lsm.Compact(ctx, v, job, opts, r, put)` runs it.
3. Save `next` with a conditional write. Delete the input objects only after
   that write succeeds.

If `Compact` fails, `edit.Add` lists the outputs it already stored. Those are
orphans, and you can delete them. `edit.NextSeq` is the first sequence no
output used. Before you retry, apply `lsm.Edit{NextSeq: edit.NextSeq}` to the
version you retry from, so the retry doesn't reuse those seqs. Never apply the
failed edit's `Add`.

If you lose the write to a newer version `head`, you don't have to throw the
work away. `head.Rebase(v, edit)` replays the compaction onto `head`, and you
can write again. Rebase works when the only other writer added level-0 files
with `Flush`. Anything else, like a second compaction over the same files,
returns `lsm.ErrStale`: delete your outputs and pick again.

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

## Options

- `Options.Workers` caps how many key-range partitions one compaction merges
  at once.
- `Options.ReadAheadBytes` (1 MiB) is the window a merge reads its inputs in.
  That's one range read per window, not per block, and the next window is
  read while the merge walks the current one.
- `Options.Uploads` (defaults to `Workers`) caps how many finished output
  files are being put at once. A merge hands a file to `put` and moves on,
  and `Compact` returns once every put has finished.
- `Options.MaxTableAge` bounds how long a delete or overwrite can wait above
  the bottom level, which is where the versions it supersedes get dropped.

That last one needs a bit more explanation. Every `FileRef` carries `Oldest`,
the unix second of the oldest write whose superseded versions it may still
hold. For a `Flush`, or a compaction into the bottom level, that's the write
time. Otherwise it's the oldest `Oldest` of the files merged into it. When no
size rule fires, `Pick` compacts the shallowest file above the bottom level
whose `Oldest` is past the age (level 0 goes whole), and later picks carry it
down. A key's superseded bytes are gone within about `MaxTableAge` plus one
job per level, as long as something keeps calling `Pick` on an idle version.
An `Oldest` of 0, from a manifest written before v0.6.0, counts as the epoch.

## Concurrency

A `Table` is safe for concurrent reads. An iterator isn't.

`Compact` runs up to `Options.Workers` merges at once, so `Source.ReadAt` and
`Merger` must be safe for concurrent use. Each merge also reads its next
window from a goroutine of its own. `Putter` is called from up to
`Options.Uploads` goroutines (`Workers` when negative), so it must be safe for
concurrent use unless that number is 1. `Opener` is called from one goroutine
at a time.

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

Writers before v0.4.0 checked only the finished table, so a table they wrote
can hold an entry past that limit. That means a key over about a third of
`MaxTableBytes` with a small value, or a value plus three times its key within
about a hundred bytes of `MaxTableBytes`, wherever it sits in its block. The
table still reads fine, but a compaction over it fails with
`ErrEntryTooLarge`.

## Subpackages

Package keyenc builds composite keys whose byte order matches the order of
their values. `keyenc.AppendString` and `keyenc.AppendFloat64` encode,
`keyenc.String` and `keyenc.Float64` decode, and `keyenc.PrefixEnd(p)` is the
exclusive upper bound of a scan over prefix `p`.

Package postings stores (docnum, weight) lists, sorted by docnum, as delta
varints, with `postings.Encode` and `postings.Decode`. Corrupt bytes return an
error that wraps `lsm.ErrCorrupt`.

## Comparisons

[Pebble](https://github.com/cockroachdb/pebble),
[RocksDB](https://github.com/facebook/rocksdb), and
[goleveldb](https://github.com/syndtr/goleveldb) are complete storage engines.
They own a directory: a write-ahead log, a memtable, and a manifest. lsm is
the part underneath. It has no log and no memtable, and it doesn't own a
manifest. You buffer and sort writes yourself, you decide where the bytes
live, and you decide how versions are published. In exchange, every file is
immutable and read by byte range, which fits object storage, and readers and
compactors can run anywhere that can reach the objects.

## Testing

```sh
go test -race ./...
go test -run '^$' -bench . -count 10 .
```

## License

MIT, see [LICENSE](LICENSE).
