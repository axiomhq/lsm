# lsm

```sh
go get github.com/axiomhq/lsm
```

Sorted, immutable key-value tables in levels, with merge and compaction,
built for object storage. Every file is written once and read by byte range.
A key's history is versions (`Put`, `Delete`, `Merge`) stored as values. The
package does no I/O: you store the table bytes, you read byte ranges back, and
you own the manifest that holds the `Version` (compare-and-swap it).

## Write a table

1. Sort your entries by key, one entry per key: `lsm.Entry{Key, Kind, Value}` with `Kind` one of `KindPut`, `KindDelete`, `KindMerge`.
2. Flush them as a level-0 table: `next, ref, err := lsm.Flush(ctx, v, entries, lsm.DefaultOptions(), put)`.
   `put` is `func(ctx, level int, seq uint64, data []byte) (key string, err error)`: store `data`, return its object key.
3. Save `next` in your manifest with a conditional write. On a lost race, reload and retry.

For bytes without levels, `data, meta, err := lsm.BuildTable(entries, lsm.DefaultBlockBytes)`, or `lsm.NewTableWriter(blockBytes)` with `Add` and `Finish` to stream.

## Read

1. Write an `Opener`: `func(ctx, f lsm.FileRef) (*lsm.Table, error)`. Inside, call `lsm.OpenTableAt(ctx, src, f.TableMeta)`.
   `src` implements `ReadAt(ctx, off, length int64) ([]byte, error)`. Use `lsm.BytesSource(data)` for bytes in memory.
2. Build a reader: `r := lsm.Reader{Open: open, Merger: setmerge.Merger{}}`.
3. Point read: `value, ok, err := r.Get(ctx, v, key)`.
4. Range read: `it, err := r.Iter(ctx, v, lo, hi)`, then `for ok := it.SeekGE(nil); ok; ok = it.Next() { it.Key(); it.Value() }` and check `it.Err()`. A nil seek lands on `lo`.

For large put-only values, `l, ok, err := r.Locate(ctx, v, key)` returns the
value's extent in its table (`Table.Single`) when it sits alone in a stored
block, so you read just those bytes with `l.Table.ReadAt`.

`Merger` folds `KindMerge` operands onto the value beneath them; leave it nil
when no key uses `KindMerge`. `setmerge.Merger` (import
`github.com/axiomhq/lsm/setmerge`) ships with the module: roaring-bitmap sets
with add/remove operands (`setmerge.Value`, `setmerge.Operand`,
`setmerge.Decode`).

## Compact

1. Pick a job: `job, ok := lsm.Pick(v, opts)`. `ok` is false when every level is in shape.
2. Run it: `next, edit, err := lsm.Compact(ctx, v, job, opts, r, put)`, with `r` the `Reader` from Read.
3. Save `next` with a conditional write. Delete the inputs' objects only after that write succeeds.
   On error, `edit.Add` lists the output files already stored, orphans you may delete, and `edit.NextSeq` is the first sequence no output used. Before retrying, apply `lsm.Edit{NextSeq: edit.NextSeq}` to the version you retry from, never the failed edit's `Add`.
   `seq` is unique within one level of a version, not across writers that start from one: name objects by more than `seq` when writers race, and identify files by `Key`.
4. Lost the write to a newer version `head`? Rebase: `next, err = head.Rebase(v, edit)`, then write again. `lsm.ErrStale` means another compaction changed the same files: delete the outputs and pick again.

Rebase holds when the only other writer adds level-0 files (`Flush`).
`Options.Workers` caps the key-range partitions merged at once.

## Table format

| part | bytes |
| --- | --- |
| block | crc32c u32, mode byte (0 zstd, 1 stored), payload; raw is uvarint(count), then per entry uvarint(len key) key, kind byte, uvarint(len value) value |
| index | uvarint(blocks), then per block offset, length, raw length, first key, last key (all uvarint-framed) |
| footer | 32 bytes: index offset u64, index length u64, entry count u64, crc32c(index) u32, magic `LSM1` (`DWL1` through v0.3.0, same layout, still read) |

Blocks close at 64 KiB raw (`DefaultBlockBytes`). A value of 4 KiB or more
(`LargeValueBytes`) is a block of its own, so a point read decodes only that
block. A block zstd cannot shrink by an eighth is stored raw. Every read
checks the checksums, and corrupt bytes return an error wrapping
`lsm.ErrCorrupt`.

`lsm.DecompressBounded(data, max)` is that size-capped zstd decode on its
own: a frame claiming or decoding to more than `max` bytes is refused.

## Concurrency and format

A `Table` is safe for concurrent reads; an iterator is not. `Compact` runs up
to `Options.Workers` merges at once: `Source.ReadAt` and `Merger` are called
from all of them and must be safe for concurrent use; `Putter` is too, unless
`Workers` is 1; `Opener` is called from one goroutine at a time.

`OpenTableAt` reads the index and the footer behind it in one range read and
checks, in order, the footer's index position against the manifest, the
magic, and the index crc32c. The 32-byte footer frame (index offset and
length first, magic last) is fixed for every format, so a footer that
disagrees with the manifest is `ErrCorrupt` and a magic the reader does not
know is `lsm.ErrUnsupportedFormat`: bytes from a newer format, not
corruption. The magic is the format's version; while the module is v0 a
format change bumps its minor version, and a reader keeps accepting the
previous magic when the layout did not change.

An entry must fit a table of its own: `Add` returns `lsm.ErrEntryTooLarge`
past `lsm.MaxEntryBytes` (value plus three times the key, 128 bytes under
`MaxTableBytes`), so a compaction can always start a file with any entry it
reads. Writers before v0.4.0 checked only the finished table, so a table they
wrote could hold an entry past that limit: a key over about a third of
`MaxTableBytes` with a small value, or a value plus three times its key within
about a hundred bytes of `MaxTableBytes`, wherever it sat in its block. It still reads, but a compaction
over it fails with `ErrEntryTooLarge`. A block's raw bytes never exceed `MaxTableBytes` whatever
`BlockBytes` says, and a compaction closes an output file before the entry
that would take it past `FileBytes`.

The manifest encoding is the json names on `Version`, `FileRef`, `TableMeta`
and `SpaceRange`. They are stable: a manifest written by any earlier version
decodes with every field in place.

## Key components and postings

1. `keyenc.AppendString` and `keyenc.AppendFloat64` build composite keys whose byte order is the value order. `keyenc.String` and `keyenc.Float64` read them back, and `keyenc.PrefixEnd(p)` is the exclusive upper bound of a scan over prefix `p`.
2. `postings.Encode` and `postings.Decode` store (docnum, weight) lists sorted by docnum as delta varints. Corrupt bytes return an error wrapping `lsm.ErrCorrupt`.

## Test

```sh
go test -race ./...
go test -run '^$' -bench . -count 10 .
```

## License

MIT, see [LICENSE](LICENSE).
