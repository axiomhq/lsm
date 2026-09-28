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

1. Write an `Opener`: `func(ctx, f lsm.FileRef) (*lsm.Table, error)`. Inside, call `lsm.OpenTableAt(ctx, src, f.Meta())`.
   `src` implements `ReadAt(ctx, off, length int64) ([]byte, error)`. Use `lsm.BytesSource(data)` for bytes in memory.
2. Point read: `value, ok, err := v.Get(ctx, open, merger, key)`.
3. Range read: `it, err := v.Iter(ctx, open, merger, lo, hi)`, then `for it.Next() { it.Key(); it.Value() }` and check `it.Err()`.

`merger` folds `KindMerge` operands onto the value beneath them. `SetMerger`
ships in the package: roaring-bitmap sets with add/remove operands
(`SetValue`, `SetOperand`, `DecodeSet`).

## Compact

1. Pick a job: `job, ok := lsm.Pick(v, opts)`. `ok` is false when every level is in shape.
2. Run it: `next, edit, err := lsm.Compact(ctx, v, job, opts, open, put)`.
3. Save `next` with a conditional write. Delete the inputs' objects only after that write succeeds.
4. Lost the write to a newer version `head`? Rebase: `next, err = head.Rebase(v, edit)`, then write again. `lsm.ErrStale` means another compaction changed the same files: delete the outputs and pick again.

Rebase holds when the only other writer adds level-0 files (`Flush`).
`Options.Workers` caps the key-range partitions merged at once.

## Table format

| part | bytes |
| --- | --- |
| block | crc32c u32, mode byte (0 zstd, 1 stored), payload; raw is uvarint(count), then per entry uvarint(len key) key, kind byte, uvarint(len value) value |
| index | uvarint(blocks), then per block offset, length, raw length, first key, last key (all uvarint-framed) |
| footer | 32 bytes: index offset u64, index length u64, entry count u64, crc32c(index) u32, magic `DWL1` |

Blocks close at 64 KiB raw (`DefaultBlockBytes`). A value of 4 KiB or more
(`LargeValueBytes`) is a block of its own, so a point read decodes only that
block. A block zstd cannot shrink by an eighth is stored raw. Every read
checks the checksums, and corrupt bytes return an error wrapping
`lsm.ErrCorrupt`.

`lsm.DecompressBounded(data, max)` is that size-capped zstd decode on its
own: a frame claiming or decoding to more than `max` bytes is refused.

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
