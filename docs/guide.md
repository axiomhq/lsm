# Guide

How to use lsm. For how it works, see [design.md](design.md).

## Key spaces

The first byte of every key is its space. This is a format rule:

- Compaction never writes a file that spans two spaces.
- It tracks each file's per-space ranges in `FileRef.Spaces`.
- It leaves alone any file that no input writes to.

The package doesn't interpret the space byte beyond that.

## Writing

Sort your entries by key, one entry per key, and call `lsm.Flush`. It writes
one level-0 table through your `Putter` and returns the next `Version`. Save
that version with a conditional write. If you lose the race, reload and try
again.

A `Putter` stores `data` and returns its object key. `seq` is unique within one
level of a version, and nothing more. Two writers that start from the same
version hand out the same seqs. If writers can race, don't name objects by
`seq` alone. Add the level and a writer ID, or let the store pick the name. A
file's identity is `FileRef.Key`.

If you only want table bytes, without levels, use
`lsm.BuildTable(entries, lsm.DefaultBlockBytes)`, or stream with
`lsm.NewTableWriter(blockBytes)`, `Add`, and `Finish`.

## Reading

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

## Merging

`KindMerge` entries are operands, and a `Merger` folds them onto the value
beneath them. Leave `Reader.Merger` nil if you never write `KindMerge`.

The module ships one Merger, `setmerge.Merger`, for roaring-bitmap sets with
add and remove operands. Build values with `setmerge.Value`, operands with
`setmerge.Operand`, and read them back with `setmerge.Decode`.

## Compacting

Three steps:

1. `job, ok := lsm.Pick(v, opts)` picks the next job. `ok` is false when every
   level is in shape.
2. `next, edit, err := lsm.Compact(ctx, v, job, opts, r, put)` runs it.
3. Save `next` with a conditional write. Delete the input objects only after
   that write succeeds.

**If `Compact` fails:** `edit.Add` lists the outputs it already stored. They
are orphans, so you can delete them. `edit.NextSeq` is the first sequence no
output used. Before you retry, apply `lsm.Edit{NextSeq: edit.NextSeq}` to the
version you retry from, so the retry doesn't reuse those seqs. Never apply the
failed edit's `Add`.

**If you lose the write to a newer version `head`:** keep the work.
`head.Rebase(v, edit)` replays the compaction onto `head`, and you can write
again. Rebase works only when the other writer added level-0 files with
`Flush`. Anything else, such as a second compaction over the same files,
returns `lsm.ErrStale`. Delete your outputs and pick again.

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

How `MaxTableAge` works: every `FileRef` carries `Oldest`, the unix second of
the oldest write whose superseded versions it may still hold.

- For a `Flush`, or a compaction into the bottom level, `Oldest` is the write
  time.
- Otherwise it is the oldest `Oldest` of the files merged into it.
- When no size rule fires, `Pick` compacts the shallowest file above the bottom
  level whose `Oldest` is past the age. Level 0 goes whole. Later picks carry
  it down.

A key's superseded bytes are gone within about `MaxTableAge` plus one job per
level, as long as something keeps calling `Pick` on an idle version. An
`Oldest` of 0, from a manifest written before v0.6.0, counts as the epoch.

## Concurrency

A `Table` is safe for concurrent reads. An iterator isn't.

`Compact` runs up to `Options.Workers` merges at once, so `Source.ReadAt` and
`Merger` must be safe for concurrent use. Each merge also reads its next
window from a goroutine of its own. `Putter` is called from up to
`Options.Uploads` goroutines (`Workers` when negative), so it must be safe for
concurrent use unless that number is 1. `Opener` is called from one goroutine
at a time.

## Subpackages

Package keyenc builds composite keys whose byte order matches the order of
their values. `keyenc.AppendString` and `keyenc.AppendFloat64` encode,
`keyenc.String` and `keyenc.Float64` decode, and `keyenc.PrefixEnd(p)` is the
exclusive upper bound of a scan over prefix `p`.

Package postings stores (docnum, weight) lists, sorted by docnum, as delta
varints, with `postings.Encode` and `postings.Decode`. Corrupt bytes return an
error that wraps `lsm.ErrCorrupt`.

