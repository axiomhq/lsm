// Package lsm is a sorted key-value table format and its levels, built for
// object storage: every table is immutable and read by byte range, and the
// caller owns the I/O and the manifest that names the live tables.
//
// A Table is an immutable, sorted run of entries in checksummed, zstd'd
// blocks with a block index of first and last keys, written once and read
// by byte range. A Version is the set of tables in levels: L0 holds one
// table per flush, newest first and mutually overlapping; every later
// level holds tables with disjoint key ranges. A read finds the tables
// whose [Min, Max] overlaps its range, opens one iterator each, and merges
// them newest first (MergeIter), then Resolve turns that stream of versions
// into one value per key: a Put or Delete from the newest table wins, and
// Merge operands are folded onto the value beneath them by the Merger.
// Compaction is the same iterator written back out into the next level.
//
// Keys are byte-comparable; the first byte names the key space. The
// package does not know what the spaces mean.
//
// A table, in file order: blocks of crc32c(payload), a mode byte (zstd or
// stored) and the entries; an index of each block's offset, lengths and
// first and last key; a 32-byte footer with the index offset and length,
// the entry count, crc32c(index) and the magic. Corrupt bytes return an
// error wrapping ErrCorrupt. DecompressBounded, the size-capped zstd decode
// behind every block read, is exported for other formats.
package lsm
