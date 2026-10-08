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
// Keys are byte-comparable and the first byte is the key's space. That is
// a format rule: compaction never writes a file that spans two spaces,
// tracks each file's per-space ranges (FileRef.Spaces), and uses them to
// leave alone the files no input writes to. The package does not interpret
// the space byte beyond that.
//
// The table layout is documented at the constants in table.go and in
// docs/design.md. Corrupt bytes return an error wrapping ErrCorrupt.
// DecompressBounded, the size-capped zstd decode behind every block read,
// is exported for other formats.
//
// Subpackages keyenc (order-preserving key components) and postings
// (delta-varint docnum lists) build the keys and values stored in tables.
package lsm
