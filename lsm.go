package lsm

import "errors"

// ErrCorrupt identifies invalid durable bytes: a table, block or version
// that fails its checksum or its framing. Every such error wraps it.
var ErrCorrupt = errors.New("corrupt")

// ErrNoMerger is a read that meets a merge operand with no Merger to fold
// it.
var ErrNoMerger = errors.New("lsm: merge entry without a Merger")

// Kind is an entry's role in a key's history.
type Kind uint8

const (
	// KindPut sets the key's value, hiding everything older.
	KindPut Kind = 1
	// KindDelete hides everything older; compaction into the bottom level
	// drops it.
	KindDelete Kind = 2
	// KindMerge is an operand for the Merger, applied over what lies
	// beneath it.
	KindMerge Kind = 3
)

// Entry is one key version. Key and Value alias the buffer they were read
// from and are valid until the iterator that produced them advances.
type Entry struct {
	Key   []byte
	Kind  Kind
	Value []byte
}

// Merger folds a key's merge operands, oldest first. An error is an
// operand or base it cannot decode; reads wrap it in ErrCorrupt.
type Merger interface {
	// Full computes the key's value from base (nil when no Put lies beneath
	// the operands) and the operands. keep false means the key is absent.
	Full(base []byte, operands [][]byte) (value []byte, keep bool, err error)
	// Partial collapses operands into one operand with the same effect,
	// for compactions that cannot see the base yet.
	Partial(operands [][]byte) ([]byte, error)
}
