package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
)

// ErrCorrupt identifies invalid durable bytes: a table, block or version
// that fails its checksum or its framing. Every such error wraps it.
var ErrCorrupt = errors.New("corrupt")

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

// Merger folds a key's merge operands, oldest first.
type Merger interface {
	// Full computes the key's value from base (nil when no Put lies beneath
	// the operands) and the operands. keep false means the key is absent.
	Full(base []byte, operands [][]byte) (value []byte, keep bool)
	// Partial collapses operands into one operand with the same effect,
	// for compactions that cannot see the base yet.
	Partial(operands [][]byte) []byte
}

// SetMerger is the Merger for keys whose value is a set of uint32s
// (roaring bitmap bytes): a document's locals in a cluster, a value's
// clusters. An operand is (add, remove); applying it removes then adds.
type SetMerger struct{}

// smallSetMax is the cardinality up to which a set is stored as a
// delta-varint list rather than a roaring bitmap: most small posting lists
// are a handful of rows, and a roaring container costs more to
// decode than the list. The list starts with a zero byte, which no roaring
// cookie does.
const smallSetMax = 16

// SetValue encodes a bitmap as a Put value.
func SetValue(set *roaring.Bitmap) []byte {
	if n := set.GetCardinality(); n <= smallSetMax {
		out := make([]byte, 0, 2+5*int(n))
		out = append(out, 0)
		out = binary.AppendUvarint(out, n)
		prev := uint32(0)
		set.Iterate(func(x uint32) bool {
			out = binary.AppendUvarint(out, uint64(x-prev))
			prev = x
			return true
		})
		return out
	}
	b, err := set.ToBytes()
	if err != nil {
		panic(err) // ToBytes only fails on a nil writer
	}
	return b
}

// SetOperand encodes one (add, remove) merge operand.
func SetOperand(add, remove *roaring.Bitmap) []byte {
	a := SetValue(orEmpty(add))
	r := SetValue(orEmpty(remove))
	out := binary.AppendUvarint(make([]byte, 0, 10+len(a)+len(r)), uint64(len(a)))
	out = append(out, a...)
	return append(out, r...)
}

func orEmpty(b *roaring.Bitmap) *roaring.Bitmap {
	if b == nil {
		return roaring.New()
	}
	return b
}

// DecodeSet decodes a Put value.
func DecodeSet(value []byte) (*roaring.Bitmap, error) {
	set := roaring.New()
	return set, DecodeSetInto(set, value)
}

// DecodeSetInto adds a Put value's members to dst, without allocating a
// bitmap for a small list.
func DecodeSetInto(dst *roaring.Bitmap, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	if value[0] == 0 {
		b := value[1:]
		n, k := binary.Uvarint(b)
		if k <= 0 || n > smallSetMax {
			return fmt.Errorf("%w: lsm: small set header", ErrCorrupt)
		}
		b = b[k:]
		var buf [smallSetMax]uint32
		prev := uint32(0)
		for i := range n {
			d, k := binary.Uvarint(b)
			if k <= 0 || d > math.MaxUint32 {
				return fmt.Errorf("%w: lsm: small set member", ErrCorrupt)
			}
			b = b[k:]
			prev += uint32(d)
			buf[i] = prev
		}
		if len(b) != 0 {
			return fmt.Errorf("%w: lsm: small set trailing bytes", ErrCorrupt)
		}
		dst.AddMany(buf[:n])
		return nil
	}
	if dst.IsEmpty() {
		if err := dst.UnmarshalBinary(value); err != nil {
			return fmt.Errorf("%w: lsm: set value: %v", ErrCorrupt, err)
		}
		return nil
	}
	tmp := roaring.New()
	if err := tmp.UnmarshalBinary(value); err != nil {
		return fmt.Errorf("%w: lsm: set value: %v", ErrCorrupt, err)
	}
	dst.Or(tmp)
	return nil
}

// DecodeSetOperand decodes one operand.
func DecodeSetOperand(op []byte) (add, remove *roaring.Bitmap, err error) {
	n, k := binary.Uvarint(op)
	if k <= 0 || n > uint64(len(op)-k) {
		return nil, nil, fmt.Errorf("%w: lsm: set operand header", ErrCorrupt)
	}
	if add, err = DecodeSet(op[k : k+int(n)]); err != nil {
		return nil, nil, err
	}
	if remove, err = DecodeSet(op[k+int(n):]); err != nil {
		return nil, nil, err
	}
	return add, remove, nil
}

// Full applies the operands to base. A malformed operand is skipped rather
// than failing the read: the file's checksums make that a logic error, not
// corruption, and a merge has no error path. The set is absent when empty.
func (SetMerger) Full(base []byte, operands [][]byte) ([]byte, bool) {
	set := roaring.New()
	if base != nil {
		if s, err := DecodeSet(base); err == nil {
			set = s
		}
	}
	for _, op := range operands {
		add, remove, err := DecodeSetOperand(op)
		if err != nil {
			continue
		}
		set.AndNot(remove)
		set.Or(add)
	}
	if set.IsEmpty() {
		return nil, false
	}
	return SetValue(set), true
}

// Partial composes operands into one: applying (A1,R1) then (A2,R2) is
// removing R1 ∪ R2 and adding (A1 \ R2) ∪ A2.
func (SetMerger) Partial(operands [][]byte) []byte {
	add, remove := roaring.New(), roaring.New()
	for _, op := range operands {
		a, r, err := DecodeSetOperand(op)
		if err != nil {
			continue
		}
		add.AndNot(r)
		add.Or(a)
		remove.Or(r)
		remove.AndNot(a)
	}
	return SetOperand(add, remove)
}
