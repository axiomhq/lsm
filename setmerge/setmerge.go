// Package setmerge is an lsm.Merger for values that are sets of uint32
// (roaring bitmaps): an operand is (add, remove); applying it removes then
// adds. Small sets are a delta-varint list.
package setmerge

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
)

// ErrCorrupt identifies a value or operand that does not decode. The
// package does not import lsm, so it has its own sentinel.
var ErrCorrupt = errors.New("setmerge: corrupt")

// Merger is the lsm.Merger for set values.
type Merger struct{}

// smallSetMax is the cardinality up to which a set is stored as a
// delta-varint list rather than a roaring bitmap: most small sets are a
// handful of members, and a roaring container costs more to decode than
// the list. The list starts with a zero byte, which no roaring cookie does.
const smallSetMax = 16

// Value encodes a bitmap as a Put value.
func Value(set *roaring.Bitmap) []byte {
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

// Operand encodes one (add, remove) merge operand.
func Operand(add, remove *roaring.Bitmap) []byte {
	a := Value(orEmpty(add))
	r := Value(orEmpty(remove))
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

// Decode decodes a Put value.
func Decode(value []byte) (*roaring.Bitmap, error) {
	set := roaring.New()
	return set, DecodeInto(set, value)
}

// DecodeInto adds a Put value's members to dst, without allocating a
// bitmap for a small list.
func DecodeInto(dst *roaring.Bitmap, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	if value[0] == 0 {
		b := value[1:]
		n, k := binary.Uvarint(b)
		if k <= 0 || n > smallSetMax {
			return fmt.Errorf("%w: small set header", ErrCorrupt)
		}
		b = b[k:]
		var buf [smallSetMax]uint32
		prev := uint32(0)
		for i := range n {
			d, k := binary.Uvarint(b)
			if k <= 0 || d > math.MaxUint32 {
				return fmt.Errorf("%w: small set member", ErrCorrupt)
			}
			b = b[k:]
			prev += uint32(d)
			buf[i] = prev
		}
		if len(b) != 0 {
			return fmt.Errorf("%w: small set trailing bytes", ErrCorrupt)
		}
		dst.AddMany(buf[:n])
		return nil
	}
	if dst.IsEmpty() {
		if err := dst.UnmarshalBinary(value); err != nil {
			return fmt.Errorf("%w: set value: %v", ErrCorrupt, err)
		}
		return nil
	}
	tmp := roaring.New()
	if err := tmp.UnmarshalBinary(value); err != nil {
		return fmt.Errorf("%w: set value: %v", ErrCorrupt, err)
	}
	dst.Or(tmp)
	return nil
}

// DecodeOperand decodes one operand.
func DecodeOperand(op []byte) (add, remove *roaring.Bitmap, err error) {
	n, k := binary.Uvarint(op)
	if k <= 0 || n > uint64(len(op)-k) {
		return nil, nil, fmt.Errorf("%w: set operand header", ErrCorrupt)
	}
	if add, err = Decode(op[k : k+int(n)]); err != nil {
		return nil, nil, err
	}
	if remove, err = Decode(op[k+int(n):]); err != nil {
		return nil, nil, err
	}
	return add, remove, nil
}

// Full applies the operands to base. The set is absent when empty.
func (Merger) Full(base []byte, operands [][]byte) (value []byte, keep bool, err error) {
	defer recoverCorrupt(&err)
	set := roaring.New()
	if base != nil {
		if err := DecodeInto(set, base); err != nil {
			return nil, false, err
		}
	}
	for _, op := range operands {
		add, remove, err := DecodeOperand(op)
		if err != nil {
			return nil, false, err
		}
		set.AndNot(remove)
		set.Or(add)
	}
	if set.IsEmpty() {
		return nil, false, nil
	}
	return Value(set), true, nil
}

// Partial composes operands into one: applying (A1,R1) then (A2,R2) is
// removing R1 ∪ R2 and adding (A1 \ R2) ∪ A2.
func (Merger) Partial(operands [][]byte) (op []byte, err error) {
	defer recoverCorrupt(&err)
	add, remove := roaring.New(), roaring.New()
	for _, op := range operands {
		a, r, err := DecodeOperand(op)
		if err != nil {
			return nil, err
		}
		add.AndNot(r)
		add.Or(a)
		remove.Or(r)
		remove.AndNot(a)
	}
	return Operand(add, remove), nil
}

// recoverCorrupt turns a panic in roaring into ErrCorrupt: a bitmap that
// unmarshals cleanly can still hold a run past 65535, which the set
// operations reject by panicking and Validate would only catch in time
// quadratic in the runs.
func recoverCorrupt(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("%w: set operation: %v", ErrCorrupt, r)
	}
}
