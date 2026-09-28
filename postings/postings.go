package postings

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/axiomhq/lsm"
)

// version is the first byte of every encoded list, so Decode refuses a
// layout it does not know.
const version byte = 1

func corrupt(what string) error {
	return fmt.Errorf("%w: postings: %s", lsm.ErrCorrupt, what)
}

// Posting is one (docnum, weight) pair.
type Posting struct {
	Docnum uint64
	Weight uint32
}

// Encode writes ps as delta varints. ps must be sorted by docnum; the order
// is not checked, and an unsorted list still round-trips (the deltas wrap)
// but costs up to 10 bytes per backwards step.
func Encode(ps []Posting) []byte {
	out := make([]byte, 0, 1+binary.MaxVarintLen32+len(ps)*6)
	out = append(out, version)
	out = binary.AppendUvarint(out, uint64(len(ps)))
	prev := uint64(0)
	for _, p := range ps {
		out = binary.AppendUvarint(out, p.Docnum-prev)
		out = binary.AppendUvarint(out, uint64(p.Weight))
		prev = p.Docnum
	}
	return out
}

// Decode appends the list in b to dst. Invalid bytes return an error
// wrapping lsm.ErrCorrupt.
func Decode(dst []Posting, b []byte) ([]Posting, error) {
	if len(b) < 2 || b[0] != version {
		return nil, corrupt("header")
	}
	b = b[1:]
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)) {
		return nil, corrupt("count")
	}
	b = b[k:]
	prev := uint64(0)
	for range n {
		d, k := binary.Uvarint(b)
		if k <= 0 {
			return nil, corrupt("docnum")
		}
		b = b[k:]
		w, j := binary.Uvarint(b)
		if j <= 0 || w > math.MaxUint32 {
			return nil, corrupt("weight")
		}
		b = b[j:]
		prev += d
		dst = append(dst, Posting{Docnum: prev, Weight: uint32(w)})
	}
	if len(b) != 0 {
		return nil, corrupt("trailing bytes")
	}
	return dst, nil
}
