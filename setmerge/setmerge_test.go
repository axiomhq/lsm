package setmerge_test

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/axiomhq/lsm/setmerge"
)

// TestRoundTrip: sets on both sides of the small-list threshold decode to
// themselves, as values and as either half of an operand.
func TestRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 15, 16, 17, 1000} {
		set := roaring.New()
		for i := range n {
			set.Add(uint32(i*7919) % math.MaxUint32)
		}
		if n > 0 {
			set.Add(math.MaxUint32)
		}
		v := setmerge.Value(set)
		if small := len(v) > 0 && v[0] == 0; small != (set.GetCardinality() <= 16) {
			t.Fatalf("%d members: small list %v", set.GetCardinality(), small)
		}
		got, err := setmerge.Decode(v)
		if err != nil || !got.Equals(set) {
			t.Fatalf("%d members: decoded %d (%v)", set.GetCardinality(), got.GetCardinality(), err)
		}
		other := roaring.BitmapOf(3, 5)
		for _, pair := range [][2]*roaring.Bitmap{{set, other}, {other, set}, {set, nil}, {nil, set}} {
			add, remove, err := setmerge.DecodeOperand(setmerge.Operand(pair[0], pair[1]))
			if err != nil {
				t.Fatal(err)
			}
			for i, got := range []*roaring.Bitmap{add, remove} {
				want := pair[i]
				if want == nil {
					want = roaring.New()
				}
				if !got.Equals(want) {
					t.Fatalf("%d members: operand half %d decoded %d", set.GetCardinality(), i, got.GetCardinality())
				}
			}
		}
	}
}

// FuzzDecodeOperand: any bytes decode or fail with ErrCorrupt, and never
// panic, as operands and as values.
func FuzzDecodeOperand(f *testing.F) {
	f.Add(setmerge.Operand(roaring.BitmapOf(1, 2, 3), roaring.BitmapOf(4)))
	big := roaring.New()
	big.AddRange(0, 100)
	f.Add(setmerge.Operand(big, roaring.BitmapOf(7)))
	f.Fuzz(func(t *testing.T, op []byte) {
		check := func(err error) {
			if err != nil && !errors.Is(err, setmerge.ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
		}
		_, _, err := setmerge.DecodeOperand(op)
		check(err)
		_, err = setmerge.Decode(op)
		check(err)
		// The merger must survive whatever decodes: roaring panics on some
		// bitmaps that unmarshal, and the merger turns that into ErrCorrupt.
		_, _, err = (setmerge.Merger{}).Full(op, [][]byte{op, op})
		check(err)
		_, err = (setmerge.Merger{}).Partial([][]byte{op, op})
		check(err)
	})
}

// TestOverflowingRunIsCorrupt: a bitmap that unmarshals cleanly but holds a
// run past 65535 panics inside roaring's set operations; the merger reports
// it as ErrCorrupt instead of crashing the read.
func TestOverflowingRunIsCorrupt(t *testing.T) {
	op := []byte("\x0f;0\x00\x0010000\x01\x000000;0\x00\x0010000\x01\x000\xcd07")
	if _, err := (setmerge.Merger{}).Partial([][]byte{op}); !errors.Is(err, setmerge.ErrCorrupt) {
		t.Fatalf("Partial: %v", err)
	}
	// Full happens to survive this input on an empty base; whatever roaring
	// does, it must come back as a result or ErrCorrupt, never a panic.
	if _, _, err := (setmerge.Merger{}).Full(nil, [][]byte{op}); err != nil && !errors.Is(err, setmerge.ErrCorrupt) {
		t.Fatalf("Full: %v", err)
	}
}

// TestUnsortedKeysAreCorrupt: a bitmap whose container keys are out of
// order unmarshals without error in roaring and then answers membership
// wrongly; Decode validates and refuses it.
func TestUnsortedKeysAreCorrupt(t *testing.T) {
	b, err := roaring.BitmapOf(1, 70000).ToBytes() // two array containers, keys 0 and 1
	if err != nil {
		t.Fatal(err)
	}
	// cookie u32, size u32, then (key u16, card-1 u16) per container.
	k0, k1 := b[8:12], b[12:16]
	swapped := slices.Concat(b[:8], k1, k0, b[16:])
	if _, err := setmerge.Decode(swapped); !errors.Is(err, setmerge.ErrCorrupt) {
		t.Fatalf("unsorted keys: %v", err)
	}
}
