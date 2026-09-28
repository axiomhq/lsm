package postings

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/axiomhq/lsm"
)

func TestRoundTrip(t *testing.T) {
	ps := []Posting{{1, 2}, {5, 1}, {1000, 7}, {math.MaxUint64, math.MaxUint32}}
	prefix := []Posting{{9, 9}}
	dec, err := Decode(slices.Clone(prefix), Encode(ps))
	if err != nil || !slices.Equal(dec, append(prefix, ps...)) {
		t.Fatalf("postings %v %v", dec, err)
	}
}

func TestEmpty(t *testing.T) {
	b := Encode(nil)
	if !bytes.Equal(b, []byte{version, 0}) {
		t.Fatalf("empty encodes as %x", b)
	}
	dec, err := Decode(nil, b)
	if err != nil || len(dec) != 0 {
		t.Fatalf("%v %v", dec, err)
	}
}

// The layout is durable: this is the byte form existing data was written in.
func TestLayout(t *testing.T) {
	want := []byte{1, 2, 3, 4, 0x80, 0x01, 5}
	if got := Encode([]Posting{{3, 4}, {131, 5}}); !bytes.Equal(got, want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}

func TestUnsortedRoundTrips(t *testing.T) {
	ps := []Posting{{10, 1}, {3, 2}}
	dec, err := Decode(nil, Encode(ps))
	if err != nil || !slices.Equal(dec, ps) {
		t.Fatalf("%v %v", dec, err)
	}
}

func TestRefusesCorrupt(t *testing.T) {
	good := Encode([]Posting{{1, 2}, {5, 1}, {1000, 7}})
	lying := slices.Clone(good)
	lying[1] = 4 // one more posting than the body holds
	huge := []byte{version, 0xFF, 0x01}
	cases := map[string][]byte{
		"nil":      nil,
		"version":  append([]byte{2}, good[1:]...),
		"lying":    lying,
		"huge":     huge,
		"trailing": append(slices.Clone(good), 0),
		"weight":   append([]byte{version, 1, 0}, binary.AppendUvarint(nil, math.MaxUint32+1)...),
	}
	for i := 2; i < len(good); i++ {
		cases["truncated"+string(rune('0'+i))] = good[:i]
	}
	for name, b := range cases {
		if _, err := Decode(nil, b); !errors.Is(err, lsm.ErrCorrupt) {
			t.Errorf("%s: %x: err %v", name, b, err)
		}
	}
}
