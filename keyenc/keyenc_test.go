package keyenc

import (
	"bytes"
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestStringRoundTripAndOrder(t *testing.T) {
	cases := []string{"", "a", "a\x00", "a\x00b", "a\x00\x00", "\x00", "\xff", "ab", "a\x01"}
	slices.Sort(cases)
	var prev []byte
	for _, s := range cases {
		enc := AppendString(nil, s)
		got, rest, ok := String(append(enc, 'X'))
		if !ok || got != s || string(rest) != "X" {
			t.Fatalf("%q: round trip %q %q %v", s, got, rest, ok)
		}
		if prev != nil && bytes.Compare(prev, enc) >= 0 {
			t.Fatalf("%q encodes out of order", s)
		}
		prev = enc
	}
}

func TestStringRejects(t *testing.T) {
	for _, b := range [][]byte{nil, []byte("ab"), []byte("ab\x00"), []byte("a\x00\x01b\x00\x00"), []byte("a\x00\xff")} {
		if _, _, ok := String(b); ok {
			t.Fatalf("%q accepted", b)
		}
	}
}

func sign(n int) int { return cmp.Compare(n, 0) }

func TestStringOrderProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	str := func() string {
		b := make([]byte, rng.IntN(6))
		for i := range b {
			b[i] = []byte{0, 1, 'a', 0xFE, 0xFF}[rng.IntN(5)]
		}
		return string(b)
	}
	for range 500 {
		a, b := str(), str()
		ea, eb := AppendString(nil, a), AppendString(nil, b)
		if sign(bytes.Compare(ea, eb)) != cmp.Compare(a, b) {
			t.Fatalf("%q vs %q: encodings %x %x", a, b, ea, eb)
		}
		// A following component must not change the order.
		if sign(bytes.Compare(append(ea, 0xFF), append(eb, 0))) != cmp.Compare(a, b) && a != b {
			t.Fatalf("%q vs %q: suffix changed order", a, b)
		}
	}
}

func TestFloat64RoundTripAndOrder(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	pool := []float64{0, math.Copysign(0, -1), 1, -1, math.MaxFloat64, -math.MaxFloat64,
		math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1), 1e300, -1e-300}
	draw := func() float64 {
		if rng.IntN(3) == 0 {
			return pool[rng.IntN(len(pool))]
		}
		f := math.Ldexp(rng.Float64(), rng.IntN(2000)-1000)
		if rng.IntN(2) == 0 {
			f = -f
		}
		return f
	}
	for range 500 {
		a, b := draw(), draw()
		ea, eb := AppendFloat64(nil, a), AppendFloat64(nil, b)
		got, rest, ok := Float64(append(ea, 'X'))
		if !ok || got != a || math.Signbit(got) != math.Signbit(a) && a != 0 || string(rest) != "X" {
			t.Fatalf("%v: round trip %v %q %v", a, got, rest, ok)
		}
		if sign(bytes.Compare(ea, eb)) != cmp.Compare(a, b) {
			t.Fatalf("%v vs %v: encodings %x %x", a, b, ea, eb)
		}
	}
	negZero, _, _ := Float64(AppendFloat64(nil, math.Copysign(0, -1)))
	if negZero != 0 || math.Signbit(negZero) {
		t.Fatalf("-0 reads back as %v, want +0", negZero)
	}
	if _, _, ok := Float64(make([]byte, 7)); ok {
		t.Fatal("7 bytes accepted")
	}
}

func TestPrefixEnd(t *testing.T) {
	for _, c := range []struct{ in, want []byte }{
		{[]byte{0x00}, []byte{0x01}},
		{[]byte{0xFF}, nil},
		{[]byte{0x01, 0xFF}, []byte{0x02}},
		{nil, nil},
	} {
		if got := PrefixEnd(c.in); !bytes.Equal(got, c.want) || (c.want == nil) != (got == nil) {
			t.Fatalf("PrefixEnd(%x) = %x, want %x", c.in, got, c.want)
		}
	}
	p := []byte{0x01, 0xFF}
	PrefixEnd(p)
	if p[1] != 0xFF {
		t.Fatal("PrefixEnd modified its input")
	}
}
