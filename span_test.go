package lsm

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/axiomhq/lsm/setmerge"
)

func TestSpanNamesTheBlocksAKeyRangeTouches(t *testing.T) {
	var entries []Entry
	for i := range 200 {
		entries = append(entries, Entry{Key: fmt.Appendf(nil, "k%03d", i), Kind: KindPut, Value: bytes.Repeat([]byte{byte(i)}, 40)})
	}
	data, meta, err := BuildTable(entries, 512)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := OpenTableAt(context.Background(), BytesSource(data), meta)
	if err != nil {
		t.Fatal(err)
	}
	n := len(tb.index)
	if n < 4 {
		t.Fatalf("want several blocks, got %d", n)
	}
	if first, last, ok := tb.Span([]byte("k000"), nil); !ok || first != 0 || last != n-1 {
		t.Fatalf("whole table: %d..%d %v, want 0..%d", first, last, ok, n-1)
	}
	if _, _, ok := tb.Span([]byte("z"), nil); ok {
		t.Fatal("a range past the last key spans a block")
	}
	if _, _, ok := tb.Span([]byte("a"), []byte("b")); ok {
		t.Fatal("a range before the first key spans a block")
	}
	for i := 0; i < 200; i += 37 {
		key := fmt.Appendf(nil, "k%03d", i)
		first, last, ok := tb.Span(key, fmt.Appendf(nil, "k%03d", i+1))
		if !ok || first != last {
			t.Fatalf("%q: %d..%d %v, want one block", key, first, last, ok)
		}
		if b := tb.index[first]; bytes.Compare(b.first, key) > 0 || bytes.Compare(key, b.last) > 0 {
			t.Fatalf("%q outside block %d [%q, %q]", key, first, b.first, b.last)
		}
	}
	if first, last, ok := tb.Span([]byte("k050"), []byte("k150")); !ok || first >= last || first == 0 || last == n-1 {
		t.Fatalf("middle range: %d..%d %v of %d blocks", first, last, ok, n)
	}
}

// TestBlockExtentServesTheSpan: the blocks' extents tile the table from 0
// to the index, and a reader whose source serves only the extents of the
// blocks Span names for a range reads every key in it.
func TestBlockExtentServesTheSpan(t *testing.T) {
	var entries []Entry
	for i := range 200 {
		entries = append(entries, Entry{Key: fmt.Appendf(nil, "k%03d", i), Kind: KindPut, Value: bytes.Repeat([]byte{byte(i)}, 40)})
	}
	data, meta, err := BuildTable(entries, 512)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tb, err := OpenTableAt(ctx, BytesSource(data), meta)
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	for i := range tb.index {
		off, length := tb.BlockExtent(i)
		if off != end || length <= 0 {
			t.Fatalf("block %d at %d+%d, want it to start at %d", i, off, length, end)
		}
		end = off + length
	}
	if end != meta.IndexOff {
		t.Fatalf("blocks end at %d, the index starts at %d", end, meta.IndexOff)
	}
	lo, hi := []byte("k050"), []byte("k090")
	first, last, ok := tb.Span(lo, hi)
	if !ok {
		t.Fatal("no span")
	}
	served := extentSource{data: data, ok: map[[2]int64]bool{{meta.IndexOff, meta.IndexLen + 32}: true}}
	for i := first; i <= last; i++ {
		off, length := tb.BlockExtent(i)
		served.ok[[2]int64{off, length}] = true
	}
	only, err := OpenTableAt(ctx, served, meta)
	if err != nil {
		t.Fatal(err)
	}
	for i := 50; i < 90; i++ {
		key := fmt.Appendf(nil, "k%03d", i)
		if e, found, err := only.Get(ctx, key); err != nil || !found || !bytes.Equal(e.Value, entries[i].Value) {
			t.Fatalf("%q from the span's extents: %v %v", key, found, err)
		}
	}
	if _, _, err := only.Get(ctx, []byte("k150")); err == nil {
		t.Fatal("a key outside the span was read from extents the source does not serve")
	}
}

// extentSource serves only the extents in ok.
type extentSource struct {
	data []byte
	ok   map[[2]int64]bool
}

func (s extentSource) ReadAt(_ context.Context, off, length int64) ([]byte, error) {
	if !s.ok[[2]int64{off, length}] {
		return nil, fmt.Errorf("extent %d+%d not served", off, length)
	}
	return s.data[off : off+length], nil
}

func TestLocateNamesTheFile(t *testing.T) {
	ctx := context.Background()
	m := newMemStore()
	v := Version{}
	val := make([]byte, 2*LargeValueBytes) // incompressible: a stored block
	rand.NewChaCha8([32]byte{7}).Read(val)
	big := Entry{Key: []byte("Fbig"), Kind: KindPut, Value: val}
	small := Entry{Key: []byte("Gsmall"), Kind: KindPut, Value: []byte("v")}
	next, _, err := Flush(ctx, v, []Entry{big, small}, DefaultOptions(), m.put)
	if err != nil {
		t.Fatal(err)
	}
	r := Reader{Open: m.open, Merger: setmerge.Merger{}}
	want := next.Levels[0][0].Key
	l, ok, err := r.Locate(ctx, next, big.Key)
	if err != nil || !ok || l.Table == nil || l.File.Key != want {
		t.Fatalf("located %+v ok=%v err=%v, want file %q with a table", l, ok, err, want)
	}
	l, ok, err = r.Locate(ctx, next, small.Key)
	if err != nil || !ok || l.Value == nil || l.File.Key != "" {
		t.Fatalf("shared block: %+v ok=%v err=%v, want a value and no file", l, ok, err)
	}
}
