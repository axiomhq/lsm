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

// TestGroupedSpaceBlocks: with SpacePolicy.Group, no block holds keys of
// two groups of the space, or of it and another space, in a flushed table
// and in a compacted one; a group's Span is exactly its own blocks, and
// every key still reads back.
func TestGroupedSpaceBlocks(t *testing.T) {
	ctx := context.Background()
	group := func(key []byte) int { return 3 } // H + 2-byte group id
	o := DefaultOptions()
	o.BlockBytes = 512
	o.L0Trigger = 2
	o.Spaces = map[byte]SpacePolicy{'H': {WriteOnce: true, Group: group}}
	rng := rand.New(rand.NewPCG(4, 4))
	var entries []Entry
	for g := range 40 {
		for i := range 1 + rng.IntN(20) { // 1 to 20 small entries a group: many groups fit a block
			entries = append(entries, Entry{Key: fmt.Appendf(nil, "H%02d%03d", g, i), Kind: KindPut, Value: bytes.Repeat([]byte{byte(g)}, 30)})
		}
	}
	for i := range 50 {
		entries = append(entries, Entry{Key: fmt.Appendf(nil, "I%04d", i), Kind: KindPut, Value: []byte("v")})
	}
	check := func(name string, data []byte, meta TableMeta) {
		t.Helper()
		tb, err := OpenTableAt(ctx, BytesSource(data), meta)
		if err != nil {
			t.Fatal(err)
		}
		for i, b := range tb.index {
			if b.first[0] == 'H' || b.last[0] == 'H' {
				if b.first[0] != b.last[0] || !bytes.Equal(b.first[:3], b.last[:3]) {
					t.Fatalf("%s: block %d holds %q..%q", name, i, b.first, b.last)
				}
			}
		}
		for g := range 40 {
			lo := fmt.Appendf(nil, "H%02d", g)
			first, last, ok := tb.Span(lo, fmt.Appendf(nil, "H%02d", g+1))
			if !ok {
				t.Fatalf("%s: group %d spans nothing", name, g)
			}
			for i := first; i <= last; i++ {
				if !bytes.HasPrefix(tb.index[i].first, lo) {
					t.Fatalf("%s: group %d's span holds block %d of %q", name, g, i, tb.index[i].first)
				}
			}
		}
		for _, e := range entries {
			if got, ok, err := tb.Get(ctx, e.Key); err != nil || !ok || !bytes.Equal(got.Value, e.Value) {
				t.Fatalf("%s: %q: %v %v", name, e.Key, ok, err)
			}
		}
	}
	data, meta, err := BuildTableOptions(entries, o)
	if err != nil {
		t.Fatal(err)
	}
	check("built", data, meta)
	if plain, _, err := BuildTable(entries, o.BlockBytes); err != nil || len(plain) >= len(data) {
		// The grouped table has more, smaller blocks: a bigger index.
		t.Fatalf("an ungrouped table is %d bytes, the grouped one %d (%v)", len(plain), len(data), err)
	}

	m := newMemStore()
	var v Version
	half := len(entries) / 2
	for _, part := range [][]Entry{entries[:half], entries[half:]} {
		if v, _, err = Flush(ctx, v, part, o, m.put); err != nil {
			t.Fatal(err)
		}
	}
	r := Reader{Open: m.open}
	jobs := 0
	for job, ok := Pick(v, o); ok; job, ok = Pick(v, o) {
		jobs++
		if v, _, err = Compact(ctx, v, job, o, r, m.put); err != nil {
			t.Fatal(err)
		}
	}
	if jobs == 0 {
		t.Fatal("no compaction ran")
	}
	for l, files := range v.Levels {
		for _, f := range files {
			tb, err := m.open(ctx, f)
			if err != nil {
				t.Fatal(err)
			}
			for i, b := range tb.index {
				if (b.first[0] == 'H' || b.last[0] == 'H') && (b.first[0] != b.last[0] || !bytes.Equal(b.first[:3], b.last[:3])) {
					t.Fatalf("level %d %s: block %d holds %q..%q", l, f.Key, i, b.first, b.last)
				}
			}
		}
	}
}

// TestBoundedIterReadsNoBlockPastTheRange: Reader.Iter over one group's
// range reads that group's blocks and no other (it used to load the next
// block to learn the range had ended), and still yields every key in it.
func TestBoundedIterReadsNoBlockPastTheRange(t *testing.T) {
	ctx := context.Background()
	o := DefaultOptions()
	o.BlockBytes = 512
	o.Spaces = map[byte]SpacePolicy{'H': {Group: func([]byte) int { return 3 }}}
	var entries []Entry
	for g := range 20 {
		for i := range 30 {
			entries = append(entries, Entry{Key: fmt.Appendf(nil, "H%02d%03d", g, i), Kind: KindPut, Value: bytes.Repeat([]byte{byte(g)}, 30)})
		}
	}
	data, meta, err := BuildTableOptions(entries, o)
	if err != nil {
		t.Fatal(err)
	}
	reads := map[int64]int{}
	src := countingExtents{data: data, reads: reads}
	f := FileRef{Key: "t", TableMeta: meta}
	v := Version{Levels: [][]FileRef{nil, {f}}}
	r := Reader{Open: func(ctx context.Context, _ FileRef) (*Table, error) { return OpenTableAt(ctx, src, meta) }}
	for _, g := range []int{0, 7, 19} {
		clear(reads)
		lo, hi := fmt.Appendf(nil, "H%02d", g), fmt.Appendf(nil, "H%02d", g+1)
		it, err := r.Iter(ctx, v, lo, hi)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for ok := it.SeekGE(lo); ok; ok = it.Next() {
			n++
		}
		if err := it.Err(); err != nil || n != 30 {
			t.Fatalf("group %d: %d keys (%v)", g, n, err)
		}
		tb, _ := OpenTableAt(ctx, BytesSource(data), meta)
		first, last, _ := tb.Span(lo, hi)
		want := map[int64]bool{}
		for i := first; i <= last; i++ {
			off, _ := tb.BlockExtent(i)
			want[off] = true
		}
		for off := range reads {
			if off != meta.IndexOff && !want[off] {
				t.Fatalf("group %d: read a block at %d outside its span %d..%d", g, off, first, last)
			}
		}
	}
}

// countingExtents records the offset of every read.
type countingExtents struct {
	data  []byte
	reads map[int64]int
}

func (s countingExtents) ReadAt(_ context.Context, off, length int64) ([]byte, error) {
	s.reads[off]++
	return s.data[off : off+length], nil
}

// TestRawSpaceKeepsLargeValuesExtentReadable: a large value zstd halves
// is compressed (no extent) unless its space is Raw, where it is stored
// and Table.Single names its bytes.
func TestRawSpaceKeepsLargeValuesExtentReadable(t *testing.T) {
	ctx := context.Background()
	val := bytes.Repeat([]byte("0123"), LargeValueBytes) // compresses to almost nothing
	o := DefaultOptions()
	o.Spaces = map[byte]SpacePolicy{'F': {Raw: true}}
	data, meta, err := BuildTableOptions([]Entry{{Key: []byte("D1"), Kind: KindPut, Value: val}, {Key: []byte("F1"), Kind: KindPut, Value: val}}, o)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := OpenTableAt(ctx, BytesSource(data), meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := tb.Single([]byte("D1")); ok {
		t.Fatal("a compressible large value outside a Raw space was stored")
	}
	off, length, ok := tb.Single([]byte("F1"))
	if !ok || length != int64(len(val)) || !bytes.Equal(data[off:off+length], val) {
		t.Fatalf("Raw space: Single %d+%d %v", off, length, ok)
	}
}
