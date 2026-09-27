package lsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"testing"
)

// randKeys returns n sorted, unique keys in space, 8-20 bytes.
func randKeys(rng *rand.Rand, n int, space byte) [][]byte {
	seen := make(map[string]bool, n)
	keys := make([][]byte, 0, n)
	for len(keys) < n {
		k := make([]byte, 8+rng.IntN(13))
		k[0] = space
		for i := 1; i < len(k); i++ {
			k[i] = byte(rng.IntN(256))
		}
		if !seen[string(k)] {
			seen[string(k)] = true
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys
}

func putEntries(keys [][]byte) []Entry {
	out := make([]Entry, len(keys))
	for i, k := range keys {
		out[i] = Entry{Key: k, Kind: KindPut, Value: binary.BigEndian.AppendUint32(nil, uint32(i))}
	}
	return out
}

func collect(t *testing.T, it Iterator) []Entry {
	t.Helper()
	var out []Entry
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		out = append(out, Entry{Key: bytes.Clone(it.Key()), Kind: it.Kind(), Value: bytes.Clone(it.Value())})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return out
}

func sameEntries(a, b []Entry) bool {
	return slices.EqualFunc(a, b, func(x, y Entry) bool {
		return bytes.Equal(x.Key, y.Key) && x.Kind == y.Kind && bytes.Equal(x.Value, y.Value)
	})
}

func TestTableRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	keys := randKeys(rng, 5000, 'A')
	entries := putEntries(keys)
	entries[7].Kind, entries[7].Value = KindDelete, nil
	entries[9].Kind = KindMerge
	data, meta, err := BuildTable(entries, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Count != 5000 || !bytes.Equal(meta.Min, keys[0]) || !bytes.Equal(meta.Max, keys[4999]) || meta.Bytes != int64(len(data)) {
		t.Fatalf("meta %+v", meta)
	}
	ctx := context.Background()
	for name, open := range map[string]func() (*Table, error){
		"meta": func() (*Table, error) { return OpenTableAt(ctx, BytesSource(data), meta) },
	} {
		tb, err := open()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(tb.index) < 50 {
			t.Fatalf("%s: %d blocks for 5000 entries at 1 KiB", name, len(tb.index))
		}
		if got := collect(t, tb.Iter(ctx)); !sameEntries(got, entries) {
			t.Fatalf("%s: iteration differs (%d vs %d)", name, len(got), len(entries))
		}
		for i, k := range keys {
			e, ok, err := tb.Get(ctx, k)
			if err != nil || !ok || !sameEntries([]Entry{e}, entries[i:i+1]) {
				t.Fatalf("%s: get %d: %v %v %+v", name, i, ok, err, e)
			}
			if i > 0 {
				// A key strictly between neighbours is absent and seeks to keys[i].
				between := append(bytes.Clone(keys[i-1]), 0)
				if bytes.Compare(between, k) >= 0 {
					continue
				}
				if _, ok, err := tb.Get(ctx, between); ok || err != nil {
					t.Fatalf("%s: get between %d: %v %v", name, i, ok, err)
				}
				it := tb.Iter(ctx)
				if !it.SeekGE(between) || !bytes.Equal(it.Key(), k) {
					t.Fatalf("%s: seek between %d landed on %x, want %x", name, i, it.Key(), k)
				}
			}
		}
		it := tb.Iter(ctx)
		if it.SeekGE(append(bytes.Clone(keys[4999]), 0)) {
			t.Fatalf("%s: seek past the end found %x", name, it.Key())
		}
	}
}

func TestTableWriterRejectsBadInput(t *testing.T) {
	w := NewTableWriter(0)
	if err := w.Add(Entry{Key: []byte("b"), Kind: KindPut}); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(Entry{Key: []byte("b"), Kind: KindPut}); err == nil {
		t.Fatal("duplicate key accepted")
	}
	if err := w.Add(Entry{Key: []byte("a"), Kind: KindPut}); err == nil {
		t.Fatal("unsorted key accepted")
	}
	if err := w.Add(Entry{Key: []byte("c"), Kind: 9}); err == nil {
		t.Fatal("bad kind accepted")
	}
	if err := w.Add(Entry{Kind: KindPut}); err == nil {
		t.Fatal("empty key accepted")
	}
	if _, _, err := NewTableWriter(0).Finish(); err == nil {
		t.Fatal("empty table accepted")
	}
}

func TestTableRejectsCorruption(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	entries := putEntries(randKeys(rng, 2000, 'B'))
	data, meta, err := BuildTable(entries, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	corrupt := func(name string, mutate func([]byte) []byte, wantOpen bool) {
		t.Helper()
		d := mutate(bytes.Clone(data))
		tb, err := OpenTableAt(ctx, BytesSource(d), meta)
		if err != nil {
			if wantOpen || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("%s: open: %v", name, err)
			}
			return
		}
		if !wantOpen {
			t.Fatalf("%s: opened", name)
		}
		it := tb.Iter(ctx)
		for ok := it.SeekGE(nil); ok; ok = it.Next() {
		}
		if !errors.Is(it.Err(), ErrCorrupt) {
			t.Fatalf("%s: iterate: %v", name, it.Err())
		}
	}
	corrupt("block byte", func(d []byte) []byte { d[meta.IndexOff/2] ^= 0x40; return d }, true)
	corrupt("block crc", func(d []byte) []byte { d[0] ^= 1; return d }, true)
	corrupt("index byte", func(d []byte) []byte { d[meta.IndexOff+3] ^= 1; return d }, false)
	corrupt("truncated index", func(d []byte) []byte { return d[:meta.IndexOff+meta.IndexLen-1] }, false)
	bad := meta
	bad.IndexCRC ^= 1
	if _, err := OpenTableAt(ctx, BytesSource(data), bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong index crc: %v", err)
	}
	bad = meta
	bad.IndexLen = 0
	if _, err := OpenTableAt(ctx, BytesSource(data), bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("zero index length: %v", err)
	}
}

func TestTableSpaces(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	var entries []Entry
	for _, space := range []byte{'A', 'B', 'C'} {
		entries = append(entries, putEntries(randKeys(rng, 300+rng.IntN(500), space))...)
	}
	_, meta, err := BuildTable(entries, 700)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Spaces) != 3 {
		t.Fatalf("spaces %+v", meta.Spaces)
	}
	for _, e := range entries {
		i := slices.IndexFunc(meta.Spaces, func(s SpaceRange) bool { return s.Space == e.Key[0] })
		s := meta.Spaces[i]
		if bytes.Compare(e.Key, s.Min) < 0 || bytes.Compare(e.Key, s.Max) > 0 {
			t.Fatalf("key %x outside its space range [%x, %x]", e.Key, s.Min, s.Max)
		}
	}
	for i := 1; i < len(meta.Spaces); i++ {
		if meta.Spaces[i-1].Max[0] > meta.Spaces[i].Space {
			t.Fatalf("space %c max %x reaches into space %c", meta.Spaces[i-1].Space, meta.Spaces[i-1].Max, meta.Spaces[i].Space)
		}
	}
}

// FuzzOpenTable feeds the index decoder: the fuzzed bytes are the index of
// a table whose blocks are the seed's, with the CRC recomputed so the
// structural checks, not the checksum, are what is exercised.
func FuzzOpenTable(f *testing.F) {
	rng := rand.New(rand.NewPCG(7, 8))
	data, meta, err := BuildTable(putEntries(randKeys(rng, 200, 'F')), 512)
	if err != nil {
		f.Fatal(err)
	}
	blocks := data[:meta.IndexOff]
	f.Add(data[meta.IndexOff : meta.IndexOff+meta.IndexLen])
	f.Fuzz(func(t *testing.T, idx []byte) {
		ctx := context.Background()
		m := TableMeta{IndexOff: int64(len(blocks)), IndexLen: int64(len(idx)), IndexCRC: crc32.Checksum(idx, castagnoli)}
		tb, err := OpenTableAt(ctx, BytesSource(slices.Concat(blocks, idx)), m)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		it := tb.Iter(ctx)
		var prev []byte
		for ok := it.SeekGE(nil); ok; ok = it.Next() {
			if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
				t.Fatalf("unsorted output")
			}
			prev = bytes.Clone(it.Key())
			tb.Get(ctx, it.Key())
		}
	})
}

// TestSingleLocatesStoredValues: Single finds, from the index alone, the
// extent of a value that is alone in a stored block, and declines a value
// that shares its block or sits in a compressed one. Large values of every
// varint width of length, keys of varied length, a large value that zstd
// would shrink (it is stored anyway: one value, one block), and a tiny
// value left alone in the table's last block, which is declined: only a
// large value is worth an unverified extent read.
func TestSingleLocatesStoredValues(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	var entries []Entry
	incompressible := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.IntN(256))
		}
		return b
	}
	for i, n := range []int{LargeValueBytes, LargeValueBytes + 1, 127, 128, 16383, 16384, 3 * LargeValueBytes} {
		key := append([]byte{'F'}, bytes.Repeat([]byte{byte('a' + i)}, 1+i*20)...)
		entries = append(entries, Entry{Key: key, Kind: KindPut, Value: incompressible(n)})
	}
	// A large value zstd shrinks: alone in its block, stored raw.
	entries = append(entries, Entry{Key: []byte("Fzeros"), Kind: KindPut, Value: make([]byte, 2*LargeValueBytes)})
	// Small values that share blocks: compressible ones and random ones.
	for i := range 40 {
		entries = append(entries, Entry{Key: fmt.Appendf(nil, "G%03d", i), Kind: KindPut, Value: bytes.Repeat([]byte{byte(i)}, 20)})
		entries = append(entries, Entry{Key: fmt.Appendf(nil, "H%03d", i), Kind: KindPut, Value: incompressible(20)})
	}
	// A large value flushes the block after it, so a tiny last entry is
	// alone: a one-byte length, stored because zstd cannot save an eighth.
	entries = append(entries, Entry{Key: []byte("Ylarge"), Kind: KindPut, Value: incompressible(LargeValueBytes)},
		Entry{Key: []byte("Zlast"), Kind: KindPut, Value: incompressible(20)})
	slices.SortFunc(entries, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
	data, meta, err := BuildTable(entries, 1024)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tb, err := OpenTableAt(ctx, BytesSource(data), meta)
	if err != nil {
		t.Fatal(err)
	}
	singles := 0
	for _, e := range entries {
		off, length, ok := tb.Single(e.Key)
		alone := len(e.Value) >= LargeValueBytes
		if ok != alone {
			t.Fatalf("%q: single %v, want %v", e.Key, ok, alone)
		}
		if !ok {
			continue
		}
		singles++
		got, err := tb.ReadAt(ctx, off, length)
		if err != nil || !bytes.Equal(got, e.Value) {
			t.Fatalf("%q: extent %d+%d reads %d bytes (%v), want the value of %d", e.Key, off, length, len(got), err, len(e.Value))
		}
	}
	if singles != 7 {
		t.Fatalf("%d single values, want 7", singles)
	}
	if _, _, ok := tb.Single([]byte("Fnone")); ok {
		t.Fatal("a missing key located")
	}
}

// TestSingleDeclinesAPackedBlock: a block that holds several stored,
// incompressible entries is not a single, whichever of its keys is asked.
func TestSingleDeclinesAPackedBlock(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	keys := randKeys(rng, 3, 'A')
	entries := make([]Entry, len(keys))
	for i, k := range keys {
		v := make([]byte, 300)
		for j := range v {
			v[j] = byte(rng.IntN(256))
		}
		entries[i] = Entry{Key: k, Kind: KindPut, Value: v}
	}
	data, meta, err := BuildTable(entries, 4096)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := OpenTableAt(context.Background(), BytesSource(data), meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(tb.index) != 1 || data[tb.index[0].off+4] != blockStored {
		t.Fatalf("%d blocks, mode %d: want one stored block", len(tb.index), data[tb.index[0].off+4])
	}
	for _, k := range keys {
		if _, _, ok := tb.Single(k); ok {
			t.Fatalf("%x located in a packed block", k)
		}
	}
}

// FuzzDecodeBlock: a block either fails with ErrCorrupt or decodes to
// strictly increasing keys whose entries read without a panic. The
// checksum is fixed up so the fuzzer reaches the decoder behind it.
func FuzzDecodeBlock(f *testing.F) {
	w := NewTableWriter(512)
	for i := 0; len(w.index) == 0; i++ {
		if err := w.Add(Entry{Key: binary.BigEndian.AppendUint32([]byte{'A'}, uint32(i)), Kind: KindPut, Value: []byte("value")}); err != nil {
			f.Fatal(err)
		}
	}
	bi := w.index[0]
	f.Add(bytes.Clone(w.out[bi.off:bi.off+bi.length]), bi.rawLen)
	f.Fuzz(func(t *testing.T, stored []byte, rawLen int64) {
		if rawLen < 0 || rawLen > 1<<20 { // a bigger claim only costs memory
			return
		}
		if len(stored) >= 4 {
			binary.BigEndian.PutUint32(stored, crc32.Checksum(stored[4:], castagnoli))
		}
		b, err := decodeBlock(stored, rawLen)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		for i := range b.offs {
			e := b.entry(i)
			if !bytes.Equal(e.Key, b.key(i)) {
				t.Fatalf("entry %d key %x, key() %x", i, e.Key, b.key(i))
			}
			if i > 0 && bytes.Compare(b.key(i-1), e.Key) >= 0 {
				t.Fatalf("keys %x, %x not increasing", b.key(i-1), e.Key)
			}
		}
	})
}
