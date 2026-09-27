package lsm

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"testing"
)

// benchTables builds n tables of rows entries each, keys disjoint by
// stripe so the merge interleaves every table.
func benchTables(b *testing.B, n, rows int) []*Table {
	b.Helper()
	ctx := context.Background()
	tables := make([]*Table, n)
	val := make([]byte, 16)
	for t := range n {
		w := NewTableWriter(DefaultBlockBytes)
		for i := range rows {
			key := binary.BigEndian.AppendUint64([]byte{'V'}, uint64(i*n+t))
			if err := w.Add(Entry{Key: key, Kind: KindPut, Value: val}); err != nil {
				b.Fatal(err)
			}
		}
		data, meta, err := w.Finish()
		if err != nil {
			b.Fatal(err)
		}
		tables[t], err = OpenTableAt(ctx, BytesSource(data), meta)
		if err != nil {
			b.Fatal(err)
		}
	}
	return tables
}

func BenchmarkMergeIter(b *testing.B) {
	const n, rows = 8, 100_000
	tables := benchTables(b, n, rows)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		its := make([]Iterator, n)
		for i, t := range tables {
			its[i] = t.Iter(ctx)
		}
		m := NewMerge(its...)
		keys := 0
		for ok := m.SeekGE(nil); ok; ok = m.Next() {
			keys++
		}
		if keys != n*rows {
			b.Fatalf("%d keys", keys)
		}
	}
	b.ReportMetric(float64(n*rows*b.N)/b.Elapsed().Seconds()/1e6, "Mkeys/s")
}

func BenchmarkResolveIter(b *testing.B) {
	const n, rows = 8, 100_000
	tables := benchTables(b, n, rows)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		its := make([]Iterator, n)
		for i, t := range tables {
			its[i] = t.Iter(ctx)
		}
		r := Resolve(NewMerge(its...), SetMerger{}, true)
		keys := 0
		for ok := r.SeekGE(nil); ok; ok = r.Next() {
			keys++
		}
		if keys != n*rows {
			b.Fatalf("%d keys", keys)
		}
	}
	b.ReportMetric(float64(n*rows*b.N)/b.Elapsed().Seconds()/1e6, "Mkeys/s")
}

func BenchmarkBlockDecode(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 1))
	w := NewTableWriter(DefaultBlockBytes)
	for i := 0; ; i++ {
		key := binary.BigEndian.AppendUint64([]byte{'A'}, uint64(i))
		val := make([]byte, 8+rng.IntN(24))
		if err := w.Add(Entry{Key: key, Kind: KindPut, Value: val}); err != nil {
			b.Fatal(err)
		}
		if len(w.index) == 1 {
			break
		}
	}
	bi := w.index[0]
	stored := w.out[bi.off : bi.off+bi.length]
	b.SetBytes(bi.rawLen)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := decodeBlock(stored, bi.rawLen); err != nil {
			b.Fatal(err)
		}
	}
}
