package lsm

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/axiomhq/lsm/setmerge"
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
		r := Resolve(NewMerge(its...), setmerge.Merger{}, true)
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

// BenchmarkBuildTable builds one 32 MiB table of small compressible
// entries, the shape of a flush's postings and document blocks, with a
// large incompressible value every so often.
func BenchmarkBuildTable(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 2))
	var entries []Entry
	for i, n := 0, 0; n < 32<<20; i++ {
		v := make([]byte, 64+rng.IntN(512))
		if i%64 == 0 {
			v = make([]byte, LargeValueBytes)
			for j := range v {
				v[j] = byte(rng.IntN(256))
			}
		} else {
			for j := range v {
				v[j] = byte('a' + rng.IntN(8))
			}
		}
		key := binary.BigEndian.AppendUint64([]byte{'D'}, uint64(i))
		entries = append(entries, Entry{Key: key, Kind: KindPut, Value: v})
		n += len(key) + len(v)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := BuildTable(entries, 0); err != nil {
			b.Fatal(err)
		}
	}
}
