package lsm

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

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

// latencySource is an object store's range read: every request waits a
// fixed round trip, whatever its size.
type latencySource struct {
	BytesSource
	rtt time.Duration
}

func (s latencySource) ReadAt(ctx context.Context, off, length int64) ([]byte, error) {
	time.Sleep(s.rtt)
	return s.BytesSource.ReadAt(ctx, off, length)
}

// BenchmarkCompactLatency is a level-0 job of about 110 MB of tables (225
// MB of entries) against a store whose every read takes a 2 ms round trip
// and every put that plus 100 MB/s for its bytes (a 48 MiB file in half a
// second): four level-0 files of updates spread over the key range, over
// sixteen level-1 files. It runs the job block by block with each merge
// waiting for its puts, with read-ahead alone, and with read-ahead and
// concurrent puts.
func BenchmarkCompactLatency(b *testing.B) {
	const rtt = 2 * time.Millisecond
	const putNsPerByte = 10 * time.Nanosecond // 100 MB/s per put stream
	const keys, l1Files, l0Files = 1_600_000, 16, 4
	rng := rand.New(rand.NewPCG(3, 3))
	entry := func(i int) Entry {
		v := make([]byte, 100) // half random, half zeros: about 2:1 under zstd
		for j := range 50 {
			v[j] = byte(rng.IntN(256))
		}
		return Entry{Key: binary.BigEndian.AppendUint64([]byte{'V'}, uint64(i)), Kind: KindPut, Value: v}
	}
	objects := map[string][]byte{}
	var v Version
	var inBytes int64
	add := func(level int, entries []Entry) {
		data, meta, err := BuildTable(entries, 0)
		if err != nil {
			b.Fatal(err)
		}
		key := fmt.Sprintf("sst/%d-%d", level, v.NextSeq)
		objects[key] = data
		inBytes += meta.Bytes
		if v, err = v.Apply(Edit{Add: map[int][]FileRef{level: {{Key: key, Seq: v.NextSeq, TableMeta: meta}}}}); err != nil {
			b.Fatal(err)
		}
	}
	for f := range l1Files {
		var entries []Entry
		for i := f * keys / l1Files; i < (f+1)*keys/l1Files; i++ {
			entries = append(entries, entry(i))
		}
		add(1, entries)
	}
	for f := range l0Files {
		var entries []Entry
		for i := f; i < keys; i += 4 * l0Files {
			entries = append(entries, entry(i))
		}
		add(0, entries)
	}
	o := DefaultOptions()
	o.Workers = 2 // eight partitions: four rounds, as a job much wider than its workers runs
	j := v.job(0, v.Levels[0], o)
	r := Reader{Open: func(ctx context.Context, f FileRef) (*Table, error) {
		return OpenTableAt(ctx, latencySource{objects[f.Key], rtt}, f.TableMeta)
	}}
	var puts atomic.Int64
	put := func(_ context.Context, level int, seq uint64, data []byte) (string, error) {
		time.Sleep(rtt + time.Duration(len(data))*putNsPerByte)
		return fmt.Sprintf("out/%d-%d-%d", level, seq, puts.Add(1)), nil
	}
	for _, arm := range []struct {
		name      string
		readAhead int64
		uploads   int
	}{
		{"block-reads,sync-puts", -1, -1},
		{"read-ahead,sync-puts", 0, -1},
		{"read-ahead,async-puts", 0, 0},
	} {
		b.Run(arm.name, func(b *testing.B) {
			o := o
			o.ReadAheadBytes, o.Uploads = arm.readAhead, arm.uploads
			b.SetBytes(inBytes)
			for b.Loop() {
				if _, _, err := Compact(context.Background(), v, j, o, r, put); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGet is a point lookup in a four-level version of 1 + 8 + 100 +
// 300 small in-memory tables, each level a full-keyspace run holding every
// fourth key of the level below; every lookup finds its key at the bottom.
func BenchmarkGet(b *testing.B) {
	const keys = 300 * 16
	ctx := context.Background()
	key := func(i int) []byte { return binary.BigEndian.AppendUint32([]byte{'K'}, uint32(i)) }
	tables := map[string]*Table{}
	add := map[int][]FileRef{}
	seq := uint64(1)
	for l, files := range []int{1, 8, 100, 300} {
		stride := 1 << (2 * (3 - l)) // 64, 16, 4, 1
		for f := range files {
			w := NewTableWriter(DefaultBlockBytes)
			for i := f * keys / files; i < (f+1)*keys/files; i += stride {
				if err := w.Add(Entry{Key: key(i), Kind: KindPut, Value: []byte{byte(l)}}); err != nil {
					b.Fatal(err)
				}
			}
			data, meta, err := w.Finish()
			if err != nil {
				b.Fatal(err)
			}
			ref := FileRef{Key: fmt.Sprint("f", seq), Seq: seq, TableMeta: meta}
			seq++
			if tables[ref.Key], err = OpenTableAt(ctx, BytesSource(data), meta); err != nil {
				b.Fatal(err)
			}
			add[l] = append(add[l], ref)
		}
	}
	v, err := Version{}.Apply(Edit{Add: add})
	if err != nil {
		b.Fatal(err)
	}
	r := Reader{Open: func(_ context.Context, f FileRef) (*Table, error) { return tables[f.Key], nil }}
	rng := rand.New(rand.NewPCG(1, 2))
	lookups := make([][]byte, 1024)
	for i := range lookups {
		lookups[i] = key(rng.IntN(keys))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, ok, err := r.Get(ctx, v, lookups[i%len(lookups)]); !ok || err != nil {
			b.Fatalf("get: %v %v", ok, err)
		}
	}
}
