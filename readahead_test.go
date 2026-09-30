package lsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axiomhq/lsm/setmerge"
)

// scanTables are the table shapes a scan must read as Iter does: small
// blocks, large values each a block of its own among small ones, and a
// table of one block.
func scanTables(t *testing.T) map[string][]byte {
	t.Helper()
	rng := rand.New(rand.NewPCG(7, 8))
	mixed := putEntries(randKeys(rng, 600, 'M'))
	for i := range mixed {
		if i%7 == 0 {
			mixed[i].Value = bytes.Repeat([]byte{byte(i)}, LargeValueBytes+i)
		}
	}
	out := map[string][]byte{}
	for name, c := range map[string]struct {
		entries    []Entry
		blockBytes int
	}{
		"small blocks": {putEntries(randKeys(rng, 3000, 'S')), 256},
		"large values": {mixed, 2048},
		"one block":    {putEntries(randKeys(rng, 20, 'O')), 0},
	} {
		data, _, err := BuildTable(c.entries, c.blockBytes)
		if err != nil {
			t.Fatal(err)
		}
		out[name] = data
	}
	return out
}

func openBytes(t *testing.T, data []byte) *Table {
	t.Helper()
	tb, err := OpenTableAt(context.Background(), BytesSource(data), tableMetaOf(t, data))
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

// tableMetaOf reads a table's index position back from its footer.
func tableMetaOf(t *testing.T, data []byte) TableMeta {
	t.Helper()
	f := data[len(data)-footerBytes:]
	return TableMeta{IndexOff: int64(binary.BigEndian.Uint64(f[0:8])), IndexLen: int64(binary.BigEndian.Uint64(f[8:16]))}
}

var scanWindows = []int64{1, 3000, 64 << 10, 1 << 20}

// TestScanMatchesIter: a scan yields the entries Iter does, byte for
// byte, under every bound, window and seek order: full passes, and seeks
// forwards and back across windows, each followed by a few steps.
func TestScanMatchesIter(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(9, 9))
	for name, data := range scanTables(t) {
		tb := openBytes(t, data)
		all := collect(t, tb.Iter(ctx))
		mid := all[len(all)/2].Key
		his := [][]byte{nil, mid, append(bytes.Clone(mid), 0), all[0].Key, {0xff}}
		for _, hi := range his {
			want := collect(t, Bound(tb.Iter(ctx), nil, hi))
			for _, w := range scanWindows {
				if got := collect(t, Bound(tb.scan(ctx, w, hi), nil, hi)); !sameEntries(got, want) {
					t.Fatalf("%s hi %x window %d: scan yields %d entries, Iter %d", name, hi, w, len(got), len(want))
				}
				plain, scan := Bound(tb.Iter(ctx), nil, hi), Bound(tb.scan(ctx, w, hi), nil, hi)
				for range 40 {
					var target []byte
					if rng.IntN(10) > 0 {
						target = all[rng.IntN(len(all))].Key
						if rng.IntN(2) == 0 {
							target = append(bytes.Clone(target), 1) // between keys
						}
					}
					okP, okS := plain.SeekGE(target), scan.SeekGE(target)
					for step := 0; ; step++ {
						if okP != okS || okP && (!bytes.Equal(plain.Key(), scan.Key()) || plain.Kind() != scan.Kind() || !bytes.Equal(plain.Value(), scan.Value())) {
							t.Fatalf("%s hi %x window %d: seek %x step %d differs", name, hi, w, target, step)
						}
						if !okP || step == rng.IntN(20) {
							break
						}
						okP, okS = plain.Next(), scan.Next()
					}
				}
				if plain.Err() != nil || scan.Err() != nil {
					t.Fatalf("%s: errors %v %v", name, plain.Err(), scan.Err())
				}
			}
		}
	}
}

// TestScanReadsWindows: a full scan reads its table a window at a time,
// where Iter reads a block at a time.
func TestScanReadsWindows(t *testing.T) {
	ctx := context.Background()
	data := scanTables(t)["small blocks"]
	meta := tableMetaOf(t, data)
	src := &atomicCountingSource{BytesSource: data}
	tb, err := OpenTableAt(ctx, src, meta)
	if err != nil {
		t.Fatal(err)
	}
	src.reads.Store(0)
	collect(t, tb.Iter(ctx))
	blocks := src.reads.Swap(0)
	const window = 16 << 10
	collect(t, tb.scan(ctx, window, nil))
	if reads, max := src.reads.Load(), meta.IndexOff/window+2; reads > max || blocks < 8*reads {
		t.Fatalf("scan made %d reads (at most %d), Iter %d", reads, max, blocks)
	}
}

// atomicCountingSource counts reads made from any goroutine: a scan reads
// its next window while the cursor walks this one.
type atomicCountingSource struct {
	BytesSource
	reads atomic.Int64
}

func (c *atomicCountingSource) ReadAt(ctx context.Context, off, length int64) ([]byte, error) {
	c.reads.Add(1)
	return c.BytesSource.ReadAt(ctx, off, length)
}

// TestScanCorruptBlock: a corrupt block inside a window fails the scan
// where it fails Iter, with the same error, after the same entries.
func TestScanCorruptBlock(t *testing.T) {
	ctx := context.Background()
	data := bytes.Clone(scanTables(t)["small blocks"])
	data[tableMetaOf(t, data).IndexOff/2] ^= 0x40
	tb := openBytes(t, data)
	run := func(it Iterator) (n int, err error) {
		for ok := it.SeekGE(nil); ok; ok = it.Next() {
			n++
		}
		return n, it.Err()
	}
	wantN, wantErr := run(tb.Iter(ctx))
	if !errors.Is(wantErr, ErrCorrupt) {
		t.Fatalf("Iter: %v", wantErr)
	}
	for _, w := range scanWindows {
		n, err := run(tb.scan(ctx, w, nil))
		if n != wantN || !errors.Is(err, ErrCorrupt) || err.Error() != wantErr.Error() {
			t.Fatalf("window %d: %d entries then %v; Iter %d then %v", w, n, err, wantN, wantErr)
		}
	}
}

// compactFixture is a level-0 job over a populated level 1: several
// partitions, each with outputs of several files.
func compactFixture(t *testing.T) (*memStore, Version, Job, Options) {
	t.Helper()
	ctx := context.Background()
	st := newMemStore()
	o := Options{L0Trigger: 2, LevelRatio: 3, BaseBytes: 1 << 30, FileBytes: 4 << 10, BlockBytes: 256, Workers: 1}
	rng := rand.New(rand.NewPCG(11, 12))
	universe := randKeys(rng, 3000, 'K')
	// batch is a flush of 600 distinct keys: puts, deletes and merge operands.
	batch := func() []Entry {
		var out []Entry
		for _, i := range rng.Perm(len(universe))[:600] {
			e := Entry{Key: universe[i], Kind: KindMerge, Value: setmerge.Operand(set(uint32(rng.IntN(50))), nil)}
			switch rng.IntN(5) {
			case 0:
				e.Kind, e.Value = KindPut, setmerge.Value(set(uint32(i)))
			case 1:
				e.Kind, e.Value = KindDelete, nil
			}
			out = append(out, e)
		}
		slices.SortFunc(out, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
		return out
	}
	var v Version
	var err error
	for round := range 2 {
		for range 4 {
			if v, _, err = Flush(ctx, v, batch(), o, st.put); err != nil {
				t.Fatal(err)
			}
		}
		if round == 0 {
			if v, _, err = Compact(ctx, v, v.job(0, v.Levels[0], o), o, st.reader(), st.put); err != nil {
				t.Fatal(err)
			}
		}
	}
	j := v.job(0, v.Levels[0], o)
	if len(j.Overlap) < 4 {
		t.Fatalf("fixture overlap %d files", len(j.Overlap))
	}
	return st, v, j, o
}

func (m *memStore) clone() *memStore {
	return &memStore{objects: maps.Clone(m.objects), puts: m.puts}
}

// TestCompactReadAheadAndUploadsMatch: read-ahead and concurrent puts
// change when bytes move, not which: the outputs are the files a
// block-at-a-time, put-and-wait compaction writes. Seqs are compared as a
// count only: partitions take them in the order their files finish.
func TestCompactReadAheadAndUploadsMatch(t *testing.T) {
	ctx := context.Background()
	st, v, j, o := compactFixture(t)
	run := func(readAhead int64, workers, uploads int) (Edit, [][]byte) {
		s := st.clone()
		o := o
		o.ReadAheadBytes, o.Workers, o.Uploads = readAhead, workers, uploads
		_, e, err := Compact(ctx, v, j, o, s.reader(), s.put)
		if err != nil {
			t.Fatal(err)
		}
		added := slices.SortedFunc(slices.Values(e.Add[1]), func(a, b FileRef) int { return bytes.Compare(a.Min, b.Min) })
		var files [][]byte
		for _, f := range added {
			files = append(files, s.objects[f.Key])
		}
		return e, files
	}
	wantEdit, want := run(-1, 1, -1)
	if len(want) < 8 {
		t.Fatalf("fixture wrote %d files", len(want))
	}
	for _, c := range []struct {
		readAhead        int64
		workers, uploads int
	}{{0, 1, -1}, {0, 1, 0}, {1000, 3, 2}, {-1, 4, 1}, {1 << 20, 4, 8}} {
		e, files := run(c.readAhead, c.workers, c.uploads)
		if !slices.EqualFunc(files, want, bytes.Equal) || !slices.Equal(e.Del, wantEdit.Del) || e.NextSeq != wantEdit.NextSeq {
			t.Fatalf("%+v: %d files, want %d; or the deletes or NextSeq differ", c, len(files), len(want))
		}
	}
}

// TestCompactUploadFailure: a put failing while others are in flight
// fails the job with its error; the Edit lists exactly the files whose
// puts succeeded, and never more than Uploads puts run at once.
func TestCompactUploadFailure(t *testing.T) {
	ctx := context.Background()
	st, v, j, o := compactFixture(t)
	o.Workers, o.Uploads = 3, 2
	boom := errors.New("boom")
	var calls, inflight, peak atomic.Int64
	put := func(ctx context.Context, level int, seq uint64, data []byte) (string, error) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		if calls.Add(1) == 3 {
			return "", boom
		}
		return st.put(ctx, level, seq, data)
	}
	_, e, err := Compact(ctx, v, j, o, st.reader(), put)
	if !errors.Is(err, boom) {
		t.Fatalf("error %v", err)
	}
	if peak.Load() > 2 {
		t.Fatalf("%d puts at once, Uploads 2", peak.Load())
	}
	if len(e.Del) != 0 || len(e.Add[1]) != int(calls.Load())-1 {
		t.Fatalf("edit %d adds, %d dels after %d puts", len(e.Add[1]), len(e.Del), calls.Load())
	}
	for _, f := range e.Add[1] {
		if _, ok := st.objects[f.Key]; !ok || f.Seq >= e.NextSeq {
			t.Fatalf("added %s (seq %d, next %d) not stored", f.Key, f.Seq, e.NextSeq)
		}
	}
}

// TestCompactCancelStopsUploads: cancelling the job's context ends the
// puts in flight and starts no other; Compact returns only after the ones
// in flight have.
func TestCompactCancelStopsUploads(t *testing.T) {
	st, v, j, o := compactFixture(t)
	o.Workers, o.Uploads = 3, 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var started, inflight atomic.Int64
	var once sync.Once
	put := func(ctx context.Context, level int, seq uint64, data []byte) (string, error) {
		started.Add(1)
		inflight.Add(1)
		defer inflight.Add(-1)
		once.Do(cancel)
		<-ctx.Done()
		return "", ctx.Err()
	}
	_, e, err := Compact(ctx, v, j, o, st.reader(), put)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	if inflight.Load() != 0 || started.Load() > 2 || len(e.Add[1]) != 0 {
		t.Fatalf("after return: %d puts in flight, %d started, %d added", inflight.Load(), started.Load(), len(e.Add[1]))
	}
}

// slowSource is a Source whose reads take a round trip, counting the
// reads in flight.
type slowSource struct {
	BytesSource
	inflight *atomic.Int64
}

func (s slowSource) ReadAt(ctx context.Context, off, length int64) ([]byte, error) {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	time.Sleep(2 * time.Millisecond)
	return s.BytesSource.ReadAt(ctx, off, length)
}

// TestCompactFailureSettlesReads: a job that fails while its merges have
// windows in flight returns only once those reads have.
func TestCompactFailureSettlesReads(t *testing.T) {
	st, v, j, o := compactFixture(t)
	o.Workers, o.Uploads, o.ReadAheadBytes = 3, 2, 1000
	var inflight atomic.Int64
	r := Reader{Merger: setmerge.Merger{}, Open: func(ctx context.Context, f FileRef) (*Table, error) {
		return OpenTableAt(ctx, slowSource{st.objects[f.Key], &inflight}, f.TableMeta)
	}}
	boom := errors.New("boom")
	put := func(context.Context, int, uint64, []byte) (string, error) { return "", boom }
	for range 20 {
		if _, _, err := Compact(context.Background(), v, j, o, r, put); !errors.Is(err, boom) {
			t.Fatalf("error %v", err)
		}
		if n := inflight.Load(); n != 0 {
			t.Fatalf("%d reads still running after Compact returned", n)
		}
	}
}
