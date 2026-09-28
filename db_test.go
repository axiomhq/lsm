package lsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/axiomhq/lsm/setmerge"
)

func set(xs ...uint32) *roaring.Bitmap { return roaring.BitmapOf(xs...) }

func TestMergeIterPrecedence(t *testing.T) {
	newer := &entriesIter{entries: []Entry{{Key: []byte("b"), Kind: KindPut, Value: []byte("new")}, {Key: []byte("d"), Kind: KindDelete}}}
	older := &entriesIter{entries: []Entry{{Key: []byte("a"), Kind: KindPut, Value: []byte("a")}, {Key: []byte("b"), Kind: KindPut, Value: []byte("old")}, {Key: []byte("c"), Kind: KindPut, Value: []byte("c")}}}
	m := NewMerge(newer, older)
	var got []string
	for ok := m.SeekGE(nil); ok; ok = m.Next() {
		got = append(got, fmt.Sprintf("%s:%d:%s:%d", m.Key(), m.Kind(), m.Value(), m.Source()))
	}
	want := []string{"a:1:a:1", "b:1:new:0", "b:1:old:1", "c:1:c:1", "d:2::0"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !m.SeekGE([]byte("c")) || string(m.Key()) != "c" || !m.Next() || string(m.Key()) != "d" || m.Next() {
		t.Fatal("seek to c then d then end")
	}
}

func TestResolveSemantics(t *testing.T) {
	k := []byte("k")
	op := func(add, remove *roaring.Bitmap) Entry {
		return Entry{Key: k, Kind: KindMerge, Value: setmerge.Operand(add, remove)}
	}
	put := func(s *roaring.Bitmap) Entry { return Entry{Key: k, Kind: KindPut, Value: setmerge.Value(s)} }
	del := Entry{Key: k, Kind: KindDelete}
	cases := []struct {
		name     string
		newest   []Entry // newest first, one per "table"
		bottom   bool
		wantKind Kind
		wantSet  *roaring.Bitmap // for KindPut, or the operand's effect on {100} for KindMerge
		absent   bool
	}{
		{"put wins", []Entry{put(set(1)), put(set(2))}, false, KindPut, set(1), false},
		{"delete kept above", []Entry{del, put(set(2))}, false, KindDelete, nil, false},
		{"delete dropped at bottom", []Entry{del, put(set(2))}, true, 0, nil, true},
		{"ops over put", []Entry{op(set(3), nil), op(nil, set(1)), put(set(1, 2))}, false, KindPut, set(2, 3), false},
		{"ops over delete", []Entry{op(set(3), nil), del, put(set(1, 2))}, false, KindPut, set(3), false},
		{"ops over delete to empty", []Entry{op(nil, set(1)), del, put(set(1))}, false, KindDelete, nil, false},
		{"ops over delete to empty at bottom", []Entry{op(nil, set(1)), del}, true, 0, nil, true},
		{"ops without base above", []Entry{op(set(3), nil), op(nil, set(100))}, false, KindMerge, set(3), false},
		{"ops without base at bottom", []Entry{op(set(3), nil), op(nil, set(100))}, true, KindPut, set(3), false},
		{"ops to empty over put", []Entry{op(nil, set(1)), put(set(1))}, false, KindDelete, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			its := make([]Iterator, len(tc.newest))
			for i, e := range tc.newest {
				its[i] = &entriesIter{entries: []Entry{e}}
			}
			r := Resolve(NewMerge(its...), setmerge.Merger{}, tc.bottom)
			ok := r.SeekGE(nil)
			if tc.absent {
				if ok {
					t.Fatalf("want absent, got kind %d", r.Kind())
				}
				return
			}
			if !ok || r.Kind() != tc.wantKind || !bytes.Equal(r.Key(), k) {
				t.Fatalf("ok %v kind %d", ok, r.Kind())
			}
			switch tc.wantKind {
			case KindPut:
				got, err := setmerge.Decode(r.Value())
				if err != nil || !got.Equals(tc.wantSet) {
					t.Fatalf("set %v %v", got, err)
				}
			case KindMerge:
				v, keep, err := setmerge.Merger{}.Full(setmerge.Value(set(100)), [][]byte{r.Value()})
				got, _ := setmerge.Decode(v)
				if err != nil || !keep || !got.Equals(tc.wantSet) {
					t.Fatalf("collapsed operand gives %v", got)
				}
			}
			if r.Next() {
				t.Fatal("second key")
			}
		})
	}
}

// memStore is the Putter and Opener over a map.
type memStore struct {
	mu      sync.Mutex // Compact publishes partitions concurrently
	objects map[string][]byte
	puts    int
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) put(_ context.Context, level int, seq uint64, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("sst/%d-%06d", level, seq)
	if _, dup := m.objects[key]; dup {
		return "", fmt.Errorf("duplicate object %s", key)
	}
	m.objects[key] = bytes.Clone(data)
	m.puts++
	return key, nil
}

func (m *memStore) open(ctx context.Context, f FileRef) (*Table, error) {
	m.mu.Lock()
	data, ok := m.objects[f.Key]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("missing object %s", f.Key)
	}
	return OpenTableAt(ctx, BytesSource(data), f.TableMeta)
}

func (m *memStore) reader() Reader { return Reader{Open: m.open, Merger: setmerge.Merger{}} }

// model is the reference: the set each key should hold.
type model map[string]*roaring.Bitmap

func (m model) apply(e Entry) {
	k := string(e.Key)
	switch e.Kind {
	case KindPut:
		s, _ := setmerge.Decode(e.Value)
		m[k] = s
	case KindDelete:
		delete(m, k)
	case KindMerge:
		add, remove, _ := setmerge.DecodeOperand(e.Value)
		s := m[k]
		if s == nil {
			s = roaring.New()
		}
		s.AndNot(remove)
		s.Or(add)
		if s.IsEmpty() {
			delete(m, k)
		} else {
			m[k] = s
		}
	}
}

func (m model) sortedKeys() [][]byte {
	keys := make([][]byte, 0, len(m))
	for k := range m {
		keys = append(keys, []byte(k))
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys
}

func checkModel(t *testing.T, ctx context.Context, v Version, st *memStore, m model, rng *rand.Rand, universe [][]byte) {
	t.Helper()
	it, err := st.reader().Iter(ctx, v, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	keys := m.sortedKeys()
	i := 0
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		if it.Kind() != KindPut {
			t.Fatalf("resolved iterator emitted kind %d", it.Kind())
		}
		if i >= len(keys) || !bytes.Equal(it.Key(), keys[i]) {
			t.Fatalf("scan key %d: got %x", i, it.Key())
		}
		got, err := setmerge.Decode(it.Value())
		if err != nil || !got.Equals(m[string(keys[i])]) {
			t.Fatalf("scan key %x: set %v (%v), want %v", keys[i], got, err, m[string(keys[i])])
		}
		i++
	}
	if err := it.Err(); err != nil || i != len(keys) {
		t.Fatalf("scan ended at %d of %d: %v", i, len(keys), err)
	}
	for range 64 {
		k := universe[rng.IntN(len(universe))]
		val, ok, err := st.reader().Get(ctx, v, k)
		if err != nil {
			t.Fatal(err)
		}
		want, exists := m[string(k)]
		if ok != exists {
			t.Fatalf("get %x: present %v want %v", k, ok, exists)
		}
		if ok {
			got, _ := setmerge.Decode(val)
			if !got.Equals(want) {
				t.Fatalf("get %x: %v want %v", k, got, want)
			}
		}
	}
	// A bounded range scan agrees too.
	if len(universe) > 2 {
		lo, hi := universe[rng.IntN(len(universe))], universe[rng.IntN(len(universe))]
		if bytes.Compare(lo, hi) > 0 {
			lo, hi = hi, lo
		}
		it, err := st.reader().Iter(ctx, v, lo, hi)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for ok := it.SeekGE(nil); ok; ok = it.Next() { // nil seeks to lo
			if bytes.Compare(it.Key(), lo) < 0 || bytes.Compare(it.Key(), hi) >= 0 {
				t.Fatalf("range [%x,%x) yielded %x", lo, hi, it.Key())
			}
			n++
		}
		want := 0
		for _, k := range keys {
			if bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
				want++
			}
		}
		if n != want {
			t.Fatalf("range count %d want %d", n, want)
		}
	}
}

// TestVersionMatchesModel is the property test: random puts, deletes and
// set operands flushed in batches and compacted at random points read
// back exactly like a map, through the resolved scan, point gets and
// bounded ranges; a final compaction to the bottom leaves no tombstone or
// operand behind.
func TestVersionMatchesModel(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			ctx := context.Background()
			st := newMemStore()
			m := model{}
			universe := randKeys(rng, 300, 'K')
			o := Options{L0Trigger: 2 + rng.IntN(3), LevelRatio: 3, BaseBytes: 20 << 10, FileBytes: 8 << 10, BlockBytes: 512}
			var v Version
			for batch := 0; batch < 30; batch++ {
				n := 20 + rng.IntN(60)
				byKey := map[string]Entry{}
				for range n {
					k := universe[rng.IntN(len(universe))]
					var e Entry
					switch rng.IntN(10) {
					case 0, 1:
						e = Entry{Key: k, Kind: KindPut, Value: setmerge.Value(set(uint32(rng.IntN(50)), uint32(rng.IntN(50))))}
					case 2:
						e = Entry{Key: k, Kind: KindDelete}
					default:
						var add, remove *roaring.Bitmap
						if rng.IntN(2) == 0 {
							add = set(uint32(rng.IntN(50)))
						}
						if rng.IntN(2) == 0 {
							remove = set(uint32(rng.IntN(50)))
						}
						e = Entry{Key: k, Kind: KindMerge, Value: setmerge.Operand(add, remove)}
					}
					byKey[string(k)] = e // one version per key per flush, last wins
				}
				var entries []Entry
				for _, e := range byKey {
					entries = append(entries, e)
				}
				slices.SortFunc(entries, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
				for _, e := range entries {
					m.apply(e)
				}
				next, _, err := Flush(ctx, v, entries, o, st.put)
				if err != nil {
					t.Fatal(err)
				}
				v = next
				checkModel(t, ctx, v, st, m, rng, universe)
				steps := rng.IntN(3)
				for s := 0; s < steps; s++ {
					j, ok := Pick(v, o)
					if !ok {
						break
					}
					if v, _, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
						t.Fatal(err)
					}
					checkModel(t, ctx, v, st, m, rng, universe)
				}
			}
			// Everything to the bottom: compact each level into the next,
			// down to the deepest level that exists now (a compaction of
			// the deepest would only open another one).
			depth := len(v.Levels)
			for l := 0; l < depth-1; l++ {
				if len(v.Levels[l]) == 0 {
					continue
				}
				j := v.job(l, v.Levels[l], o)
				next, _, err := Compact(ctx, v, j, o, st.reader(), st.put)
				if err != nil {
					t.Fatal(err)
				}
				v = next
				checkModel(t, ctx, v, st, m, rng, universe)
			}
			for l, files := range v.Levels {
				if l != len(v.Levels)-1 && len(files) != 0 {
					t.Fatalf("level %d still holds %d files", l, len(files))
				}
			}
			var raw int
			for _, f := range slices.Concat(v.Levels...) {
				tb, err := st.open(ctx, f)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range collect(t, tb.Iter(ctx)) {
					if e.Kind != KindPut {
						t.Fatalf("bottom level holds kind %d for %x", e.Kind, e.Key)
					}
					raw++
				}
			}
			if raw != len(m) {
				t.Fatalf("bottom holds %d entries, model %d", raw, len(m))
			}
		})
	}
}

func TestPickAndApplyInvariants(t *testing.T) {
	o := Options{L0Trigger: 2, LevelRatio: 2, BaseBytes: 100, FileBytes: 1 << 20}
	f := func(seq uint64, lo, hi string, n int64) FileRef {
		return FileRef{Key: fmt.Sprint("f", seq), Seq: seq, TableMeta: TableMeta{Min: []byte(lo), Max: []byte(hi), Bytes: n}}
	}
	v, err := Version{}.Apply(Edit{Add: map[int][]FileRef{0: {f(1, "a", "m", 10)}}})
	if err != nil || v.NextSeq != 2 {
		t.Fatalf("%v %+v", err, v)
	}
	if _, ok := Pick(v, o); ok {
		t.Fatal("one L0 file picked")
	}
	v, _ = v.Apply(Edit{Add: map[int][]FileRef{0: {f(2, "k", "z", 10)}, 1: {f(3, "a", "c", 60), f(4, "d", "f", 70), f(5, "x", "z", 5)}, 2: {f(6, "b", "e", 5)}}})
	if v.Levels[0][0].Seq != 2 {
		t.Fatal("L0 not newest first")
	}
	j, ok := Pick(v, o)
	if !ok || j.Level != 0 || len(j.Inputs) != 2 || len(j.Overlap) != 3 || j.Bottom {
		t.Fatalf("L0 job %+v", j)
	}
	// Without L0 pressure, level 1 (135 bytes over a budget of 100) picks
	// the file with the least overlap below: f5 ("x".."z") overlaps nothing
	// in level 2, so it is the bottom for its range.
	v2, _ := v.Apply(Edit{Del: []string{"f1", "f2"}})
	j, ok = Pick(v2, o)
	if !ok || j.Level != 1 || len(j.Inputs) != 1 || j.Inputs[0].Key != "f5" || len(j.Overlap) != 0 || !j.Bottom {
		t.Fatalf("L1 job %+v", j)
	}
	if _, err := v.Apply(Edit{Add: map[int][]FileRef{1: {f(7, "b", "b", 1)}}}); err == nil {
		t.Fatal("overlapping level-1 files accepted")
	}
}

// TestCompactLeavesUntouchedFiles: a level-0 file whose bounds span a
// level-1 file but holds no key inside it leaves that file where it is,
// same object, and the merged level still reads every key right.
func TestCompactLeavesUntouchedFiles(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	o := DefaultOptions()
	o.FileBytes = 200 // the ten-key files (~105 bytes) are past half a file
	put := func(key string, x uint32) Entry {
		return Entry{Key: []byte(key), Kind: KindPut, Value: setmerge.Value(set(x))}
	}
	m := model{}
	var v Version
	var l1 []FileRef
	for _, group := range []string{"a", "m", "z"} {
		var entries []Entry
		for i := range 10 {
			entries = append(entries, put(fmt.Sprintf("%s%02d", group, i), 1))
		}
		for _, e := range entries {
			m.apply(e)
		}
		next, f, err := Flush(ctx, v, entries, o, st.put)
		if err != nil {
			t.Fatal(err)
		}
		v = next
		l1 = append(l1, f)
	}
	var keys []string
	for _, f := range l1 {
		keys = append(keys, f.Key)
	}
	v, err := v.Apply(Edit{Del: keys, Add: map[int][]FileRef{1: l1}})
	if err != nil {
		t.Fatal(err)
	}
	// m05 updates the m file, m50 falls in the gap before z, zz after it:
	// the level-0 file's bounds span z00..z09 without a key there.
	entries := []Entry{put("m05", 2), put("m50", 3), put("zz", 4)}
	for _, e := range entries {
		m.apply(e)
	}
	if v, _, err = Flush(ctx, v, entries, o, st.put); err != nil {
		t.Fatal(err)
	}
	j := v.job(0, v.Levels[0], o)
	if len(j.Overlap) != 2 {
		t.Fatalf("overlap %d files, want m and z", len(j.Overlap))
	}
	puts := st.puts
	if v, _, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, f := range v.Levels[1] {
		have[f.Key] = true
	}
	if !have[l1[0].Key] || !have[l1[2].Key] || have[l1[1].Key] {
		t.Fatalf("level 1 %v: want a and z kept, m rewritten", v.Levels[1])
	}
	// The m file with m50 and the gap after z: two outputs, z not written.
	if n := st.puts - puts; n != 2 {
		t.Fatalf("compaction wrote %d files, want 2", n)
	}
	var universe [][]byte
	for k := range m {
		universe = append(universe, []byte(k))
	}
	checkModel(t, ctx, v, st, m, rand.New(rand.NewPCG(1, 2)), universe)
}

// TestCompactCutsFilesAtSpaces: a compaction's output files each hold one
// key space (the first key byte), so a later round can leave a space alone.
func TestCompactCutsFilesAtSpaces(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	o := DefaultOptions()
	var entries []Entry
	for _, k := range []string{"a1", "a2", "b1", "c1", "c2"} {
		entries = append(entries, Entry{Key: []byte(k), Kind: KindPut, Value: setmerge.Value(set(1))})
	}
	v, _, err := Flush(ctx, Version{}, entries, o, st.put)
	if err != nil {
		t.Fatal(err)
	}
	if v, _, err = Compact(ctx, v, v.job(0, v.Levels[0], o), o, st.reader(), st.put); err != nil {
		t.Fatal(err)
	}
	if len(v.Levels[1]) != 3 {
		t.Fatalf("level 1 has %d files, want one per space", len(v.Levels[1]))
	}
	for _, f := range v.Levels[1] {
		if len(f.Spaces) != 1 {
			t.Fatalf("file %s spans %d spaces", f.Key, len(f.Spaces))
		}
	}
}

// TestCompactGrowsSmallTailFile: a space whose keys only ever append grows
// its last file while it is under half a file, rather than leaving a small
// file behind every round.
func TestCompactGrowsSmallTailFile(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	o := DefaultOptions()
	m := model{}
	var v Version
	for round := range 6 {
		var entries []Entry
		for i := range 10 {
			e := Entry{Key: []byte(fmt.Sprintf("d%04d", round*10+i)), Kind: KindPut, Value: setmerge.Value(set(uint32(round)))}
			entries = append(entries, e)
			m.apply(e)
		}
		next, _, err := Flush(ctx, v, entries, o, st.put)
		if err != nil {
			t.Fatal(err)
		}
		if v, _, err = Compact(ctx, next, next.job(0, next.Levels[0], o), o, st.reader(), st.put); err != nil {
			t.Fatal(err)
		}
		if len(v.Levels[1]) != 1 {
			t.Fatalf("round %d: level 1 has %d files, want the tail grown onto one", round, len(v.Levels[1]))
		}
	}
	var universe [][]byte
	for k := range m {
		universe = append(universe, []byte(k))
	}
	checkModel(t, ctx, v, st, m, rand.New(rand.NewPCG(3, 4)), universe)
}

// readMergeErr reads key k of a one-file version holding a single merge
// operand op, through Get and through Iter, and returns both errors.
func readMergeErr(t *testing.T, r func(*memStore) Reader, op []byte) (getErr, iterErr error) {
	t.Helper()
	ctx := context.Background()
	st := newMemStore()
	k := []byte("k")
	v, _, err := Flush(ctx, Version{}, []Entry{{Key: k, Kind: KindMerge, Value: op}}, DefaultOptions(), st.put)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, getErr = r(st).Get(ctx, v, k); getErr == nil {
		t.Fatal("get: no error")
	}
	it, err := r(st).Iter(ctx, v, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if it.SeekGE(nil) {
		t.Fatalf("iter: yielded %x", it.Key())
	}
	return getErr, it.Err()
}

func TestMergeWithoutMerger(t *testing.T) {
	nilMerger := func(st *memStore) Reader { return Reader{Open: st.open} }
	getErr, iterErr := readMergeErr(t, nilMerger, setmerge.Operand(set(1), nil))
	if !errors.Is(getErr, ErrNoMerger) || !errors.Is(iterErr, ErrNoMerger) {
		t.Fatalf("get %v, iter %v: want ErrNoMerger", getErr, iterErr)
	}
}

func TestMalformedOperandIsCorrupt(t *testing.T) {
	getErr, iterErr := readMergeErr(t, (*memStore).reader, []byte{0xff})
	if !errors.Is(getErr, ErrCorrupt) || !errors.Is(iterErr, ErrCorrupt) {
		t.Fatalf("get %v, iter %v: want ErrCorrupt", getErr, iterErr)
	}
}

// TestVersionSignature: a file outside the ranges leaves the signature
// alone, one inside changes it, and no overlap at all is not ok.
func TestVersionSignature(t *testing.T) {
	f := func(seq uint64, lo, hi string) FileRef {
		return FileRef{Key: fmt.Sprint("f", seq), Seq: seq, TableMeta: TableMeta{Min: []byte(lo), Max: []byte(hi)}}
	}
	r := [2][]byte{[]byte("b"), []byte("d")}
	v, _ := Version{}.Apply(Edit{Add: map[int][]FileRef{1: {f(1, "a", "c")}}})
	sig, ok := v.Signature(r)
	if !ok {
		t.Fatal("no overlap found")
	}
	unrelated, _ := v.Apply(Edit{Add: map[int][]FileRef{0: {f(2, "x", "z")}, 1: {f(3, "m", "n")}}})
	if s, ok := unrelated.Signature(r); !ok || s != sig {
		t.Fatal("an unrelated file changed the signature")
	}
	overlapping, _ := unrelated.Apply(Edit{Add: map[int][]FileRef{0: {f(4, "c", "c")}}})
	if s, ok := overlapping.Signature(r); !ok || s == sig {
		t.Fatal("an overlapping file kept the signature")
	}
	if _, ok := overlapping.Signature([2][]byte{[]byte("p"), []byte("w")}); ok {
		t.Fatal("ok with no overlapping file")
	}
}

// TestReaderLocate: a large value comes back as its extent, a small one as
// the value, a deleted or missing key as not ok, and a merge entry as
// ErrCorrupt.
func TestReaderLocate(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	big := make([]byte, LargeValueBytes)
	for i := range big {
		big[i] = byte(i * 7)
	}
	v, _, err := Flush(ctx, Version{}, []Entry{
		{Key: []byte("big"), Kind: KindPut, Value: big},
		{Key: []byte("gone"), Kind: KindPut, Value: []byte("old")},
		{Key: []byte("op"), Kind: KindMerge, Value: setmerge.Operand(set(1), nil)},
		{Key: []byte("small"), Kind: KindPut, Value: []byte("tiny")},
	}, DefaultOptions(), st.put)
	if err != nil {
		t.Fatal(err)
	}
	// The delete is alone in its file and so in a stored block: Single must
	// decline it (the kind byte is not in the index) for Locate to see it.
	if v, _, err = Flush(ctx, v, []Entry{{Key: []byte("gone"), Kind: KindDelete}}, DefaultOptions(), st.put); err != nil {
		t.Fatal(err)
	}
	r := st.reader()
	l, ok, err := r.Locate(ctx, v, []byte("big"))
	if err != nil || !ok || l.Table == nil || l.Value != nil {
		t.Fatalf("big: %+v %v %v, want an extent", l, ok, err)
	}
	if got, err := l.Table.ReadAt(ctx, l.Off, l.Length); err != nil || !bytes.Equal(got, big) {
		t.Fatalf("big: extent reads %d bytes (%v)", len(got), err)
	}
	if l, ok, err := r.Locate(ctx, v, []byte("small")); err != nil || !ok || l.Table != nil || string(l.Value) != "tiny" {
		t.Fatalf("small: %+v %v %v, want the value", l, ok, err)
	}
	for _, k := range []string{"gone", "missing"} {
		if _, ok, err := r.Locate(ctx, v, []byte(k)); err != nil || ok {
			t.Fatalf("%s: ok %v err %v", k, ok, err)
		}
	}
	if _, _, err := r.Locate(ctx, v, []byte("op")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("op: %v, want ErrCorrupt", err)
	}
}

// TestManifestEncodingIsStable: a manifest written by v0.2.0, whose json
// names are the encoding every later version promises, decodes with every
// field in place.
func TestManifestEncodingIsStable(t *testing.T) {
	const old = `{"levels":[[{"key":"sst/0-000007","seq":7,"min":"YQ==","max":"eg==","bytes":1234,"count":9,
		"index_off":1100,"index_len":102,"spaces":[{"space":97,"min":"YQ==","max":"eg=="}]}]],"next_seq":8}`
	var v Version
	if err := json.Unmarshal([]byte(old), &v); err != nil {
		t.Fatal(err)
	}
	f := v.Levels[0][0]
	if v.NextSeq != 8 || f.Key != "sst/0-000007" || f.Seq != 7 || f.IndexOff != 1100 || f.IndexLen != 102 || f.Count != 9 || f.Bytes != 1234 ||
		string(f.Min) != "a" || len(f.Spaces) != 1 || f.Spaces[0].Space != 'a' {
		t.Fatalf("decoded %+v", v)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{`"key"`, `"seq"`, `"index_off"`, `"index_len"`, `"next_seq"`, `"spaces"`} {
		if !bytes.Contains(out, []byte(name)) {
			t.Fatalf("encoding lost %s: %s", name, out)
		}
	}
}

// TestCompactReportsOrphansOnFailure: a Putter that fails part way leaves
// the files already published listed in the returned Edit, and the error
// names the level and sequence that failed.
func TestCompactReportsOrphansOnFailure(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	o := Options{L0Trigger: 2, LevelRatio: 3, BaseBytes: 20 << 10, FileBytes: 1 << 10, BlockBytes: 256, Workers: 1}
	rng := rand.New(rand.NewPCG(5, 5))
	m := model{}
	var v Version
	var err error
	for range 6 {
		if v, _, err = Flush(ctx, v, randBatch(rng, randKeys(rng, 200, 'K'), m), o, st.put); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("boom")
	puts := 0
	failing := func(ctx context.Context, level int, seq uint64, data []byte) (string, error) {
		puts++
		if puts == 2 {
			return "", boom
		}
		return st.put(ctx, level, seq, data)
	}
	_, e, err := Compact(ctx, v, v.job(0, v.Levels[0], o), o, st.reader(), failing)
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "put level 1 seq") {
		t.Fatalf("error %v", err)
	}
	if len(e.Add[1]) != 1 || len(e.Del) != 0 || e.NextSeq != e.Add[1][0].Seq+2 {
		t.Fatalf("edit after failure %+v, want the one published file, no deletes, and NextSeq past the failed put", e)
	}
	// Applying the failed edit's floor makes a retry skip the consumed seqs.
	if next, err := v.Apply(Edit{NextSeq: e.NextSeq}); err != nil || next.NextSeq != e.NextSeq {
		t.Fatalf("NextSeq floor: %+v %v", next, err)
	}
	if _, ok := st.objects[e.Add[1][0].Key]; !ok {
		t.Fatalf("orphan %s not in the store", e.Add[1][0].Key)
	}
}

// TestPickByAge: with MaxTableAge set and no size rule firing, an aged file
// above the bottom level is picked (level 0 whole); a young file and an aged
// bottom-level file are not.
func TestPickByAge(t *testing.T) {
	o := Options{L0Trigger: 4, LevelRatio: 10, BaseBytes: 1 << 20, FileBytes: 1 << 20, MaxTableAge: time.Hour}
	old, young := time.Now().Add(-2*time.Hour).Unix(), time.Now().Unix()
	f := func(seq uint64, lo, hi string, oldest int64) FileRef {
		return FileRef{Key: fmt.Sprint("f", seq), Seq: seq, Oldest: oldest, TableMeta: TableMeta{Min: []byte(lo), Max: []byte(hi), Bytes: 10}}
	}
	v, _ := Version{}.Apply(Edit{Add: map[int][]FileRef{0: {f(1, "a", "c", young)}, 1: {f(2, "a", "c", young), f(3, "d", "f", old)}, 2: {f(4, "a", "z", old)}}})
	j, ok := Pick(v, o)
	if !ok || j.Level != 1 || len(j.Inputs) != 1 || j.Inputs[0].Key != "f3" {
		t.Fatalf("aged level-1 file not picked: %v %+v", ok, j)
	}
	v, _ = v.Apply(Edit{Add: map[int][]FileRef{0: {f(5, "x", "z", old)}}})
	if j, ok = Pick(v, o); !ok || j.Level != 0 || len(j.Inputs) != 2 {
		t.Fatalf("aged level-0 file did not take level 0 whole: %v %+v", ok, j)
	}
	v, _ = v.Apply(Edit{Del: []string{"f3", "f5"}})
	if j, ok = Pick(v, o); ok {
		t.Fatalf("young files and an aged bottom file picked: %+v", j)
	}
	o.MaxTableAge = 0
	v, _ = v.Apply(Edit{Add: map[int][]FileRef{1: {f(6, "d", "f", old)}}})
	if j, ok = Pick(v, o); ok {
		t.Fatalf("picked by age with MaxTableAge 0: %+v", j)
	}
}

// TestAgeCompactionPurgesDeletes: a tombstone in an aged level-0 file over a
// key two levels down is carried to the bottom by consecutive age picks, and
// the key is then in no table; every output of the last job is fresh. One
// key space (first byte), so each level holds one file.
func TestAgeCompactionPurgesDeletes(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	o := DefaultOptions()
	o.MaxTableAge = time.Hour
	old := time.Now().Add(-2 * time.Hour).Unix()
	var seq uint64
	mk := func(oldest int64, es ...Entry) FileRef {
		_, f, err := Flush(ctx, Version{NextSeq: seq}, es, o, st.put)
		if err != nil {
			t.Fatal(err)
		}
		seq++
		f.Oldest = oldest
		return f
	}
	put := func(k string) Entry { return Entry{Key: []byte(k), Kind: KindPut, Value: []byte("v")} }
	v, err := Version{}.Apply(Edit{Add: map[int][]FileRef{
		0: {mk(old, Entry{Key: []byte("ka"), Kind: KindDelete})},
		1: {mk(time.Now().Unix(), put("ka"), put("kb"))},
		2: {mk(time.Now().Unix(), put("ka"), put("kc"))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := Pick(v, Options{MaxTableAge: 0}); ok {
		t.Fatal("size rules fired; the test needs the age rule alone")
	}
	start := time.Now().Unix()
	var last Edit
	rounds := 0
	for {
		j, ok := Pick(v, o)
		if !ok {
			break
		}
		if rounds++; rounds > 3 {
			t.Fatalf("age compaction did not settle: %+v", v.Levels)
		}
		if v, last, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
			t.Fatal(err)
		}
	}
	if rounds != 2 {
		t.Fatalf("%d rounds, want 2 (level 0 → 1 keeping the age, 1 → 2 dropping it)", rounds)
	}
	for _, f := range slices.Concat(v.Levels...) {
		tb, err := st.open(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok, err := tb.Get(ctx, []byte("ka")); err != nil || ok {
			t.Fatalf("key ka still in %s: %v", f.Key, err)
		}
	}
	for _, fs := range last.Add {
		for _, f := range fs {
			if f.Oldest < start {
				t.Fatalf("bottom output %s is not fresh: oldest %d < %d", f.Key, f.Oldest, start)
			}
		}
	}
	if got, ok, _ := st.reader().Get(ctx, v, []byte("kb")); !ok || string(got) != "v" {
		t.Fatal("key kb lost")
	}
}
