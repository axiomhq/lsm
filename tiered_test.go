package lsm

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestTieredRunInvariants: random batches over three key spaces, flushed
// and compacted with the tiered picker until it owes nothing. After every
// job each level below 0 holds single-space files with disjoint ranges, a
// new run's edit adds its outputs to level 1 alone and moves the files it
// shifts under their own keys, and the version reads back like the model:
// the newest run's put, delete or operand wins over the runs beneath it.
func TestTieredRunInvariants(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 23))
			ctx := context.Background()
			st := newMemStore()
			m := model{}
			universe := slices.Concat(randKeys(rng, 150, 'A'), randKeys(rng, 150, 'B'), randKeys(rng, 150, 'C'))
			o := Options{L0Trigger: 2, MaxRuns: 3, FileBytes: 2 << 10, BlockBytes: 256}
			var v Version
			shifts, multi := 0, 0
			for batch := 0; batch < 50; batch++ {
				next, _, err := Flush(ctx, v, randBatch(rng, universe, m), o, st.put)
				if err != nil {
					t.Fatal(err)
				}
				v = next
				for {
					j, ok := Pick(v, o)
					if !ok {
						break
					}
					levels := map[int]bool{}
					for _, f := range j.Inputs {
						levels[slices.IndexFunc(v.Levels, func(l []FileRef) bool {
							return slices.ContainsFunc(l, func(g FileRef) bool { return g.Key == f.Key })
						})] = true
					}
					if !j.NewRun && len(levels) > 1 {
						multi++
					}
					prev := v
					next, e, err := Compact(ctx, v, j, o, st.reader(), st.put)
					if err != nil {
						t.Fatal(err)
					}
					v = next
					if j.NewRun {
						if j.Level != 0 || len(j.Overlap) != 0 {
							t.Fatalf("new run job %+v", j)
						}
						was := map[string]bool{}
						for _, f := range slices.Concat(prev.Levels...) {
							was[f.Key] = true
						}
						for _, f := range e.Add[1] {
							if was[f.Key] && !slices.ContainsFunc(prev.Levels[0], func(g FileRef) bool { return g.Key == f.Key }) {
								t.Fatalf("level-1 add %s is neither a new output nor a moved level-0 file", f.Key)
							}
						}
						for l, fs := range e.Add {
							for _, f := range fs {
								if l != 1 && !was[f.Key] {
									t.Fatalf("level-%d add %s is not a moved file", l, f.Key)
								}
							}
						}
						if len(e.Add) > 1 {
							shifts++
						}
						if !slices.EqualFunc(v.Levels[1], e.Add[1], func(a, b FileRef) bool { return a.Key == b.Key }) {
							t.Fatal("level 1 holds more than the new run")
						}
					}
					for l := 1; l < len(v.Levels); l++ {
						for i, f := range v.Levels[l] {
							if f.Min[0] != f.Max[0] {
								t.Fatalf("level %d file %s spans spaces %c..%c", l, f.Key, f.Min[0], f.Max[0])
							}
							if i > 0 && string(v.Levels[l][i-1].Max) >= string(f.Min) {
								t.Fatalf("level %d files overlap", l)
							}
						}
					}
					checkModel(t, ctx, v, st, m, rng, universe)
				}
			}
			if shifts == 0 || multi == 0 {
				t.Fatalf("%d shifting new runs, %d merges of more than one run: the test exercised neither", shifts, multi)
			}
		})
	}
}

// TestPickTiered: level 0 becomes a new run; the size rule merges a
// space's newest equal runs, the cheapest space first; the count rule
// merges the cheapest adjacent window; a new run shifts each space it
// displaces down to that space's first free level.
func TestPickTiered(t *testing.T) {
	o := Options{L0Trigger: 2, MaxRuns: 2, FileBytes: 1 << 20}
	f := func(seq uint64, lo, hi string, n int64) FileRef {
		return FileRef{Key: fmt.Sprint("f", seq), Seq: seq, TableMeta: TableMeta{Min: []byte(lo), Max: []byte(hi), Bytes: n}}
	}
	// Space a: runs of 10, 10 at levels 1, 2; space b: 5, 5 at levels 1, 3.
	v, err := Version{}.Apply(Edit{Add: map[int][]FileRef{
		1: {f(1, "a1", "a3", 10), f(2, "b1", "b2", 5)},
		2: {f(3, "a0", "a9", 10), f(4, "c0", "c9", 50)},
		3: {f(5, "b0", "b9", 5)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	j, ok := Pick(v, o)
	if !ok || j.NewRun || j.Level != 2 || len(j.Inputs) != 1 || j.Inputs[0].Key != "f2" || len(j.Overlap) != 1 || j.Overlap[0].Key != "f5" || !j.Bottom {
		t.Fatalf("size job %+v, want space b (10 bytes) before space a (20)", j)
	}
	// A new run moves a from 1 → 2 and 2 → 3, b from 1 → 2 (level 2 holds
	// no b); c stays.
	v2, err := v.Apply(Edit{Add: map[int][]FileRef{0: {f(6, "a5", "b5", 1), f(7, "a2", "c2", 1)}}})
	if err != nil {
		t.Fatal(err)
	}
	if j, ok = Pick(v2, o); !ok || !j.NewRun || j.Level != 0 || len(j.Inputs) != 2 || j.Bottom {
		t.Fatalf("new run job %+v", j)
	}
	del, add := v2.shift()
	moved := map[string]int{}
	for l, fs := range add {
		for _, f := range fs {
			moved[f.Key] = l
		}
	}
	want := map[string]int{"f1": 2, "f2": 2, "f3": 3}
	if len(del) != 3 || fmt.Sprint(moved) != fmt.Sprint(want) {
		t.Fatalf("shift del %v add %v, want %v", del, moved, want)
	}
	if _, err := v2.Apply(Edit{Del: del, Add: add}); err != nil {
		t.Fatal(err)
	}
	// Count: space c alone, runs 20 30 45 80 (newest first), each bigger
	// than the one before, so no size window: past MaxRuns 2, the three
	// adjacent runs with the fewest bytes (20 30 45) merge into the oldest
	// of them.
	v3, _ := Version{}.Apply(Edit{Add: map[int][]FileRef{
		1: {f(1, "c0", "c1", 20)}, 2: {f(2, "c0", "c1", 30)}, 3: {f(3, "c0", "c1", 45)}, 4: {f(4, "c0", "c1", 80)},
	}})
	if j, ok = Pick(v3, o); !ok || j.Level != 2 || len(j.Inputs) != 2 || j.Inputs[0].Key != "f1" || j.Inputs[1].Key != "f2" || j.Overlap[0].Key != "f3" || j.Bottom {
		t.Fatalf("count job %+v", j)
	}
	o.MaxRuns = 4
	if j, ok = Pick(v3, o); ok {
		t.Fatalf("picked %+v with the runs within MaxRuns and no size window", j)
	}
}

// TestTieredAgeCarriesDeletesToBottom: a tombstone in an aged file over a
// key held by the two runs beneath reaches the bottom in one job, level 0
// (merging every level into the deepest) or a run (merging it and the
// older runs into the oldest): the key is then in no table, the other keys
// read back, and the job's outputs are fresh.
func TestTieredAgeCarriesDeletesToBottom(t *testing.T) {
	for _, at := range []int{0, 1} {
		t.Run(fmt.Sprint("level", at), func(t *testing.T) {
			ctx := context.Background()
			st := newMemStore()
			o := DefaultOptions()
			o.MaxTableAge = time.Hour
			old, now := time.Now().Add(-2*time.Hour).Unix(), time.Now().Unix()
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
			// Runs of 2 and 3 entries under the tombstone: no size window
			// (each run is bigger than the one above), within MaxRuns.
			v, err := Version{}.Apply(Edit{Add: map[int][]FileRef{
				at:     {mk(old, Entry{Key: []byte("ka"), Kind: KindDelete})},
				at + 1: {mk(now, put("ka"), put("kd"))},
				at + 2: {mk(now, put("ka"), put("kb"), put("kc"), put("ke"), put("kf"))},
			}})
			if err != nil {
				t.Fatal(err)
			}
			young := o
			young.MaxTableAge = 0
			if j, ok := Pick(v, young); ok {
				t.Fatalf("size rules owe %+v; the test needs the age rule alone", j)
			}
			start := time.Now().Unix()
			j, ok := Pick(v, o)
			if !ok || !j.Bottom || j.Level != at+1 {
				t.Fatalf("age job %+v, want a bottom job into level %d", j, at+2)
			}
			var last Edit
			if v, last, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
				t.Fatal(err)
			}
			if j, ok := Pick(v, o); ok {
				t.Fatalf("a second job owed after the age job: %+v", j)
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
			for _, f := range last.Add[j.Level+1] {
				if f.Oldest < start {
					t.Fatalf("bottom output %s is not fresh: oldest %d < %d", f.Key, f.Oldest, start)
				}
			}
			for _, k := range []string{"kb", "kc", "kd"} {
				if got, ok, _ := st.reader().Get(ctx, v, []byte(k)); !ok || string(got) != "v" {
					t.Fatalf("key %s lost", k)
				}
			}
		})
	}
}

// TestPickWriteOnce: a write-once space skips the size rule's pairs,
// where a tiered space with the same runs merges, merges Fanout runs of
// about one size, and merges by count past its own MaxRuns.
func TestPickWriteOnce(t *testing.T) {
	f := func(seq uint64, lo, hi string, n int64) FileRef {
		return FileRef{Key: fmt.Sprint("f", seq), Seq: seq, TableMeta: TableMeta{Min: []byte(lo), Max: []byte(hi), Bytes: n, Count: 1}}
	}
	o := Options{L0Trigger: 2, MaxRuns: 2, FileBytes: 1 << 20, Spaces: map[byte]SpacePolicy{'a': {WriteOnce: true, Fanout: 3, MaxRuns: 3}}}
	v, _ := Version{}.Apply(Edit{Add: map[int][]FileRef{1: {f(1, "a1", "a3", 10)}, 2: {f(2, "a4", "a6", 10)}}})
	if j, ok := Pick(v, o); ok {
		t.Fatalf("write-once space picked %+v by size", j)
	}
	if j, ok := Pick(v, Options{L0Trigger: 2, MaxRuns: 2, FileBytes: 1 << 20}); !ok || j.Level != 1 {
		t.Fatalf("tiered space: %+v, want the size merge", j)
	}
	// Fanout: three runs within twice the newest merge into the oldest.
	v3, _ := v.Apply(Edit{Add: map[int][]FileRef{3: {f(3, "a7", "a8", 20)}, 4: {f(4, "a0", "a0", 100)}}})
	if j, ok := Pick(v3, o); !ok || j.Level != 2 || len(j.Inputs) != 2 {
		t.Fatalf("fanout job %+v, want runs 1-3 merged into run 3", j)
	}
	// Count: runs 10 30 90 270, none within twice another, past MaxRuns 3:
	// the cheapest adjacent pair merges.
	v4, _ := Version{}.Apply(Edit{Add: map[int][]FileRef{
		1: {f(1, "a1", "a3", 10)}, 2: {f(2, "a4", "a6", 30)}, 3: {f(3, "a7", "a8", 90)}, 4: {f(4, "a0", "a0", 270)},
	}})
	if j, ok := Pick(v4, o); !ok || j.Level != 1 || len(j.Inputs) != 1 || j.Inputs[0].Key != "f1" {
		t.Fatalf("write-once count job %+v, want run 1 merged into run 2", j)
	}
	o.Spaces['a'] = SpacePolicy{WriteOnce: true, Fanout: 3, MaxRuns: 4}
	if j, ok := Pick(v4, o); ok {
		t.Fatalf("write-once space at its MaxRuns picked %+v", j)
	}
}

// TestWriteOnceChurnReclaims: new keys over flushes become runs that never
// merge; tombstones under DeadRatio of the puts leave them be, and past it
// every run merges into the oldest, into the bottom: the deleted keys are
// in no table, the rest read back, and no tombstone is left.
func TestWriteOnceChurnReclaims(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	const dead = 0.25
	o := Options{L0Trigger: 1, Spaces: map[byte]SpacePolicy{'F': {WriteOnce: true, MaxRuns: 64, DeadRatio: dead}}}
	key := func(i int) []byte { return fmt.Appendf(nil, "F%04d", i) }
	var v Version
	jobs := 0
	flush := func(es []Entry) {
		var err error
		if v, _, err = Flush(ctx, v, es, o, st.put); err != nil {
			t.Fatal(err)
		}
		for {
			j, ok := Pick(v, o)
			if !ok {
				return
			}
			if !j.NewRun {
				jobs++
			}
			if v, _, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
				t.Fatal(err)
			}
		}
	}
	for b := range 4 {
		var es []Entry
		for i := range 10 {
			es = append(es, Entry{Key: key(b*10 + i), Kind: KindPut, Value: []byte("v")})
		}
		flush(es)
	}
	del := func(lo, hi int) {
		var es []Entry
		for i := lo; i < hi; i++ {
			es = append(es, Entry{Key: key(i), Kind: KindDelete})
		}
		flush(es)
	}
	under := int(math.Ceil(dead*40)) - 1
	del(0, under) // just under DeadRatio of the 40 puts
	if jobs != 0 || len(v.Levels) != 6 {
		t.Fatalf("%d merges, %d levels: write-once runs merged under DeadRatio", jobs, len(v.Levels))
	}
	del(under, under+1)
	if jobs != 1 {
		t.Fatalf("%d merges past DeadRatio, want 1", jobs)
	}
	var files []FileRef
	for _, l := range v.Levels {
		files = append(files, l...)
	}
	var count, deletes int64
	for _, f := range files {
		count, deletes = count+f.Count, deletes+f.Deletes
	}
	if count != int64(40-under-1) || deletes != 0 {
		t.Fatalf("after the merge: %d entries, %d tombstones; want %d puts", count, deletes, 40-under-1)
	}
	for i := range 40 {
		_, ok, err := st.reader().Get(ctx, v, key(i))
		if err != nil || ok != (i > under) {
			t.Fatalf("key %d: found %v, %v", i, ok, err)
		}
	}
}

// TestNewRunMovesSortedRun: a new run whose level-0 files hold one space
// each, without overlap, moves them to level 1 without reading or writing
// any; a space whose files overlap still merges. Reads see the same data.
func TestNewRunMovesSortedRun(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	o := Options{L0Trigger: 4, FileBytes: 1 << 20, BlockBytes: 256}
	entry := func(space byte, i int) Entry {
		return Entry{Key: []byte(fmt.Sprintf("%c%04d", space, i)), Kind: KindPut, Value: []byte(fmt.Sprint(i))}
	}
	var v Version
	// Space A: two disjoint files. Space B: two overlapping files.
	for _, batch := range [][]Entry{
		{entry('A', 0), entry('A', 1)}, {entry('A', 2), entry('A', 3)},
		{entry('B', 0), entry('B', 2)}, {entry('B', 1), entry('B', 3)},
	} {
		next, _, err := Flush(ctx, v, batch, o, st.put)
		if err != nil {
			t.Fatal(err)
		}
		v = next
	}
	j, ok := Pick(v, o)
	if !ok || !j.NewRun {
		t.Fatalf("want a new run, got %+v", j)
	}
	puts := st.puts
	next, e, err := Compact(ctx, v, j, o, st.reader(), st.put)
	if err != nil {
		t.Fatal(err)
	}
	if st.puts-puts != 1 {
		t.Fatalf("%d tables written, want 1 (space B's merge)", st.puts-puts)
	}
	moved := 0
	for _, f := range e.Add[1] {
		if slices.ContainsFunc(v.Levels[0], func(g FileRef) bool { return g.Key == f.Key }) {
			moved++
			if f.Min[0] != 'A' {
				t.Fatalf("moved a file of space %c", f.Min[0])
			}
		}
	}
	if moved != 2 || len(next.Levels[0]) != 0 {
		t.Fatalf("moved %d files, level 0 holds %d", moved, len(next.Levels[0]))
	}
	for _, space := range []byte{'A', 'B'} {
		for i := range 4 {
			want := entry(space, i)
			got, ok, err := st.reader().Get(ctx, next, want.Key)
			if err != nil || !ok || string(got) != string(want.Value) {
				t.Fatalf("%s: got %q %v %v", want.Key, got, ok, err)
			}
		}
	}
}

// TestPlacedExtentsReadBack: flushes and the merges the tiered picker
// runs over them (several runs merged into one, Workers at once, put whole
// or streamed) tell
// SpacePolicy.Placed where every entry of a reporting space lands. In
// every table of the final version, a flushed one too, each reported
// extent read alone decodes (DecodeBlocks) to exactly the entries reported
// in it, with the table's values (in level 0, other spaces' entries may
// share a block); every entry of a reporting space is reported, at the
// block the index puts it in; a group's blocks are one contiguous run
// holding no other group; and a space without Placed reports nothing.
func TestPlacedExtentsReadBack(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", stream), func(t *testing.T) {
			type placement struct {
				key       string
				off, size int64
			}
			ctx := context.Background()
			rng := rand.New(rand.NewPCG(4, 4))
			st := newMemStore()
			var mu sync.Mutex
			placed := map[uint64][]placement{} // by table seq
			report := func(seq uint64, off, length int64, keys [][]byte) {
				mu.Lock()
				defer mu.Unlock()
				for _, k := range keys {
					placed[seq] = append(placed[seq], placement{string(k), off, length})
				}
			}
			group := func(key []byte) int { return 3 }
			o := Options{L0Trigger: 4, MaxRuns: 2, FileBytes: 64 << 10, BlockBytes: 1024, Workers: 4, Spaces: map[byte]SpacePolicy{
				'V': {Raw: true, Placed: report},
				'P': {Group: group, Placed: report},
			}}
			if stream {
				o.Stream = streaming(st.put)
			}
			vKeys, pKeys, xKeys := randKeys(rng, 300, 'V'), randKeys(rng, 600, 'P'), randKeys(rng, 200, 'X')
			for _, k := range pKeys { // 20 groups
				k[1], k[2] = 0, byte(rng.IntN(20))
			}
			slices.SortFunc(pKeys, bytes.Compare)
			pKeys = slices.CompactFunc(pKeys, bytes.Equal)
			value := func(key []byte) []byte {
				if key[0] == 'V' && rng.IntN(3) == 0 {
					v := make([]byte, LargeValueBytes+rng.IntN(512))
					for i := range v {
						v[i] = byte(rng.IntN(256))
					}
					return v
				}
				return fmt.Appendf(nil, "%x-%d", key, rng.IntN(1000))
			}
			var v Version
			merges := 0
			for round := range 25 {
				var batch []Entry
				for _, keys := range [][][]byte{vKeys, pKeys, xKeys} {
					for _, k := range keys {
						switch r := rng.IntN(10); {
						case r == 0:
							batch = append(batch, Entry{Key: k, Kind: KindDelete})
						case r < 4:
							batch = append(batch, Entry{Key: k, Kind: KindPut, Value: value(k)})
						}
					}
				}
				slices.SortFunc(batch, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
				var err error
				if v, _, err = Flush(ctx, v, batch, o, st.put); err != nil {
					t.Fatal(err)
				}
				for round < 24 { // the last flush stays in level 0

					j, ok := Pick(v, o)
					if !ok {
						break
					}
					if !j.NewRun {
						merges++
					}
					if v, _, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
						t.Fatal(err)
					}
				}
			}
			if merges == 0 {
				t.Fatal("no merge of runs")
			}
			tables, stored, shared := 0, 0, 0
			for _, level := range v.Levels {
				for _, f := range level {
					tables++
					data := st.objects[f.Key]
					tb, err := OpenTableAt(ctx, BytesSource(data), f.TableMeta)
					if err != nil {
						t.Fatal(err)
					}
					reps := placed[f.Seq]
					at := map[string]placement{}
					byExtent := map[[2]int64][]string{}
					for _, p := range reps {
						if _, dup := at[p.key]; dup {
							t.Fatalf("%s: %x reported twice", f.Key, p.key)
						}
						at[p.key] = p
						byExtent[[2]int64{p.off, p.size}] = append(byExtent[[2]int64{p.off, p.size}], p.key)
					}
					for ext, keys := range byExtent {
						all, err := DecodeBlocks(data[ext[0] : ext[0]+ext[1]])
						if err != nil {
							t.Fatalf("%s: extent %v: %v", f.Key, ext, err)
						}
						// A level-0 block may hold other spaces' entries too.
						got := slices.DeleteFunc(all, func(e Entry) bool { return e.Key[0] == 'X' })
						if got[0].Key[0] == 'P' && len(got) != len(all) {
							t.Fatalf("%s: extent %v holds group %x and another space", f.Key, ext, got[0].Key[:3])
						}
						if len(got) != len(keys) {
							t.Fatalf("%s: extent %v holds %d entries, reported %d", f.Key, ext, len(got), len(keys))
						}
						if data[ext[0]+4] == blockStored && len(got) == 1 && len(got[0].Value) >= LargeValueBytes {
							stored++
						} else if len(got) > 1 {
							shared++
						}
						if got[0].Key[0] == 'P' && !bytes.Equal(got[0].Key[:3], got[len(got)-1].Key[:3]) {
							t.Fatalf("%s: extent %v holds groups %x and %x", f.Key, ext, got[0].Key[:3], got[len(got)-1].Key[:3])
						}
						for i, e := range got {
							if string(e.Key) != keys[i] {
								t.Fatalf("%s: extent %v entry %d is %x, reported %x", f.Key, ext, i, e.Key, keys[i])
							}
							want, ok, err := tb.Get(ctx, e.Key)
							if !ok || err != nil || want.Kind != e.Kind || !bytes.Equal(want.Value, e.Value) {
								t.Fatalf("%s: %x reads %v, table has %v %v %v", f.Key, e.Key, e, want, ok, err)
							}
						}
					}
					n := 0
					lastOf := map[string]int64{} // a group's extent end so far
					it := tb.Iter(ctx)
					for ok := it.SeekGE(nil); ok; ok = it.Next() {
						k := it.Key()
						p, reported := at[string(k)]
						if k[0] == 'X' {
							if reported {
								t.Fatalf("%s: %x of a space without Placed reported", f.Key, k)
							}
							continue
						}
						n++
						i := tb.blockFor(k)
						if off, size := tb.BlockExtent(i); !reported || p.off != off || p.size != size {
							t.Fatalf("%s: %x reported %v, its block is %d+%d", f.Key, k, p, off, size)
						}
						if k[0] == 'P' {
							g := string(k[:3])
							if end, seen := lastOf[g]; seen && p.off != end && p.off+p.size != end {
								t.Fatalf("%s: group %x's blocks are not contiguous", f.Key, g)
							}
							lastOf[g] = p.off + p.size
						}
					}
					if err := it.Err(); err != nil {
						t.Fatal(err)
					}
					if n != len(reps) {
						t.Fatalf("%s: %d entries of reporting spaces, %d reported", f.Key, n, len(reps))
					}
				}
			}
			if tables < 3 || stored == 0 || shared == 0 {
				t.Fatalf("%d tables, %d stored large values, %d shared blocks", tables, stored, shared)
			}
			t.Logf("%d tables, %d merges, %d stored large values, %d shared blocks", tables, merges, stored, shared)
		})
	}
}
