package lsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/axiomhq/lsm/setmerge"
)

// randBatch is one flush's entries over universe, sorted and unique, with
// the model advanced by them.
func randBatch(rng *rand.Rand, universe [][]byte, m model) []Entry {
	byKey := map[string]Entry{}
	for range 20 + rng.IntN(60) {
		k := universe[rng.IntN(len(universe))]
		var e Entry
		switch rng.IntN(10) {
		case 0, 1:
			e = Entry{Key: k, Kind: KindPut, Value: setmerge.Value(set(uint32(rng.IntN(50))))}
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
		byKey[string(k)] = e
	}
	var entries []Entry
	for _, e := range byKey {
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
	for _, e := range entries {
		m.apply(e)
	}
	return entries
}

// TestRebaseOverFlushesMatchesModel: a compaction computed on a snapshot
// and rebased over the flushes published while it ran reads back exactly
// like the model, at every step and after everything is compacted.
func TestRebaseOverFlushesMatchesModel(t *testing.T) {
	for _, leveled := range []bool{false, true} {
		for seed := uint64(1); seed <= 12; seed++ {
			t.Run(fmt.Sprintf("%d/leveled=%v", seed, leveled), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, 7))
				ctx := context.Background()
				st := newMemStore()
				m := model{}
				universe := slices.Concat(randKeys(rng, 100, 'J'), randKeys(rng, 100, 'K'), randKeys(rng, 100, 'L'))
				o := Options{L0Trigger: 2 + rng.IntN(3), LevelRatio: 3, BaseBytes: 20 << 10, FileBytes: 8 << 10, BlockBytes: 512}
				o.Leveled = leveled
				var v Version
				rebased := 0
				for batch := 0; batch < 40; batch++ {
					next, _, err := Flush(ctx, v, randBatch(rng, universe, m), o, st.put)
					if err != nil {
						t.Fatal(err)
					}
					v = next
					j, ok := Pick(v, o)
					if !ok {
						continue
					}
					snap := v
					_, e, err := Compact(ctx, snap, j, o, st.reader(), st.put)
					if err != nil {
						t.Fatal(err)
					}
					// Flushes publish while the compaction runs.
					for range rng.IntN(3) {
						if v, _, err = Flush(ctx, v, randBatch(rng, universe, m), o, st.put); err != nil {
							t.Fatal(err)
						}
					}
					if v, err = v.Rebase(snap, e); err != nil {
						t.Fatalf("batch %d: %v", batch, err)
					}
					rebased++
					checkModel(t, ctx, v, st, m, rng, universe)
				}
				if rebased == 0 {
					t.Fatal("no compaction ran")
				}
			})
		}
	}
}

// TestRebaseMergeOverNewRunsMatchesModel: a tiered merge planned on a
// snapshot rebases over the new runs (Job.NewRun) published while it ran,
// which shift its runs deeper, and reads back exactly like the model.
func TestRebaseMergeOverNewRunsMatchesModel(t *testing.T) {
	for seed := uint64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 13))
			ctx := context.Background()
			st := newMemStore()
			m := model{}
			universe := slices.Concat(randKeys(rng, 100, 'J'), randKeys(rng, 100, 'K'), randKeys(rng, 100, 'L'))
			o := Options{L0Trigger: 2 + rng.IntN(2), FileBytes: 4 << 10, BlockBytes: 512}
			var v Version
			flush := func() {
				t.Helper()
				var err error
				if v, _, err = Flush(ctx, v, randBatch(rng, universe, m), o, st.put); err != nil {
					t.Fatal(err)
				}
			}
			compact := func(snap Version, j Job) Edit {
				t.Helper()
				_, e, err := Compact(ctx, snap, j, o, st.reader(), st.put)
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			shifted := 0
			for round := 0; round < 60; round++ {
				flush()
				j, ok := Pick(v, o)
				if !ok {
					continue
				}
				if j.NewRun {
					var err error
					if v, err = v.Rebase(v, compact(v, j)); err != nil {
						t.Fatal(err)
					}
					continue
				}
				snap, merge := v, compact(v, j)
				// New runs publish while the merge runs.
				runs := 0
				for range 1 + rng.IntN(3) {
					for len(v.Levels[0]) < o.L0Trigger {
						flush()
					}
					nr, _ := Pick(v, o)
					if !nr.NewRun {
						t.Fatalf("round %d: level 0 at the trigger picked %+v", round, nr)
					}
					base, e := v, compact(v, nr)
					var err error
					if v, err = v.Rebase(base, e); err != nil {
						t.Fatal(err)
					}
					runs++
				}
				var err error
				if v, err = v.Rebase(snap, merge); err != nil {
					t.Fatalf("round %d: merge over %d new runs: %v", round, runs, err)
				}
				shifted++
				checkModel(t, ctx, v, st, m, rng, universe)
			}
			if shifted == 0 {
				t.Fatal("no merge ran beside a new run")
			}
			for {
				j, ok := Pick(v, o)
				if !ok {
					break
				}
				var err error
				if v, err = v.Rebase(v, compact(v, j)); err != nil {
					t.Fatal(err)
				}
			}
			checkModel(t, ctx, v, st, m, rng, universe)
		})
	}
}

// TestRebaseRefusesANewRunOverAMerge: a new run planned on a snapshot is
// stale once a merge of runs it would shift has published.
func TestRebaseRefusesANewRunOverAMerge(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(5, 5))
	st := newMemStore()
	m := model{}
	universe := randKeys(rng, 300, 'K')
	o := Options{L0Trigger: 2, FileBytes: 4 << 10, BlockBytes: 512}
	var v Version
	var err error
	for tries := 0; ; tries++ {
		if tries == 200 {
			t.Fatal("setup: never a merge owed with level 0 at the trigger")
		}
		if v, _, err = Flush(ctx, v, randBatch(rng, universe, m), o, st.put); err != nil {
			t.Fatal(err)
		}
		if len(v.Levels[0]) >= o.L0Trigger {
			hold := v.Levels[0]
			v.Levels[0] = nil
			j, ok := Pick(v, o)
			v.Levels[0] = hold
			if ok && !j.NewRun {
				break
			}
		}
		// Only new runs: the merges they come to owe stay owed.
		if j, ok := Pick(v, o); ok && j.NewRun {
			if v, _, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
				t.Fatal(err)
			}
		}
	}
	snap := v
	nr, _ := Pick(snap, o)
	_, en, err := Compact(ctx, snap, nr, o, st.reader(), st.put)
	if err != nil {
		t.Fatal(err)
	}
	hold := snap.Levels[0]
	snap.Levels[0] = nil
	mj, _ := Pick(snap, o)
	snap.Levels[0] = hold
	_, em, err := Compact(ctx, snap, mj, o, st.reader(), st.put)
	if err != nil {
		t.Fatal(err)
	}
	head, err := v.Rebase(snap, em)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := head.Rebase(snap, en); !errors.Is(err, ErrStale) {
		t.Fatalf("a new run rebased over a merge: %v", err)
	}
	checkModel(t, ctx, head, st, m, rng, universe)
}

// TestRebaseRefusesAConcurrentCompaction: two compactions planned on one
// snapshot; the second to publish is stale, whichever it is.
func TestRebaseRefusesAConcurrentCompaction(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(3, 3))
	st := newMemStore()
	m := model{}
	universe := randKeys(rng, 300, 'K')
	o := Options{L0Trigger: 3, LevelRatio: 3, BaseBytes: 4 << 10, FileBytes: 2 << 10, BlockBytes: 512}
	var v Version
	var err error
	// Build levels 0 and 1 and 2.
	for range 12 {
		if v, _, err = Flush(ctx, v, randBatch(rng, universe, m), o, st.put); err != nil {
			t.Fatal(err)
		}
		if j, ok := Pick(v, o); ok {
			if v, _, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(v.Levels) < 2 || len(v.Levels[1]) == 0 {
		t.Fatalf("setup: levels %d", len(v.Levels))
	}
	if v, _, err = Flush(ctx, v, randBatch(rng, universe, m), o, st.put); err != nil {
		t.Fatal(err)
	}
	snap := v
	// Two jobs on snap: all of level 0, and one level-1 file down.
	l0 := snap.job(0, snap.Levels[0], o)
	l1 := snap.job(1, snap.Levels[1][:1], o)
	_, ea, err := Compact(ctx, snap, l0, o, st.reader(), st.put)
	if err != nil {
		t.Fatal(err)
	}
	_, eb, err := Compact(ctx, snap, l1, o, st.reader(), st.put)
	if err != nil {
		t.Fatal(err)
	}
	head, err := v.Rebase(snap, ea)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := head.Rebase(snap, eb); !errors.Is(err, ErrStale) {
		t.Fatalf("second compaction rebased: %v", err)
	}
	// The same job twice: its inputs are gone.
	if _, err := head.Rebase(snap, ea); !errors.Is(err, ErrStale) {
		t.Fatalf("replayed edit rebased: %v", err)
	}
	// An input that moved level (here: a level-0 input named at level 1).
	moved := Edit{Del: []string{snap.Levels[0][0].Key}}
	lvl := Version{Levels: [][]FileRef{nil, {snap.Levels[0][0]}}}
	if _, err := lvl.Rebase(snap, moved); !errors.Is(err, ErrStale) {
		t.Fatalf("moved input rebased: %v", err)
	}
	if len(ea.Add[1]) == 0 || len(eb.Del) == 0 {
		t.Fatalf("setup: jobs wrote %d and deleted %d", len(ea.Add[1]), len(eb.Del))
	}
	checkModel(t, ctx, head, st, m, rng, universe)
}

// TestManyFilesPerLevelMatchesModel: with small files every level below 0
// holds many, so a point get and a range pick one file per level by
// binary search; overwrites, tombstones and operands spread across the
// levels still read back like the model.
func TestManyFilesPerLevelMatchesModel(t *testing.T) {
	for _, leveled := range []bool{false, true} {
		for seed := uint64(1); seed <= 8; seed++ {
			t.Run(fmt.Sprintf("%d/leveled=%v", seed, leveled), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, 11))
				ctx := context.Background()
				st := newMemStore()
				m := model{}
				universe := randKeys(rng, 600, 'K')
				o := Options{L0Trigger: 3, LevelRatio: 4, BaseBytes: 4 << 10, FileBytes: 1 << 10, BlockBytes: 256}
				o.Leveled = leveled
				var v Version
				most := 0
				for batch := 0; batch < 60; batch++ {
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
						if v, _, err = Compact(ctx, v, j, o, st.reader(), st.put); err != nil {
							t.Fatal(err)
						}
					}
					checkModel(t, ctx, v, st, m, rng, universe)
					deep := 0
					for l := 1; l < len(v.Levels); l++ {
						if len(v.Levels[l]) >= 4 {
							deep++
						}
					}
					most = max(most, deep)
				}
				if most < 2 {
					t.Fatalf("at most %d levels held 4+ files", most)
				}
			})
		}
	}
}
