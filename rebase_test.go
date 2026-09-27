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
	for seed := uint64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 7))
			ctx := context.Background()
			st := newMemStore()
			m := model{}
			universe := randKeys(rng, 300, 'K')
			o := Options{L0Trigger: 2 + rng.IntN(3), LevelRatio: 3, BaseBytes: 20 << 10, FileBytes: 8 << 10, BlockBytes: 512}
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
