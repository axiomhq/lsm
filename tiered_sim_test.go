package lsm

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

// The fold pattern of an ingest-heavy writer: every fold flushes 24
// level-0 files of 0.7 GB together, two per key space over 12 spaces,
// each spanning its space (random keys), every 35 s of fold work. One
// compaction runs at a time and reads its inputs at 270 MB/s. A fold
// waits while level 0 holds more than two folds' bytes and Pick owes a
// level-0 job (the writer's stall rule). No key is overwritten, so a
// compaction writes what it reads.
const (
	simFoldBytes = 700_000_000
	simFoldFiles = 24
	simSpaces    = 12
	simFoldSecs  = 35.0
	simReadRate  = 270e6
)

type simResult struct {
	ingested, read, written int64
	folds, stalls           int     // folds done; folds that waited on level 0
	stallSecs, secs         float64 // time folds waited; time to ingest every fold
	maxL0, maxRuns, depth   int     // level-0 files, levels holding one space, levels
	jobs                    int
	biggest                 int64 // bytes one job read
}

func simKey(space byte, pos uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{space}, pos)
}

func simPos(key []byte) uint64 { return binary.BigEndian.Uint64(key[1:]) }

// simFile is a file of space holding [lo, hi] of it.
func simFile(id *int, seq uint64, space byte, lo, hi uint64, n int64) FileRef {
	*id++
	min, max := simKey(space, lo), simKey(space, hi)
	return FileRef{Key: fmt.Sprint("f", *id), Seq: seq, TableMeta: TableMeta{Min: min, Max: max, Bytes: n,
		Spaces: []SpaceRange{{Space: space, Min: min, Max: max}}}}
}

// simCompact is Compact on sizes: per space, the inputs and the overlap
// files they touch become ceil(bytes / FileBytes) files evenly over the
// range they held, less the overlap files they leave in place.
func simCompact(v Version, j Job, o Options, id *int) (e Edit, read int64) {
	type span struct {
		n      int64
		lo, hi uint64
		ok     bool
	}
	var spaces [256]span
	add := func(f FileRef) {
		s := &spaces[f.Min[0]]
		lo, hi := simPos(f.Min), simPos(f.Max)
		if !s.ok || lo < s.lo {
			s.lo = lo
		}
		if !s.ok || hi > s.hi {
			s.hi = hi
		}
		s.n, s.ok = s.n+f.Bytes, true
		e.Del = append(e.Del, f.Key)
		read += f.Bytes
	}
	for _, f := range j.Inputs {
		add(f)
	}
	var stay [256][][2]uint64 // per space, the overlap files no input touches
	for i, f := range j.Overlap {
		if j.touches(i, o) {
			add(f)
		} else {
			stay[f.Min[0]] = append(stay[f.Min[0]], [2]uint64{simPos(f.Min), simPos(f.Max)})
		}
	}
	e.Add = map[int][]FileRef{}
	seq := v.NextSeq
	for b, s := range spaces {
		if !s.ok {
			continue
		}
		// The outputs cover the range the job held but the files it
		// leaves in place, bytes by width.
		var segs [][2]uint64
		lo, open := s.lo, true
		for _, r := range stay[b] {
			if !open || r[1] < lo || r[0] > s.hi {
				continue
			}
			if r[0] > lo {
				segs = append(segs, [2]uint64{lo, r[0] - 1})
			}
			if r[1] >= s.hi {
				open = false
			} else {
				lo = r[1] + 1
			}
		}
		if open {
			segs = append(segs, [2]uint64{lo, s.hi})
		}
		width := func(g [2]uint64) float64 { return float64(g[1]-g[0]) + 1 }
		var total float64
		for _, g := range segs {
			total += width(g)
		}
		for _, g := range segs {
			gn := int64(float64(s.n) * width(g) / total)
			n := max(1, int64(min(math.Ceil(float64(gn)/float64(o.FileBytes)), width(g))))
			w := (g[1] - g[0]) / uint64(n)
			for k := range n {
				lo, hi := g[0]+uint64(k)*w, g[0]+uint64(k+1)*w-1
				if k == n-1 {
					hi = g[1]
				}
				e.Add[j.Level+1] = append(e.Add[j.Level+1], simFile(id, seq, byte(b), lo, hi, gn/n))
				seq++
			}
		}
	}
	if j.NewRun {
		del, moved := v.shift()
		e.Del = append(e.Del, del...)
		for l, fs := range moved {
			e.Add[l] = fs
		}
	}
	return e, read
}

// simulate replays folds through Pick until every fold is in.
// With skew, the first space takes half of every fold's bytes.
func simulate(t *testing.T, o Options, folds int, skew bool) simResult {
	var r simResult
	var v Version
	id := 0
	now := 0.0
	foldEnd, jobEnd := -1.0, -1.0
	waiting := -1.0 // since when the next fold waits on level 0
	var snap Version
	var edit Edit
	l0Bytes := func() (n int64) {
		for _, f := range v.Levels[0] {
			n += f.Bytes
		}
		return n
	}
	stalled := func() bool {
		if len(v.Levels) == 0 || l0Bytes() <= 2*simFoldBytes {
			return false
		}
		j, ok := Pick(v, o)
		return ok && j.Level == 0
	}
	for r.folds < folds {
		if jobEnd < 0 {
			if j, ok := Pick(v, o); ok {
				var in int64
				snap = v
				edit, in = simCompact(v, j, o, &id)
				jobEnd = now + float64(in)/simReadRate
				r.read += in
				r.written += in
				r.jobs++
				r.biggest = max(r.biggest, in)
			}
		}
		if foldEnd < 0 {
			switch {
			case !stalled():
				if waiting >= 0 {
					r.stallSecs += now - waiting
					waiting = -1
				}
				foldEnd = now + simFoldSecs
			case waiting < 0:
				waiting = now
				r.stalls++
			}
		}
		switch {
		case foldEnd >= 0 && (jobEnd < 0 || foldEnd <= jobEnd):
			now = foldEnd
			var files []FileRef
			for i := range simFoldFiles {
				n := int64(simFoldBytes / simFoldFiles)
				if skew {
					n = simFoldBytes / (2 * (2*simSpaces - 2)) // a weight of 1 in 2×(spaces-1) + 2×(spaces-1)
					if i%simSpaces == 0 {
						n *= simSpaces - 1
					}
				}
				files = append(files, simFile(&id, v.NextSeq+uint64(i), byte('A'+i%simSpaces), 0, math.MaxUint64, n))
			}
			var err error
			if v, err = v.Apply(Edit{Add: map[int][]FileRef{0: files}}); err != nil {
				t.Fatal(err)
			}
			r.folds++
			r.ingested += simFoldBytes
			r.maxL0 = max(r.maxL0, len(v.Levels[0]))
			foldEnd = -1
		case jobEnd >= 0:
			now = jobEnd
			var err error
			if v, err = v.Rebase(snap, edit); err != nil {
				t.Fatal(err)
			}
			jobEnd = -1
		default:
			t.Fatal("simulation stuck: no fold and no job")
		}
		var runs [256]int
		for l := 1; l < len(v.Levels); l++ {
			var held [256]bool
			for _, f := range v.Levels[l] {
				held[f.Min[0]] = true
			}
			for b, h := range held {
				if h {
					runs[b]++
					r.maxRuns = max(r.maxRuns, runs[b])
				}
			}
		}
		r.depth = max(r.depth, len(v.Levels))
	}
	r.secs = now
	return r
}

// TestFoldPatternSimulation replays 300 folds of the ingest-heavy pattern
// through the leveled and the tiered picker and reports what each costs:
// compaction bytes read and written per ingested byte, the deepest level
// 0, the folds that waited on it, and the ingest rate that leaves.
func TestFoldPatternSimulation(t *testing.T) {
	folds := 300
	if testing.Short() {
		folds = 60
	}
	leveledOpts := DefaultOptions()
	leveledOpts.Leveled = true
	res := map[string]simResult{}
	for _, c := range []struct {
		name string
		o    Options
		skew bool
	}{
		{"leveled", leveledOpts, false},
		{"tiered", DefaultOptions(), false},
		{"leveled, one space half the bytes", leveledOpts, true},
		{"tiered, one space half the bytes", DefaultOptions(), true},
	} {
		r := simulate(t, c.o, folds, c.skew)
		res[c.name] = r
		gb := func(n int64) float64 { return float64(n) / 1e9 }
		t.Logf("%s: folds %d, ingested %.0f GB, compaction read+written %.1f× the ingested bytes, write amp %.1f×, max level 0 %d files, max runs in a space %d, levels %d, stalled folds %d (%.0f s), jobs %d, biggest job %.1f GB read, ingest %.1f MB/s over %.0f s",
			c.name, r.folds, gb(r.ingested), float64(r.read+r.written)/float64(r.ingested),
			float64(r.ingested+r.written)/float64(r.ingested), r.maxL0, r.maxRuns, r.depth, r.stalls, r.stallSecs,
			r.jobs, gb(r.biggest), float64(r.ingested)/r.secs/1e6, r.secs)
	}
	for _, skew := range []string{"", ", one space half the bytes"} {
		lv, ti := res["leveled"+skew], res["tiered"+skew]
		// Leveled read+wrote 25x the ingested bytes, tiered 9.5x.
		if 2*(ti.read+ti.written) > lv.read+lv.written {
			t.Errorf("tiered%s: compaction I/O %d is not half of leveled %d", skew, ti.read+ti.written, lv.read+lv.written)
		}
		// Runs pass MaxRuns while level-0 jobs, which go first, land runs
		// faster than the merges behind them retire them.
		if ti.maxRuns > 2*DefaultOptions().MaxRuns || ti.stalls > lv.stalls {
			t.Errorf("tiered%s: %d runs in a space (MaxRuns %d), %d stalled folds (leveled %d)", skew, ti.maxRuns, DefaultOptions().MaxRuns, ti.stalls, lv.stalls)
		}
	}
}

// Shift is Version.shift, for the external simulation's NewRun jobs.
func Shift(v Version) ([]string, map[int][]FileRef) { return v.shift() }
