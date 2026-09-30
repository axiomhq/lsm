package lsm_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/axiomhq/lsm"
)

// TestFoldPatternWriteAmp replays a busy writer's shape through Pick, sizes
// only: every fold adds 0.7 GB of level-0 files, two per key space over
// 12 spaces whose keys land uniformly, while one compaction at a time runs
// at 270 MB/s read+written, and a fold waits while level 0 holds more than
// 2 GiB at its trigger. A job's output is its inputs' and overlap's bytes,
// cut into FileBytes files per space. It reports compaction bytes per
// ingested byte and the level-0 stalls, for the tiered and the leveled
// picker. LSM_SIM_FOLDS sets the folds (300).
func TestFoldPatternWriteAmp(t *testing.T) {
	if testing.Short() {
		t.Skip("simulation")
	}
	folds := 300
	if s := os.Getenv("LSM_SIM_FOLDS"); s != "" {
		folds, _ = strconv.Atoi(s)
	}
	for _, leveled := range []bool{false, true} {
		name := "tiered"
		if leveled {
			name = "leveled"
		}
		t.Run(name, func(t *testing.T) {
			o := lsm.DefaultOptions()
			o.Leveled = leveled
			foldWriteAmp(t, o, folds)
		})
	}
}

func foldWriteAmp(t *testing.T, o lsm.Options, folds int) {
	const (
		spaces     = 12
		perSpace   = 2
		foldBytes  = 0.7e9
		foldSec    = 35.0
		rate       = 270e6
		stallBytes = 2 << 30
	)
	rng := rand.New(rand.NewPCG(1, 2))
	var v lsm.Version
	var seq uint64
	file := func(space byte, lo, hi uint64, n int64) lsm.FileRef {
		seq++
		return lsm.FileRef{Key: fmt.Sprint("t", seq), Seq: seq, TableMeta: lsm.TableMeta{Min: simKey(space, lo), Max: simKey(space, hi), Bytes: n}}
	}
	var (
		now, stalled, foldEnd, jobEnd float64
		done, stalls                  int
		folding, running, waiting     bool
		snap                          lsm.Version
		edit                          lsm.Edit
		read, written                 float64
		maxL0Files                    int
		maxL0Bytes, l1Sum, l1Max      int64
		l1N                           int
		jobs                          = map[int]int{}
	)
	for done < folds {
		if !folding {
			n, b := len(level(v, 0)), bytesOf(level(v, 0))
			if b > stallBytes && n >= o.L0Trigger {
				if !waiting {
					stalls++
					waiting = true
				}
			} else {
				waiting, folding, foldEnd = false, true, now+foldSec
			}
		}
		if !running {
			if j, ok := lsm.Pick(v, o); ok {
				var in, out int64
				snap, edit, in, out = simCompact(v, j, o, file)
				read, written = read+float64(in), written+float64(out)
				running, jobEnd = true, now+float64(in+out)/rate
				jobs[j.Level]++
			}
		}
		if !folding && !running {
			t.Fatalf("fold %d: level 0 stalls with no job owed", done)
		}
		next := math.Inf(1)
		if folding {
			next = foldEnd
		}
		if running {
			next = min(next, jobEnd)
		}
		if waiting {
			stalled += next - now
		}
		now = next
		var err error
		if running && jobEnd == now {
			running = false
			if v, err = v.Rebase(snap, edit); err != nil {
				t.Fatal(err)
			}
		}
		if folding && foldEnd == now {
			folding = false
			var add []lsm.FileRef
			for s := range spaces {
				mid := math.MaxUint32/4 + rng.Uint64N(math.MaxUint32/2)
				n := int64(foldBytes) / spaces / perSpace
				add = append(add, file(byte('A'+s), 0, mid, n), file(byte('A'+s), mid+1, math.MaxUint32, n))
			}
			if v, err = v.Apply(lsm.Edit{Add: map[int][]lsm.FileRef{0: add}}); err != nil {
				t.Fatal(err)
			}
			done++
			l1 := bytesOf(level(v, 1))
			if done > folds/3 {
				l1Sum += l1
				l1N++
			}
			l1Max = max(l1Max, l1)
		}
		maxL0Files = max(maxL0Files, len(level(v, 0)))
		maxL0Bytes = max(maxL0Bytes, bytesOf(level(v, 0)))
	}
	ingested := float64(done) * foldBytes
	t.Logf("folds %d, %.0f GB ingested in %.0f s: %.0f MB/s, %.0f folds/h", done, ingested/1e9, now, ingested/now/1e6, float64(done)/now*3600)
	t.Logf("compaction written/ingested %.1fx, read+written/ingested %.1fx", written/ingested, (read+written)/ingested)
	t.Logf("level 0 max %d files %.1f GB; stalls %d, stalled %.0f s (%.0f%% of the run)", maxL0Files, float64(maxL0Bytes)/1e9, stalls, stalled, 100*stalled/now)
	t.Logf("level 1 mean %.2f GB (last two thirds), max %.2f GB; jobs by level %v", float64(l1Sum)/float64(max(l1N, 1))/1e9, float64(l1Max)/1e9, jobs)
	for l := range v.Levels {
		t.Logf("level %d: %d files %.1f GB", l, len(v.Levels[l]), float64(bytesOf(v.Levels[l]))/1e9)
	}
	// Leveled at ratio 10 over four or five levels is under 30x; v0.8.1,
	// which always picked level 0 first, wrote 64x here and growing.
	// Tiered merges a byte about log2(space bytes / level-0 bytes) times.
	if wa := written / ingested; wa > 30 {
		t.Errorf("compaction wrote %.1fx the ingested bytes", wa)
	}
}

func simKey(space byte, pos uint64) []byte {
	return binary.BigEndian.AppendUint32([]byte{space}, uint32(pos))
}

func level(v lsm.Version, l int) []lsm.FileRef {
	if l < len(v.Levels) {
		return v.Levels[l]
	}
	return nil
}

func bytesOf(files []lsm.FileRef) (n int64) {
	for _, f := range files {
		n += f.Bytes
	}
	return n
}

// simCompact is Compact by sizes: each input's bytes spread evenly over its
// key range, summed, and cut into files of about FileBytes per space.
func simCompact(v lsm.Version, j lsm.Job, o lsm.Options, file func(byte, uint64, uint64, int64) lsm.FileRef) (lsm.Version, lsm.Edit, int64, int64) {
	all := slices.Concat(j.Inputs, j.Overlap)
	e := lsm.Edit{Add: map[int][]lsm.FileRef{}}
	bySpace := map[byte][]lsm.FileRef{}
	var in int64
	for _, f := range all {
		e.Del = append(e.Del, f.Key)
		in += f.Bytes
		bySpace[f.Min[0]] = append(bySpace[f.Min[0]], f)
	}
	var out int64
	for space, fs := range bySpace {
		var cuts []uint64
		for _, f := range fs {
			cuts = append(cuts, pos(f.Min), pos(f.Max)+1)
		}
		slices.Sort(cuts)
		cuts = slices.Compact(cuts)
		start, acc := uint64(math.MaxUint64), 0.0
		emit := func(hi uint64) {
			if start != math.MaxUint64 && acc > 0 {
				e.Add[j.Level+1] = append(e.Add[j.Level+1], file(space, start, hi, int64(acc)))
				out += int64(acc)
			}
			start, acc = math.MaxUint64, 0
		}
		for k := 0; k+1 < len(cuts); k++ {
			lo, hi := cuts[k], cuts[k+1] // [lo, hi)
			var d float64
			for _, f := range fs {
				if pos(f.Min) <= lo && hi-1 <= pos(f.Max) {
					d += float64(f.Bytes) / float64(pos(f.Max)-pos(f.Min)+1)
				}
			}
			if d == 0 {
				emit(lo - 1)
				continue
			}
			for lo < hi {
				if start == math.MaxUint64 {
					start = lo
				}
				need := uint64(math.Ceil((float64(o.FileBytes) - acc) / d))
				if need >= hi-lo {
					acc += d * float64(hi-lo)
					lo = hi
					break
				}
				acc += d * float64(need)
				lo += need
				emit(lo - 1)
			}
		}
		emit(cuts[len(cuts)-1] - 1)
	}
	if j.NewRun {
		del, moved := lsm.Shift(v)
		e.Del = append(e.Del, del...)
		for l, fs := range moved {
			e.Add[l] = fs
		}
	}
	if _, err := v.Apply(e); err != nil {
		panic(err)
	}
	return v, e, in, out
}

func pos(k []byte) uint64 { return uint64(binary.BigEndian.Uint32(k[1:])) }
