package lsm

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Options sizes the levels.
type Options struct {
	L0Trigger  int   // L0 files that start an L0→L1 compaction
	LevelRatio int64 // level n+1 holds LevelRatio × level n
	BaseBytes  int64 // level 1 target bytes
	FileBytes  int64 // output file target
	BlockBytes int
	// Workers bounds the partitions one compaction merges at once: a job
	// is split by its overlap files' key ranges, and each range is an
	// independent merge (0: half the CPUs, at least one).
	Workers int
}

// DefaultOptions: 4 L0 files, ratio 10, 256 MiB L1, 48 MiB files.
func DefaultOptions() Options {
	return Options{L0Trigger: 4, LevelRatio: 10, BaseBytes: 256 << 20, FileBytes: 48 << 20, BlockBytes: DefaultBlockBytes}
}

func (o Options) workers() int {
	if o.Workers > 0 {
		return o.Workers
	}
	return max(1, runtime.GOMAXPROCS(0)/2)
}

func (o Options) withDefaults() Options {
	d := DefaultOptions()
	if o.L0Trigger <= 0 {
		o.L0Trigger = d.L0Trigger
	}
	if o.LevelRatio <= 1 {
		o.LevelRatio = d.LevelRatio
	}
	if o.BaseBytes <= 0 {
		o.BaseBytes = d.BaseBytes
	}
	if o.FileBytes <= 0 || o.FileBytes > MaxTableBytes {
		o.FileBytes = d.FileBytes
	}
	if o.BlockBytes <= 0 {
		o.BlockBytes = d.BlockBytes
	}
	return o
}

// Putter stores one table and returns its object key. seq names it
// uniquely within the namespace.
type Putter func(ctx context.Context, level int, seq uint64, data []byte) (string, error)

// Flush writes sorted, unique entries as one level-0 file and returns the
// version holding it. Cost: the entries' own bytes once.
func Flush(ctx context.Context, v Version, entries []Entry, o Options, put Putter) (Version, FileRef, error) {
	o = o.withDefaults()
	data, meta, err := BuildTable(entries, o.BlockBytes)
	if err != nil {
		return Version{}, FileRef{}, err
	}
	f, err := publish(ctx, 0, v.NextSeq, data, meta, put)
	if err != nil {
		return Version{}, FileRef{}, err
	}
	next, err := v.Apply(Edit{Add: map[int][]FileRef{0: {f}}})
	return next, f, err
}

func publish(ctx context.Context, level int, seq uint64, data []byte, meta TableMeta, put Putter) (FileRef, error) {
	key, err := put(ctx, level, seq, data)
	if err != nil {
		return FileRef{}, err
	}
	return FileRef{Key: key, Seq: seq, Min: meta.Min, Max: meta.Max, Bytes: meta.Bytes, Count: meta.Count,
		IndexOff: meta.IndexOff, IndexLen: meta.IndexLen, Spaces: meta.Spaces}, nil
}

// Job is one compaction: Inputs from Level merged with Overlap from
// Level+1, written to Level+1. Bottom means no deeper level holds any of
// the inputs' key range, so tombstones can be dropped.
type Job struct {
	Level   int
	Inputs  []FileRef
	Overlap []FileRef
	Bottom  bool
}

func levelBytes(files []FileRef) int64 {
	var n int64
	for _, f := range files {
		n += f.Bytes
	}
	return n
}

func keyRange(files []FileRef) (lo, hi []byte) {
	for _, f := range files {
		if lo == nil || bytes.Compare(f.Min, lo) < 0 {
			lo = f.Min
		}
		if hi == nil || bytes.Compare(f.Max, hi) > 0 {
			hi = f.Max
		}
	}
	return lo, hi
}

// afterMax is the exclusive bound just past an inclusive Max.
func afterMax(max []byte) []byte { return append(bytes.Clone(max), 0) }

// receives reports whether any input may hold a key in [lo, hi] (hi
// exclusive unless incl; nil is unbounded), by the inputs' key-space
// ranges (FileRef.Spaces; the file's bounds when it has none): no read,
// and exact for the case it is for, a space whose new keys all sort past
// the file.
func receives(inputs []FileRef, lo, hi []byte, incl bool) bool {
	meets := func(min, max []byte) bool {
		if bytes.Compare(max, lo) < 0 {
			return false
		}
		if hi == nil {
			return true
		}
		c := bytes.Compare(min, hi)
		return c < 0 || incl && c == 0
	}
	for _, f := range inputs {
		if len(f.Spaces) == 0 {
			if meets(f.Min, f.Max) {
				return true
			}
			continue
		}
		for _, sp := range f.Spaces {
			if meets(sp.Min, sp.Max) {
				return true
			}
		}
	}
	return false
}

// spaceEnd is the first key past key's space (its first byte), nil past
// the last.
func spaceEnd(key []byte) []byte {
	if key[0] == 0xff {
		return nil
	}
	return []byte{key[0] + 1}
}

// Pick chooses the next compaction, or none. Level 0 compacts whole when it
// reaches L0Trigger files; a level over its budget compacts the file with
// the least overlap below it, so a compaction never rewrites more than it
// must.
func Pick(v Version, o Options) (Job, bool) {
	o = o.withDefaults()
	if len(v.Levels) == 0 {
		return Job{}, false
	}
	if len(v.Levels[0]) >= o.L0Trigger {
		return v.job(0, v.Levels[0], o), true
	}
	budget := o.BaseBytes
	for l := 1; l < len(v.Levels); l++ {
		if levelBytes(v.Levels[l]) > budget {
			var best FileRef
			bestOverlap := int64(-1)
			for _, f := range v.Levels[l] {
				over := levelBytes(v.overlapIn(l+1, f.Min, afterMax(f.Max)))
				if bestOverlap < 0 || over < bestOverlap {
					best, bestOverlap = f, over
				}
			}
			return v.job(l, []FileRef{best}, o), true
		}
		budget *= o.LevelRatio
	}
	return Job{}, false
}

func (v Version) overlapIn(level int, lo, hi []byte) []FileRef {
	if level >= len(v.Levels) {
		return nil
	}
	var out []FileRef
	for _, f := range v.Levels[level] {
		if f.Overlaps(lo, hi) {
			out = append(out, f)
		}
	}
	return out
}

func (v Version) job(level int, inputs []FileRef, o Options) Job {
	o = o.withDefaults()
	lo, hi := keyRange(inputs)
	hi = afterMax(hi)
	j := Job{Level: level, Inputs: inputs, Overlap: v.overlapIn(level+1, lo, hi), Bottom: true}
	// The level-below file just before the inputs joins when it is small:
	// Compact grows it with the input keys in the gap after it (a space
	// whose keys only append, lowest in the inputs' range, would otherwise
	// leave a small file behind every round).
	if level+1 < len(v.Levels) {
		below := v.Levels[level+1]
		i := sort.Search(len(below), func(i int) bool { return bytes.Compare(below[i].Max, lo) >= 0 })
		if i > 0 {
			if p := below[i-1]; p.Bytes < o.FileBytes/2 && p.Max[0] == lo[0] {
				j.Overlap = append([]FileRef{p}, j.Overlap...)
			}
		}
	}
	// Overlap widens the range that deeper levels must be clear of.
	if len(j.Overlap) > 0 {
		olo, ohi := keyRange(j.Overlap)
		if bytes.Compare(olo, lo) < 0 {
			lo = olo
		}
		if ohi = afterMax(ohi); bytes.Compare(ohi, hi) > 0 {
			hi = ohi
		}
	}
	for l := level + 2; l < len(v.Levels); l++ {
		if len(v.overlapIn(l, lo, hi)) > 0 {
			j.Bottom = false
			break
		}
	}
	return j
}

// Compact runs j: its inputs merged with its overlap, resolved, written as
// files of about FileBytes into Level+1, and returns the version with the
// inputs replaced. The merge is partitioned by the overlap files' key
// ranges (and the gaps around them, which hold input keys alone), one
// independent merge per range run Workers at a time: a level-0 job whose
// files span every key space would otherwise rewrite all of level 1 in
// one serial pass. An overlap file no input key falls in stays where it
// is, unread and unwritten: a level-0 file spans every key space by its
// bounds, but a space whose keys only grow gains keys only past its last
// file, so bounds alone would rewrite them every round.
// Output files are unique by sequence, so a partition publishes as it
// goes; the caller publishes the version, and a crash before that leaves
// only orphan objects. The returned Edit is the change from v (inputs and
// rewritten overlap deleted, outputs added), for a caller that publishes
// it over a newer version (Version.Rebase).
func Compact(ctx context.Context, v Version, j Job, o Options, r Reader, put Putter) (Version, Edit, error) {
	o = o.withDefaults()
	inputs := make([]*Table, len(j.Inputs))
	for i, f := range j.Inputs {
		t, err := r.Open(ctx, f)
		if err != nil {
			return Version{}, Edit{}, err
		}
		inputs[i] = t
	}
	var touched []FileRef
	overlaps := make([]*Table, len(j.Overlap))
	for i, f := range j.Overlap {
		// A small file also takes its space's input keys in the gap after
		// it, so a space's tail grows onto its last file until that file
		// is half a file's size, instead of leaving one small file per
		// round.
		hi, incl := f.Max, true
		if f.Bytes < o.FileBytes/2 {
			hi, incl = spaceEnd(f.Max), false
			if i+1 < len(j.Overlap) && (hi == nil || bytes.Compare(j.Overlap[i+1].Min, hi) < 0) {
				hi = j.Overlap[i+1].Min
			}
		}
		if !receives(j.Inputs, f.Min, hi, incl) {
			continue
		}
		t, err := r.Open(ctx, f)
		if err != nil {
			return Version{}, Edit{}, err
		}
		overlaps[i] = t
		touched = append(touched, f)
	}
	// The partitions, in key order, each a run of consecutive touched
	// overlap files (and the gaps around them, which hold input keys alone)
	// worth about FileBytes of overlap: enough files for the merge to run
	// wide, never so many that every round leaves a smaller file behind. An
	// untouched file ends the partition before it and the next starts after
	// it, so no output spans it.
	type partition struct {
		lo, hi   []byte
		overlaps []*Table
	}
	var parts []partition
	cur := partition{}
	var curBytes int64
	for i, f := range j.Overlap {
		if overlaps[i] == nil {
			cur.hi = f.Min
			parts = append(parts, cur)
			cur, curBytes = partition{lo: afterMax(f.Max)}, 0
			continue
		}
		if curBytes >= o.FileBytes/2 {
			cur.hi = f.Min
			parts = append(parts, cur)
			cur, curBytes = partition{lo: f.Min}, 0
		}
		cur.overlaps = append(cur.overlaps, overlaps[i])
		curBytes += f.Bytes
	}
	parts = append(parts, cur)
	seq := atomic.Uint64{}
	seq.Store(v.NextSeq)
	outputs := make([][]FileRef, len(parts))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(o.workers())
	for pi, p := range parts {
		g.Go(func() error {
			its := make([]Iterator, 0, len(inputs)+len(p.overlaps))
			for _, t := range inputs {
				its = append(its, Bound(t.Iter(gctx), nil, p.hi))
			}
			for _, t := range p.overlaps {
				its = append(its, Bound(t.Iter(gctx), nil, p.hi))
			}
			in := Resolve(NewMerge(its...), r.Merger, j.Bottom)
			var w *TableWriter
			finish := func() error {
				if w == nil {
					return nil
				}
				data, meta, err := w.Finish()
				if err != nil {
					return err
				}
				f, err := publish(gctx, j.Level+1, seq.Add(1)-1, data, meta, put)
				if err != nil {
					return err
				}
				outputs[pi] = append(outputs[pi], f)
				w = nil
				return nil
			}
			var space byte
			for ok := in.SeekGE(p.lo); ok; ok = in.Next() {
				if err := gctx.Err(); err != nil {
					return err
				}
				// A key space starts a file: the next round's untouched
				// check (receives) can then leave a space no input writes
				// to, where a file shared with a busy space is rewritten
				// with it.
				if w != nil && in.Key()[0] != space {
					if err := finish(); err != nil {
						return err
					}
				}
				if w == nil {
					w = NewTableWriter(o.BlockBytes)
					space = in.Key()[0]
				}
				if err := w.Add(Entry{Key: in.Key(), Kind: in.Kind(), Value: in.Value()}); err != nil {
					return err
				}
				if w.Bytes() >= o.FileBytes {
					if err := finish(); err != nil {
						return err
					}
				}
			}
			if err := in.Err(); err != nil {
				return err
			}
			return finish()
		})
	}
	if err := g.Wait(); err != nil {
		return Version{}, Edit{}, err
	}
	e := Edit{Add: map[int][]FileRef{}}
	for _, f := range j.Inputs {
		e.Del = append(e.Del, f.Key)
	}
	for _, f := range touched {
		e.Del = append(e.Del, f.Key)
	}
	for _, out := range outputs {
		e.Add[j.Level+1] = append(e.Add[j.Level+1], out...)
	}
	next, err := v.Apply(e)
	if err != nil {
		return Version{}, Edit{}, fmt.Errorf("lsm: compaction of level %d: %w", j.Level, err)
	}
	if next.NextSeq < seq.Load() {
		next.NextSeq = seq.Load()
	}
	return next, e, nil
}
