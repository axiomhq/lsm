package lsm

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Options sizes the levels.
type Options struct {
	L0Trigger  int   // L0 files that start an L0→L1 compaction
	LevelRatio int64 // level n+1 holds LevelRatio × level n
	BaseBytes  int64 // level 1 target bytes
	FileBytes  int64 // output file target
	BlockBytes int   // raw bytes a block closes at (DefaultBlockBytes when 0); a block never exceeds MaxTableBytes
	// Workers bounds the partitions one compaction merges at once: a job
	// is split by its overlap files' key ranges, and each range is an
	// independent merge (0: half the CPUs, at least one).
	Workers int
	// MaxTableAge bounds how long a write stays above the bottom level:
	// Pick compacts a file whose Oldest is older than this down a level
	// even when no size rule fires, so a delete or an overwrite reaches
	// the bottom, where the superseded versions are dropped, within about
	// MaxTableAge (0: never by age).
	MaxTableAge time.Duration
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

// Putter stores one table and returns its object key. seq is the file's
// sequence: unique within one level of the version it joins, and no more.
// Two writers starting from one version hand out the same seqs (a Flush
// on head while a Compact runs on a snapshot, which Rebase then puts in
// one version at different levels; two Compacts of one job), so a key
// must not be seq alone when writers race: add the level and a writer
// id, or let the store name the object. FileRef.Key is a file's identity.
// Compact calls put from up to Options.Workers goroutines at once; it
// must be safe for concurrent use unless Workers is 1.
type Putter func(ctx context.Context, level int, seq uint64, data []byte) (string, error)

// Flush writes sorted, unique entries as one level-0 file and returns the
// version holding it. Cost: the entries' own bytes once.
func Flush(ctx context.Context, v Version, entries []Entry, o Options, put Putter) (Version, FileRef, error) {
	o = o.withDefaults()
	data, meta, err := BuildTable(entries, o.BlockBytes)
	if err != nil {
		return Version{}, FileRef{}, err
	}
	f, err := publish(ctx, 0, v.NextSeq, time.Now().Unix(), data, meta, put)
	if err != nil {
		return Version{}, FileRef{}, err
	}
	next, err := v.Apply(Edit{Add: map[int][]FileRef{0: {f}}})
	return next, f, err
}

func publish(ctx context.Context, level int, seq uint64, oldest int64, data []byte, meta TableMeta, put Putter) (FileRef, error) {
	key, err := put(ctx, level, seq, data)
	if err != nil {
		return FileRef{}, fmt.Errorf("lsm: put level %d seq %d: %w", level, seq, err)
	}
	return FileRef{Key: key, Seq: seq, Oldest: oldest, TableMeta: meta}, nil
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
// must. Failing both, with MaxTableAge set, the shallowest file above the
// bottom level older than it compacts (level 0 whole, as its files
// overlap): its output keeps the age until it lands where tombstones drop,
// so the next picks carry it down. The bottom level is never picked by
// age, as it holds no tombstone and no shadowed version.
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
	if o.MaxTableAge <= 0 {
		return Job{}, false
	}
	cutoff := time.Now().Add(-o.MaxTableAge).Unix()
	for l := 0; l == 0 || l < len(v.Levels)-1; l++ {
		for _, f := range v.Levels[l] {
			if f.Oldest >= cutoff {
				continue
			}
			if l == 0 {
				return v.job(0, v.Levels[0], o), true
			}
			return v.job(l, []FileRef{f}, o), true
		}
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

// job plans the compaction of inputs at level; o has its defaults filled.
func (v Version) job(level int, inputs []FileRef, o Options) Job {
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
// Output files are unique by sequence within one compaction, so a partition publishes as it
// goes; the caller publishes the version, and a crash before that leaves
// only orphan objects. The returned Edit is the change from v (inputs and
// rewritten overlap deleted, outputs added), for a caller that publishes
// it over a newer version (Version.Rebase). On error the Edit's Add lists
// the files already published, orphans the caller may delete, and its
// NextSeq is the first sequence no output used. A retry reuses those
// seqs unless the version it starts from is past them: apply
// Edit{NextSeq: edit.NextSeq} to it first, never the failed Edit's Add.
func Compact(ctx context.Context, v Version, j Job, o Options, r Reader, put Putter) (Version, Edit, error) {
	o = o.withDefaults()
	inputs := make([]named, len(j.Inputs))
	for i, f := range j.Inputs {
		t, err := r.Open(ctx, f)
		if err != nil {
			return Version{}, Edit{}, fmt.Errorf("lsm: open %s: %w", f.Key, err)
		}
		inputs[i] = named{f.Key, t}
	}
	var touched []FileRef
	overlaps := make([]*named, len(j.Overlap))
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
			return Version{}, Edit{}, fmt.Errorf("lsm: open %s: %w", f.Key, err)
		}
		overlaps[i] = &named{f.Key, t}
		touched = append(touched, f)
	}
	// The partitions, in key order, each a run of consecutive touched
	// overlap files (and the gaps around them, which hold input keys alone)
	// worth about FileBytes of overlap: enough files for the merge to run
	// wide, never so many that every round leaves a smaller file behind. An
	// untouched file ends the partition before it and the next starts after
	// it, so no output spans it.
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
	// The outputs' age: a bottom job drops every tombstone and shadowed
	// version, so its outputs are as new as the job; any other keeps the
	// oldest age it merged, for MaxTableAge to carry down.
	oldest := time.Now().Unix()
	if !j.Bottom {
		for _, f := range slices.Concat(j.Inputs, touched) {
			oldest = min(oldest, f.Oldest)
		}
	}
	c := &compaction{o: o, j: j, r: r, put: put, inputs: inputs, oldest: oldest, outputs: make([][]FileRef, len(parts))}
	c.seq.Store(v.NextSeq)
	// Partitions run o.workers() at a time; the first error cancels the rest.
	gctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	sem := make(chan struct{}, o.workers())
	for pi, p := range parts {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := c.writePartition(gctx, pi, p); err != nil {
				cancel(err)
			}
		})
	}
	wg.Wait()
	published := Edit{Add: map[int][]FileRef{j.Level + 1: slices.Concat(c.outputs...)}, NextSeq: c.seq.Load()}
	if err := context.Cause(gctx); err != nil {
		return Version{}, published, err
	}
	e := published
	for _, f := range j.Inputs {
		e.Del = append(e.Del, f.Key)
	}
	for _, f := range touched {
		e.Del = append(e.Del, f.Key)
	}
	next, err := v.Apply(e)
	if err != nil {
		return Version{}, published, fmt.Errorf("lsm: compaction of level %d: %w", j.Level, err)
	}
	return next, e, nil
}

// partition is one independent merge of a compaction: the inputs and the
// touched overlap files in [lo, hi), written to its own output files.
type partition struct {
	lo, hi   []byte
	overlaps []*named
}

// compaction is the state Compact's partitions share.
type compaction struct {
	o       Options
	j       Job
	r       Reader
	put     Putter
	inputs  []named
	oldest  int64 // every output's FileRef.Oldest
	seq     atomic.Uint64
	outputs [][]FileRef // per partition; only its own goroutine writes it
}

// writePartition merges partition pi and publishes its files as it goes.
func (c *compaction) writePartition(ctx context.Context, pi int, p partition) error {
	its := make([]Iterator, 0, len(c.inputs)+len(p.overlaps))
	for _, t := range c.inputs {
		its = append(its, Bound(t.iter(ctx), nil, p.hi))
	}
	for _, t := range p.overlaps {
		its = append(its, Bound(t.iter(ctx), nil, p.hi))
	}
	in := Resolve(NewMerge(its...), c.r.Merger, c.j.Bottom)
	var w *TableWriter
	finish := func() error {
		if w == nil {
			return nil
		}
		data, meta, err := w.Finish()
		if err != nil {
			return err
		}
		f, err := publish(ctx, c.j.Level+1, c.seq.Add(1)-1, c.oldest, data, meta, c.put)
		if err != nil {
			return err
		}
		c.outputs[pi] = append(c.outputs[pi], f)
		w = nil
		return nil
	}
	var space byte
	for ok := in.SeekGE(p.lo); ok; ok = in.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A key space starts a file: the next round's untouched check
		// (receives) can then leave a space no input writes to, where a
		// file shared with a busy space is rewritten with it.
		// A file closes before the entry that would take it past FileBytes
		// (at most MaxTableBytes); every entry fits a file of its own.
		e := Entry{Key: in.Key(), Kind: in.Kind(), Value: in.Value()}
		if w != nil && (e.Key[0] != space || !w.fits(e, c.o.FileBytes)) {
			if err := finish(); err != nil {
				return err
			}
		}
		if w == nil {
			w = NewTableWriter(c.o.BlockBytes)
			space = e.Key[0]
		}
		if err := w.Add(e); err != nil {
			return fmt.Errorf("lsm: level %d: %w", c.j.Level+1, err)
		}
	}
	if err := in.Err(); err != nil {
		return err
	}
	return finish()
}
