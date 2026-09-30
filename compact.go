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
	L0Trigger int // L0 files that start a compaction of level 0
	// Leveled picks leveled compaction (the default through v0.9.0, and
	// v0.9.0's picker): levels 1 and deeper are budgeted, LevelRatio
	// apart, a level over budget drains a run of files into the level
	// below before level 0 goes, and level 0 goes a key space at a time.
	// The default is tiered: every level below 0 is one sorted run,
	// newest first, and runs merge with each other by size (Pick), so a
	// byte is rewritten about log2 of the data over level 0's size times,
	// where a leveled level-0 job rewrites its spaces' share of level 1.
	Leveled    bool
	LevelRatio int64 // leveled: level n+1 holds LevelRatio × level n
	BaseBytes  int64 // leveled: level 1 target bytes
	// MaxRuns bounds a key space's sorted runs when tiered (0: 8): past it,
	// the adjacent runs with the fewest bytes merge.
	MaxRuns    int
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
	// ReadAheadBytes is the window a compaction reads its input tables in:
	// a block miss reads the consecutive blocks up to this many bytes in
	// one Source.ReadAt, and the window after it is read while the merge
	// walks this one, so each input a merge reads holds up to two windows,
	// three at a window's edge (0: 1 MiB; negative: one block per read,
	// through Table.Cache).
	ReadAheadBytes int64
	// Uploads bounds the output files one compaction puts at once: a
	// merge hands a finished file to put and goes on with the next, and
	// waits only when Uploads puts are in flight, so a compaction holds up
	// to Workers files being written and Uploads being put. Compact returns
	// once every put has (0: Workers; negative: each merge waits for its
	// own put).
	Uploads int
}

// DefaultOptions: tiered, 4 L0 files, 8 runs, 48 MiB files (leveled:
// ratio 10, 256 MiB L1).
func DefaultOptions() Options {
	return Options{L0Trigger: 4, MaxRuns: 8, LevelRatio: 10, BaseBytes: 256 << 20, FileBytes: 48 << 20, BlockBytes: DefaultBlockBytes}
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
	if o.MaxRuns <= 0 {
		o.MaxRuns = d.MaxRuns
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
	if o.ReadAheadBytes == 0 {
		o.ReadAheadBytes = 1 << 20
	}
	if o.Uploads == 0 {
		o.Uploads = o.workers()
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
// Compact calls put from up to Options.Uploads goroutines at once
// (Workers when Uploads is negative); it must be safe for concurrent use
// unless that is 1.
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

// Job is one compaction: Inputs merged with Overlap from Level+1, written
// to Level+1. Inputs are newest first: level 0, or the files of Level,
// or (tiered) the files of the runs above Level+1 being merged into it.
// Bottom means no deeper level holds any of the inputs' key range, so
// tombstones can be dropped. NewRun (tiered, Level 0, no Overlap) makes
// the outputs a run of their own: each key space level 1 holds moves a
// level deeper, down to its first level without the space, so level 1 is
// free for them. The moved files are in the Edit's Del and in its Add
// below level 1 under their own keys; Add[Level+1] is the outputs alone.
type Job struct {
	Level   int
	Inputs  []FileRef
	Overlap []FileRef
	Bottom  bool
	NewRun  bool
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

// Pick chooses the next compaction, or none: pickTiered unless
// Options.Leveled.
func Pick(v Version, o Options) (Job, bool) {
	o = o.withDefaults()
	if len(v.Levels) == 0 {
		return Job{}, false
	}
	if o.Leveled {
		return v.pickLeveled(o)
	}
	return v.pickTiered(o)
}

// pickLeveled is the leveled picker (v0.9.0). A level over its budget
// goes first, the one furthest over (bytes / budget): it moves the
// contiguous run of files with the least overlap below per byte, at least
// its excess (at least one file, at most BaseBytes), so a drain costs a
// few jobs and a level never grows past its budget by more than one job's
// push. Level 0 compacts when it holds L0Trigger files and every deeper
// level is within budget: each level-0 job then owes, before the next one,
// the deeper work its bytes cause, and level 1 holds about BaseBytes plus
// one level-0 job. Level 0 goes one key space at a time: the files of the
// oldest file's space (its first key byte), and every level-0 file whose
// range meets them, to closure, so no file left behind overlaps what moves
// down and the job rewrites only that space's share of level 1. Failing
// both, with MaxTableAge set, the shallowest file above the bottom level
// older than it compacts (level 0 whole, as its files overlap): its output
// keeps the age until it lands where tombstones drop, so the next picks
// carry it down. The bottom level is never picked by age, as it holds no
// tombstone and no shadowed version.
func (v Version) pickLeveled(o Options) (Job, bool) {
	over, excess, worst, budget := 0, int64(0), 1.0, o.BaseBytes
	for l := 1; l < len(v.Levels); l++ {
		n := levelBytes(v.Levels[l])
		if s := float64(n) / float64(budget); s > worst {
			over, excess, worst = l, n-budget, s
		}
		budget *= o.LevelRatio
	}
	if over > 0 {
		return v.job(over, v.drain(over, min(excess, o.BaseBytes)), o), true
	}
	if len(v.Levels[0]) >= o.L0Trigger {
		return v.job(0, v.spaceOfOldest(), o), true
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

// drain is the run of consecutive files of level l (1 or deeper) holding
// at least want bytes (one file at least) whose overlap in level l+1 is
// the fewest bytes per input byte; the first such run on a tie.
func (v Version) drain(l int, want int64) []FileRef {
	files := v.Levels[l]
	var below []FileRef
	if l+1 < len(v.Levels) {
		below = v.Levels[l+1]
	}
	sum := make([]int64, len(below)+1) // sum[i]: bytes of below[:i]
	for i, f := range below {
		sum[i+1] = sum[i] + f.Bytes
	}
	bi, bj, best := 0, len(files), -1.0
	var in int64
	j := 0 // files[i:j] is the run from i
	for i := range files {
		for j < len(files) && (j == i || in < want) {
			in += files[j].Bytes
			j++
		}
		if in < want && i > 0 {
			break // no later run reaches want
		}
		lo, hi := files[i].Min, afterMax(files[j-1].Max)
		a := firstReaching(below, lo)
		b := sort.Search(len(below), func(k int) bool { return bytes.Compare(below[k].Min, hi) >= 0 })
		if r := float64(sum[max(a, b)]-sum[a]) / float64(max(in, 1)); best < 0 || r < best {
			bi, bj, best = i, j, r
		}
		in -= files[i].Bytes
	}
	return files[bi:bj]
}

// spaceOfOldest is the level-0 files a level-0 job takes: the oldest
// file's key space and every file that meets the range taken so far,
// newest first. A file spanning spaces widens the range, so the job may
// take more than one space, and all of level 0 at worst.
func (v Version) spaceOfOldest() []FileRef {
	files := v.Levels[0]
	seed := files[len(files)-1].Min
	if len(seed) == 0 {
		return files
	}
	lo, hi := []byte{seed[0]}, spaceEnd(seed)
	in := make([]bool, len(files))
	for grew := true; grew; {
		grew = false
		for i, f := range files {
			if in[i] || !f.Overlaps(lo, hi) {
				continue
			}
			in[i], grew = true, true
			if bytes.Compare(f.Min, lo) < 0 {
				lo = f.Min
			}
			if hi != nil && bytes.Compare(f.Max, hi) >= 0 {
				hi = afterMax(f.Max)
			}
		}
	}
	var out []FileRef
	for i, f := range files {
		if in[i] {
			out = append(out, f)
		}
	}
	return out
}

// pickTiered treats each level below 0 as a sorted run, newest first, and
// each key space as its own stack of runs: a file below level 0 holds one
// space (compaction has always started a file per space, and only
// compaction writes below level 0), so the runs of one space
// merge without touching another's, and a job is bounded by a space's
// bytes, not the version's. A level may hold a space or not: a merge
// leaves the space empty in the levels of its newer runs.
//
//  1. Level 0 at L0Trigger files becomes a new run (Job.NewRun): it never
//     merges with a run, so a level-0 job costs level 0's bytes alone.
//  2. Size: in a space, the newest window of adjacent runs in which every
//     run is no bigger than the runs before it in the window put together
//     merges into its oldest run. Runs of equal size pair up like the bits
//     of a binary counter: a byte is merged about log2(space bytes /
//     level-0 bytes of the space) times, and a space holds about as many
//     runs.
//  3. Count: past MaxRuns runs in a space, the adjacent window of
//     runs-MaxRuns+1 of them with the fewest bytes merges.
//  4. Age (MaxTableAge): an aged file in any run of a space but the oldest
//     merges its run and every older one into the oldest; an aged level-0
//     file (an idle version: an active one makes level 0 a new run first)
//     merges level 0 and every level into the deepest. Either way the
//     tombstones and shadowed versions it holds drop in one job.
//
// Of the merges rules 2 and 3 owe, the one with the fewest bytes runs
// first. A merge's Overlap is its oldest run's files, which partition it
// (Compact); the files of that run no newer key falls in stay where they
// are.
func (v Version) pickTiered(o Options) (Job, bool) {
	if len(v.Levels[0]) >= o.L0Trigger {
		return v.newRun(), true
	}
	type run struct {
		level int
		bytes int64
	}
	var spaces [256][]run // per space, the levels holding it, newest first
	for l := 1; l < len(v.Levels); l++ {
		for _, f := range v.Levels[l] {
			rs := spaces[f.Min[0]]
			if n := len(rs); n > 0 && rs[n-1].level == l {
				rs[n-1].bytes += f.Bytes
			} else {
				rs = append(rs, run{l, f.Bytes})
			}
			spaces[f.Min[0]] = rs
		}
	}
	best, bestBytes := Job{}, int64(-1)
	owe := func(space int, rs []run) {
		var n int64
		levels := make([]int, len(rs))
		for i, r := range rs {
			n += r.bytes
			levels[i] = r.level
		}
		if bestBytes < 0 || n < bestBytes {
			best, bestBytes = v.mergeRuns(byte(space), levels, o), n
		}
	}
	for s, rs := range spaces {
		for i := range rs {
			sum, k := rs[i].bytes, i
			for k+1 < len(rs) && rs[k+1].bytes <= sum {
				k++
				sum += rs[k].bytes
			}
			if k > i {
				owe(s, rs[i:k+1])
				break
			}
		}
	}
	if bestBytes < 0 {
		for s, rs := range spaces {
			if len(rs) <= o.MaxRuns {
				continue
			}
			w := len(rs) - o.MaxRuns + 1
			win, winBytes := 0, int64(-1)
			for i := 0; i+w <= len(rs); i++ {
				var n int64
				for _, r := range rs[i : i+w] {
					n += r.bytes
				}
				if winBytes < 0 || n < winBytes {
					win, winBytes = i, n
				}
			}
			owe(s, rs[win:win+w])
		}
	}
	if bestBytes >= 0 {
		return best, true
	}
	if o.MaxTableAge <= 0 {
		return Job{}, false
	}
	cutoff := time.Now().Add(-o.MaxTableAge).Unix()
	if slices.ContainsFunc(v.Levels[0], func(f FileRef) bool { return f.Oldest < cutoff }) {
		return v.toBottom(o), true
	}
	for s, rs := range spaces {
		for i := 0; i+1 < len(rs); i++ {
			aged := slices.ContainsFunc(v.Levels[rs[i].level], func(f FileRef) bool { return f.Min[0] == byte(s) && f.Oldest < cutoff })
			if aged {
				levels := make([]int, 0, len(rs)-i)
				for _, r := range rs[i:] {
					levels = append(levels, r.level)
				}
				return v.mergeRuns(byte(s), levels, o), true
			}
		}
	}
	return Job{}, false
}

// toBottom is the job merging level 0 and every level but the deepest
// into the deepest, whole: a level-0 file is not one space's, and only
// whole levels keep a key's versions in order when they move.
func (v Version) toBottom(o Options) Job {
	d := len(v.Levels) - 1
	if d == 0 {
		return v.newRun()
	}
	return v.job(d-1, slices.Concat(v.Levels[:d]...), o)
}

// newRun is the job making level 0 a new run above every level.
func (v Version) newRun() Job {
	j := Job{Level: 0, Inputs: v.Levels[0], Bottom: true, NewRun: true}
	lo, hi := keyRange(j.Inputs)
	for l := 1; l < len(v.Levels); l++ {
		if len(v.overlapIn(l, lo, afterMax(hi))) > 0 {
			j.Bottom = false
			break
		}
	}
	return j
}

// mergeRuns is the job merging space's runs at levels (ascending, adjacent
// runs of the space) into the last of them.
func (v Version) mergeRuns(space byte, levels []int, o Options) Job {
	var inputs []FileRef
	for _, l := range levels[:len(levels)-1] {
		for _, f := range v.Levels[l] {
			if f.Min[0] == space {
				inputs = append(inputs, f)
			}
		}
	}
	return v.job(levels[len(levels)-1]-1, inputs, o)
}

// shift is the Edit part of a NewRun job over v: every space level 1
// holds moves a level deeper, from level 1 down to the first level
// without that space, so level 1 is empty and each space's runs keep
// their order.
func (v Version) shift() (del []string, add map[int][]FileRef) {
	add = map[int][]FileRef{}
	var moving [256]bool
	for l := 1; l < len(v.Levels); l++ {
		var held [256]bool
		for _, f := range v.Levels[l] {
			held[f.Min[0]] = true
		}
		any := false
		for s := range moving {
			moving[s] = held[s] && (l == 1 || moving[s])
			any = any || moving[s]
		}
		if !any {
			break
		}
		for _, f := range v.Levels[l] {
			if moving[f.Min[0]] {
				del = append(del, f.Key)
				add[l+1] = append(add[l+1], f)
			}
		}
	}
	return del, add
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
// inputs replaced (and, for a NewRun job, the levels it displaces moved a
// level deeper). The merge is partitioned by the overlap files' key
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
		if !j.touches(i, o) {
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
	if len(j.Overlap) == 0 {
		parts = spaceParts(j.Inputs)
	}
	// The outputs' age: a bottom job drops every tombstone and shadowed
	// version, so its outputs are as new as the job; any other keeps the
	// oldest age it merged, for MaxTableAge to carry down.
	oldest := time.Now().Unix()
	if !j.Bottom {
		for _, f := range slices.Concat(j.Inputs, touched) {
			oldest = min(oldest, f.Oldest)
		}
	}
	// Partitions run o.workers() at a time, their puts o.Uploads at a time;
	// the first error cancels the rest.
	gctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	c := &compaction{o: o, j: j, r: r, put: put, inputs: inputs, oldest: oldest, outputs: make([][]*FileRef, len(parts)), cancel: cancel}
	if o.Uploads > 0 {
		c.uploads = make(chan struct{}, o.Uploads)
	}
	c.seq.Store(v.NextSeq)
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
	c.puts.Wait()
	var added []FileRef
	for _, fs := range c.outputs {
		for _, f := range fs {
			if f.Key != "" { // "": its put failed or never ran
				added = append(added, *f)
			}
		}
	}
	published := Edit{Add: map[int][]FileRef{j.Level + 1: added}, NextSeq: c.seq.Load()}
	if err := context.Cause(gctx); err != nil {
		return Version{}, published, err
	}
	e := Edit{Add: map[int][]FileRef{j.Level + 1: added}, NextSeq: published.NextSeq}
	for _, f := range j.Inputs {
		e.Del = append(e.Del, f.Key)
	}
	for _, f := range touched {
		e.Del = append(e.Del, f.Key)
	}
	if j.NewRun {
		del, add := v.shift()
		e.Del = append(e.Del, del...)
		for l, fs := range add {
			e.Add[l] = fs
		}
	}
	next, err := v.Apply(e)
	if err != nil {
		return Version{}, published, fmt.Errorf("lsm: compaction of level %d: %w", j.Level, err)
	}
	return next, e, nil
}

// touches reports whether an input may write into overlap file i, by the
// inputs' key-space ranges (receives): a file no input key falls in stays
// where it is. A small file also takes its space's input keys in the gap
// after it, so a space's tail grows onto its last file until that file is
// half a file's size, instead of leaving one small file per round.
func (j Job) touches(i int, o Options) bool {
	f := j.Overlap[i]
	hi, incl := f.Max, true
	if f.Bytes < o.FileBytes/2 {
		hi, incl = spaceEnd(f.Max), false
		if i+1 < len(j.Overlap) && (hi == nil || bytes.Compare(j.Overlap[i+1].Min, hi) < 0) {
			hi = j.Overlap[i+1].Min
		}
	}
	return receives(j.Inputs, f.Min, hi, incl)
}

// spaceParts is the partitions of a job with no overlap: one per key
// space the inputs hold, so a new run is written as wide as its spaces.
func spaceParts(inputs []FileRef) []partition {
	var spaces []byte
	for _, f := range inputs {
		if len(f.Spaces) == 0 {
			for b := int(f.Min[0]); b <= int(f.Max[0]); b++ {
				spaces = append(spaces, byte(b))
			}
		}
		for _, sp := range f.Spaces {
			spaces = append(spaces, sp.Space)
		}
	}
	slices.Sort(spaces)
	spaces = slices.Compact(spaces)
	parts := make([]partition, len(spaces))
	for i, b := range spaces {
		if i > 0 {
			parts[i].lo = []byte{b}
		}
		if i+1 < len(spaces) {
			parts[i].hi = []byte{spaces[i+1]}
		}
	}
	return parts
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
	outputs [][]*FileRef // per partition; only its own goroutine appends, its puts fill them
	cancel  context.CancelCauseFunc
	uploads chan struct{} // a slot per put in flight; nil: a merge puts its own files
	puts    sync.WaitGroup
}

// writePartition merges partition pi and hands its files to put as it goes.
func (c *compaction) writePartition(ctx context.Context, pi int, p partition) error {
	its := make([]Iterator, 0, len(c.inputs)+len(p.overlaps))
	scans := make([]*namedIter, 0, cap(its))
	// A partition that fails part way returns only once its scans' reads
	// have: Compact never returns with a Source read still running.
	defer func() {
		for _, s := range scans {
			s.settle()
		}
	}()
	scan := func(t named) {
		s := t.scan(ctx, c.o.ReadAheadBytes, p.hi)
		scans = append(scans, s)
		its = append(its, Bound(s, nil, p.hi))
	}
	for i, t := range c.inputs {
		// An input past the partition would still read a block to seek.
		if c.j.Inputs[i].Overlaps(p.lo, p.hi) {
			scan(t)
		}
	}
	for _, t := range p.overlaps {
		scan(*t)
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
		w = nil
		if c.uploads != nil {
			select {
			case c.uploads <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			// A failed put cancels before it frees its slot: no put starts
			// after one failed.
			if err := ctx.Err(); err != nil {
				<-c.uploads
				return err
			}
		}
		f := new(FileRef)
		c.outputs[pi] = append(c.outputs[pi], f)
		seq := c.seq.Add(1) - 1
		up := func() (err error) {
			*f, err = publish(ctx, c.j.Level+1, seq, c.oldest, data, meta, c.put)
			return err
		}
		if c.uploads == nil {
			return up()
		}
		c.puts.Go(func() {
			defer func() { <-c.uploads }()
			if err := up(); err != nil {
				c.cancel(err)
			}
		})
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
