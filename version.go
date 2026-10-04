package lsm

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
)

// FileRef names one table in the manifest: its object key, its sequence
// (allocation order; level-0 precedence) and what the writer learned about
// it, which a reader prunes and opens by without touching the object. The
// json names are the manifest encoding and are stable; a manifest written
// by any earlier version decodes.
type FileRef struct {
	Key string `json:"key"`
	Seq uint64 `json:"seq"`
	// Oldest is the unix second of the oldest write the file may still
	// hold a shadowed version or a tombstone of: its write time for a
	// Flush and for a compaction that dropped tombstones (Job.Bottom),
	// otherwise the oldest of the files merged into it. Options.MaxTableAge
	// compacts by it. 0, as from a manifest before v0.6.0, is the epoch.
	Oldest int64 `json:"oldest,omitempty"`
	TableMeta
}

// Overlaps reports whether the file's key range meets [lo, hi); nil lo or
// hi is unbounded.
func (f FileRef) Overlaps(lo, hi []byte) bool {
	if lo != nil && bytes.Compare(f.Max, lo) < 0 {
		return false
	}
	return hi == nil || bytes.Compare(f.Min, hi) < 0
}

// holds reports whether key is within the file's [Min, Max].
func (f FileRef) holds(key []byte) bool {
	return bytes.Compare(f.Min, key) <= 0 && bytes.Compare(key, f.Max) <= 0
}

// Version is the levels of a namespace's LSM at one manifest. Level 0 is
// newest first and its files overlap; every later level is sorted by Min
// with disjoint ranges. NextSeq names the next file.
type Version struct {
	Levels  [][]FileRef `json:"levels,omitempty"`
	NextSeq uint64      `json:"next_seq,omitempty"`
}

// Overlapping is the files whose range meets [lo, hi), newest first: level
// 0 in Seq order, then each level in key order. That order is MergeIter's
// precedence.
func (v Version) Overlapping(lo, hi []byte) []FileRef {
	var out []FileRef
	for l, files := range v.Levels {
		if l > 0 && lo != nil {
			files = files[firstReaching(files, lo):]
		}
		for _, f := range files {
			if f.Overlaps(lo, hi) {
				out = append(out, f)
			} else if l > 0 {
				break // every later file starts at or past hi
			}
		}
	}
	return out
}

// firstReaching is the index of the first file of a sorted level (1 and
// deeper: ordered by Min, disjoint) whose Max is at or past key: the only
// file of the level that may hold key, and the first that may meet a range
// from key.
func firstReaching(files []FileRef, key []byte) int {
	return sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].Max, key) >= 0 })
}

// candidates is the files of level l that may hold key: all of level 0,
// whose files overlap, and at most one of a later level.
func candidates(l int, files []FileRef, key []byte) []FileRef {
	if l == 0 {
		return files
	}
	i := firstReaching(files, key)
	return files[i:min(i+1, len(files))]
}

// Signature identifies what a read of the given key ranges sees in v: a
// hash of the names of the files overlapping any of them. Versions that
// did not touch those files share the signature, so a value decoded from
// the ranges can be cached under it. ok is false when no file overlaps.
func (v Version) Signature(ranges ...[2][]byte) (sig uint64, ok bool) {
	h := fnv.New64a()
	for _, l := range v.Levels {
		for _, f := range l {
			for _, r := range ranges {
				if f.Overlaps(r[0], r[1]) {
					h.Write([]byte(f.Key))
					h.Write([]byte{0})
					ok = true
					break
				}
			}
		}
	}
	return h.Sum64(), ok
}

// Edit is one change: files removed by key and files added to a level.
type Edit struct {
	Del     []string
	Add     map[int][]FileRef
	NextSeq uint64 // a floor for the version's NextSeq: sequences consumed by files not in Add
}

// Apply returns the version after e. Levels grow as needed; ordering
// invariants are restored.
func (v Version) Apply(e Edit) (Version, error) {
	del := make(map[string]bool, len(e.Del))
	for _, k := range e.Del {
		del[k] = true
	}
	out := Version{NextSeq: max(v.NextSeq, e.NextSeq)}
	depth := len(v.Levels)
	for l := range e.Add {
		depth = max(depth, l+1)
	}
	out.Levels = make([][]FileRef, depth)
	for l := range out.Levels {
		if l < len(v.Levels) {
			for _, f := range v.Levels[l] {
				if !del[f.Key] {
					out.Levels[l] = append(out.Levels[l], f)
				}
			}
		}
		out.Levels[l] = append(out.Levels[l], e.Add[l]...)
		for _, f := range e.Add[l] {
			out.NextSeq = max(out.NextSeq, f.Seq+1)
		}
		if l == 0 {
			slices.SortFunc(out.Levels[l], func(a, b FileRef) int { return cmp.Compare(b.Seq, a.Seq) })
			continue
		}
		slices.SortFunc(out.Levels[l], func(a, b FileRef) int { return bytes.Compare(a.Min, b.Min) })
		for i := 1; i < len(out.Levels[l]); i++ {
			if bytes.Compare(out.Levels[l][i-1].Max, out.Levels[l][i].Min) >= 0 {
				return Version{}, fmt.Errorf("lsm: level %d files %s and %s overlap", l, out.Levels[l][i-1].Key, out.Levels[l][i].Key)
			}
		}
	}
	for len(out.Levels) > 0 && len(out.Levels[len(out.Levels)-1]) == 0 {
		out.Levels = out.Levels[:len(out.Levels)-1]
	}
	return out, nil
}

// ErrStale is Rebase's refusal: the newer version no longer holds what the
// edit was computed against.
var ErrStale = errors.New("lsm: edit is stale")

// Rebase applies e, a compaction's edit computed on base, to v, a version
// published since. It holds when v differs from base by level-0 files
// added (Flush) and by tiered new runs (Job.NewRun) published since:
//
//   - every file e deletes is still in v, level 0 at level 0;
//   - every file of base at level 1 or deeper is still in v, each space's
//     run moved whole to one level, the runs of a space in their order;
//   - every file of v at level 1 or deeper that base lacks lies above
//     every run of its space that base holds.
//
// Then e's outputs land in the level their run moved to: the added
// files stay newer than every input, and a job that dropped tombstones
// (Job.Bottom) still has nothing beneath it. Anything else, a second
// merge say, or a new run over a merge, is ErrStale.
func (v Version) Rebase(base Version, e Edit) (Version, error) {
	level := func(ver Version) map[string]int {
		at := map[string]int{}
		for l, files := range ver.Levels {
			for _, f := range files {
				at[f.Key] = l
			}
		}
		return at
	}
	was, now := level(base), level(v)
	for _, k := range e.Del {
		l, ok := was[k]
		if nl, present := now[k]; !ok || !present || (l == 0) != (nl == 0) {
			return Version{}, fmt.Errorf("%w: %s", ErrStale, k)
		}
	}
	type run struct {
		space byte
		level int
	}
	moved := map[run]int{} // a base run → its level in v
	top := [256]int{}      // per space, its shallowest base run's level in v (0: none)
	for l := 1; l < len(base.Levels); l++ {
		for _, f := range base.Levels[l] {
			nl, ok := now[f.Key]
			r := run{f.Min[0], l}
			if p, seen := moved[r]; !ok || nl == 0 || seen && p != nl {
				return Version{}, fmt.Errorf("%w: level %d changed", ErrStale, l)
			}
			moved[r] = nl
			if top[r.space] == 0 {
				top[r.space] = nl
			}
		}
	}
	// Runs of a space keep their order: base levels ascend, so must theirs.
	last := [256]int{}
	for l := 1; l < len(base.Levels); l++ {
		for _, f := range base.Levels[l] {
			s := f.Min[0]
			if nl := moved[run{s, l}]; nl != last[s] {
				if nl < last[s] {
					return Version{}, fmt.Errorf("%w: level %d reordered", ErrStale, l)
				}
				last[s] = nl
			}
		}
	}
	same := true
	for r, nl := range moved {
		same = same && nl == r.level
	}
	for l := 1; l < len(v.Levels); l++ {
		for _, f := range v.Levels[l] {
			if _, ok := was[f.Key]; ok {
				continue
			}
			if t := top[f.Min[0]]; t != 0 && l >= t {
				return Version{}, fmt.Errorf("%w: level %d changed", ErrStale, l)
			}
			same = false
		}
	}
	if same {
		return v.Apply(e)
	}
	add := make(map[int][]FileRef, len(e.Add))
	for l, files := range e.Add {
		for _, f := range files {
			nl, ok := moved[run{f.Min[0], l}]
			if !ok {
				return Version{}, fmt.Errorf("%w: level %d has no run of space %#x to land in", ErrStale, l, f.Min[0])
			}
			add[nl] = append(add[nl], f)
		}
	}
	e.Add = add
	return v.Apply(e)
}

// Opener opens a file's table.
type Opener func(ctx context.Context, f FileRef) (*Table, error)

// Reader reads versions: Open opens a file's table and Merger folds merge
// operands. Merger may be nil when no key uses KindMerge. Open is called
// from one goroutine at a time. Merger, like Source.ReadAt, is called from
// up to Options.Workers goroutines at once and must be safe for concurrent
// use.
type Reader struct {
	Open   Opener
	Merger Merger
}

// Iter is a resolved iterator over [lo, hi) across every level: the
// merge of one iterator per overlapping file, newest first.
func (r Reader) Iter(ctx context.Context, v Version, lo, hi []byte) (Iterator, error) {
	files := v.Overlapping(lo, hi)
	its := make([]Iterator, 0, len(files))
	for _, f := range files {
		t, err := r.Open(ctx, f)
		if err != nil {
			return nil, fmt.Errorf("lsm: open %s: %w", f.Key, err)
		}
		its = append(its, Bound(named{f.Key, t}.iterBelow(ctx, hi), lo, hi))
	}
	return Resolve(NewMerge(its...), r.Merger, true), nil
}

// Get is a point lookup: tables newest first until a Put or Delete, the
// versions found resolved as Iter resolves them.
func (r Reader) Get(ctx context.Context, v Version, key []byte) ([]byte, bool, error) {
	var hits []Entry
	h := keyHash(key)
	// The files newest first, without materialising the overlapping list:
	// a point lookup is the hottest read.
walk:
	for l, files := range v.Levels {
		for _, f := range candidates(l, files, key) {
			if !f.holds(key) {
				continue
			}
			e, ok, err := r.getIn(ctx, f, key, h)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			if len(hits) == 0 && e.Kind != KindMerge { // the newest version decides alone
				return e.Value, e.Kind == KindPut, nil
			}
			hits = append(hits, e)
			if e.Kind != KindMerge {
				break walk
			}
		}
	}
	if len(hits) == 0 {
		return nil, false, nil
	}
	res := Resolve(&entriesIter{entries: hits}, r.Merger, true)
	if !res.SeekGE(key) {
		return nil, false, res.Err()
	}
	return res.Value(), true, nil
}

// Located is the newest version of a key: the value's extent in a table
// when Table.Single applies, otherwise the value itself.
type Located struct {
	File        FileRef // for an extent, the file it is in: its Key names the object
	Table       *Table  // for an extent, the open table to read it from; nil for a value
	Off, Length int64
	Value       []byte
}

// Locate is Get for a key space of large put-only values (Table.Single):
// the newest file holding key answers with the value's extent when the key
// is alone in a stored block, with the value when the block had to be
// decoded anyway. ok is false for a missing or deleted key. A merge entry
// is an error: Locate does not resolve merges.
func (r Reader) Locate(ctx context.Context, v Version, key []byte) (l Located, ok bool, err error) {
	h := keyHash(key)
	for l, files := range v.Levels {
		for _, f := range candidates(l, files, key) {
			if !f.holds(key) {
				continue
			}
			t, err := r.Open(ctx, f)
			if err != nil {
				return Located{}, false, fmt.Errorf("lsm: open %s: %w", f.Key, err)
			}
			if !t.filter.mayHold(h) {
				continue
			}
			if off, length, ok := t.Single(key); ok {
				return Located{File: f, Table: t, Off: off, Length: length}, true, nil
			}
			e, ok, err := t.get(ctx, key, h)
			if err != nil {
				return Located{}, false, fmt.Errorf("lsm: %s: %w", f.Key, err)
			}
			if !ok {
				continue
			}
			switch e.Kind {
			case KindPut:
				return Located{Value: e.Value}, true, nil
			case KindDelete:
				return Located{}, false, nil
			}
			return Located{}, false, fmt.Errorf("%w: lsm: merge entry under a located key", ErrCorrupt)
		}
	}
	return Located{}, false, nil
}

// getIn is one table's point lookup.
func (r Reader) getIn(ctx context.Context, f FileRef, key []byte, h uint64) (Entry, bool, error) {
	t, err := r.Open(ctx, f)
	if err != nil {
		return Entry{}, false, fmt.Errorf("lsm: open %s: %w", f.Key, err)
	}
	e, ok, err := t.get(ctx, key, h)
	if err != nil {
		return Entry{}, false, fmt.Errorf("lsm: %s: %w", f.Key, err)
	}
	return e, ok, nil
}
