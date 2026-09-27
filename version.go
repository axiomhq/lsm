package lsm

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
)

// FileRef names one table in the manifest: its object key, its sequence
// (allocation order; level-0 precedence) and what the writer learned about
// it, which a reader prunes and opens by without touching the object.
type FileRef struct {
	Key string
	Seq uint64
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

// Version is the levels of a namespace's LSM at one manifest. Level 0 is
// newest first and its files overlap; every later level is sorted by Min
// with disjoint ranges. NextSeq names the next file.
type Version struct {
	Levels  [][]FileRef
	NextSeq uint64
}

// Overlapping is the files whose range meets [lo, hi), newest first: level
// 0 in Seq order, then each level in key order. That order is MergeIter's
// precedence.
func (v Version) Overlapping(lo, hi []byte) []FileRef {
	var out []FileRef
	for _, l := range v.Levels {
		for _, f := range l {
			if f.Overlaps(lo, hi) {
				out = append(out, f)
			}
		}
	}
	return out
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
	Del []string
	Add map[int][]FileRef
}

// Apply returns the version after e. Levels grow as needed; ordering
// invariants are restored.
func (v Version) Apply(e Edit) (Version, error) {
	del := make(map[string]bool, len(e.Del))
	for _, k := range e.Del {
		del[k] = true
	}
	out := Version{NextSeq: v.NextSeq}
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
// published since. It holds when v differs from base only by level-0 files
// added (Flush): every file e deletes is still in v at its level in base,
// and levels 1 and deeper name the same files as in base. Then the outputs
// sit where the compaction put them, the added level-0 files stay newer
// than every input, and a job that dropped tombstones (Job.Bottom) still
// has nothing beneath it. Anything else, a second compaction say, is
// ErrStale.
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
		if nl, present := now[k]; !ok || !present || nl != l {
			return Version{}, fmt.Errorf("%w: %s", ErrStale, k)
		}
	}
	for l := 1; l < max(len(base.Levels), len(v.Levels)); l++ {
		var a, b []FileRef
		if l < len(base.Levels) {
			a = base.Levels[l]
		}
		if l < len(v.Levels) {
			b = v.Levels[l]
		}
		if !slices.EqualFunc(a, b, func(x, y FileRef) bool { return x.Key == y.Key }) {
			return Version{}, fmt.Errorf("%w: level %d changed", ErrStale, l)
		}
	}
	return v.Apply(e)
}

// Opener opens a file's table.
type Opener func(ctx context.Context, f FileRef) (*Table, error)

// Reader reads versions: Open opens a file's table and Merger folds merge
// operands. Merger may be nil when no key uses KindMerge.
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
			return nil, err
		}
		its = append(its, Bound(t.Iter(ctx), lo, hi))
	}
	return Resolve(NewMerge(its...), r.Merger, true), nil
}

// Get is a point lookup: tables newest first until a Put or Delete, the
// versions found resolved as Iter resolves them.
func (r Reader) Get(ctx context.Context, v Version, key []byte) ([]byte, bool, error) {
	var hits []Entry
	// The files newest first, without materialising the overlapping list:
	// a point lookup is the hottest read.
walk:
	for _, l := range v.Levels {
		for _, f := range l {
			if !f.Overlaps(key, nil) || bytes.Compare(f.Min, key) > 0 {
				continue
			}
			e, ok, err := r.getIn(ctx, f, key)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			if len(hits) == 0 && e.Kind == KindPut {
				return e.Value, true, nil
			}
			if len(hits) == 0 && e.Kind == KindDelete {
				return nil, false, nil
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
	Table       *Table
	Off, Length int64
	Value       []byte
}

// Locate is Get for a key space of large put-only values (Table.Single):
// the newest file holding key answers with the value's extent when the key
// is alone in a stored block, with the value when the block had to be
// decoded anyway. ok is false for a missing or deleted key. A merge entry
// is an error: Locate does not resolve merges.
func (r Reader) Locate(ctx context.Context, v Version, key []byte) (l Located, ok bool, err error) {
	for _, level := range v.Levels {
		for _, f := range level {
			if !f.Overlaps(key, nil) || bytes.Compare(f.Min, key) > 0 {
				continue
			}
			t, err := r.Open(ctx, f)
			if err != nil {
				return Located{}, false, err
			}
			if off, length, ok := t.Single(key); ok {
				return Located{Table: t, Off: off, Length: length}, true, nil
			}
			e, ok, err := t.Get(ctx, key)
			if err != nil {
				return Located{}, false, err
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
func (r Reader) getIn(ctx context.Context, f FileRef, key []byte) (Entry, bool, error) {
	t, err := r.Open(ctx, f)
	if err != nil {
		return Entry{}, false, err
	}
	return t.Get(ctx, key)
}
