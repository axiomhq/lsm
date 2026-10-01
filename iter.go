package lsm

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
)

// Iterator walks entries in key order. Key and Value alias the iterator's
// buffers until the next call. An iterator is unpositioned until SeekGE.
type Iterator interface {
	SeekGE(target []byte) bool
	Next() bool
	Key() []byte
	Kind() Kind
	Value() []byte
	Err() error
}

// MergeIter merges iterators in key order; for equal keys, the iterator
// given first comes first. Callers pass newest first, so the first entry of
// each key is the winning version. Every version is emitted; Resolve
// collapses them.
type MergeIter struct {
	its  []Iterator
	keys [][]byte // each input's current key, so the heap compares without a call
	heap []int    // indices into its, ordered by (key, index)
	cur  int      // its index of the current entry, -1 when done
	err  error
}

// NewMerge merges its, newest first.
func NewMerge(its ...Iterator) *MergeIter {
	return &MergeIter{its: its, keys: make([][]byte, len(its)), heap: make([]int, 0, len(its)), cur: -1}
}

// Precedence is the index of the iterator holding the current entry, 0
// being newest.
func (m *MergeIter) Precedence() int { return m.cur }

// Source is Precedence.
//
// Deprecated: use Precedence; Source is also the name of the table byte
// source interface.
func (m *MergeIter) Source() int { return m.cur }

func (m *MergeIter) SeekGE(target []byte) bool {
	m.heap = m.heap[:0]
	m.cur = -1
	for i, it := range m.its {
		if it.SeekGE(target) {
			m.keys[i] = it.Key()
			m.heap = append(m.heap, i)
		} else if err := it.Err(); err != nil {
			m.err = err
			return false
		}
	}
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.down(i)
	}
	return m.pick()
}

func (m *MergeIter) Next() bool {
	if m.cur < 0 {
		return false
	}
	top := m.heap[0]
	it := m.its[top]
	if it.Next() {
		m.keys[top] = it.Key()
		m.down(0)
	} else {
		if err := it.Err(); err != nil {
			m.err, m.cur = err, -1
			return false
		}
		last := len(m.heap) - 1
		m.heap[0] = m.heap[last]
		m.heap = m.heap[:last]
		if last > 0 {
			m.down(0)
		}
	}
	return m.pick()
}

func (m *MergeIter) pick() bool {
	if m.err != nil || len(m.heap) == 0 {
		m.cur = -1
		return false
	}
	m.cur = m.heap[0]
	return true
}

func (m *MergeIter) less(a, b int) bool {
	if c := bytes.Compare(m.keys[a], m.keys[b]); c != 0 {
		return c < 0
	}
	return a < b
}

func (m *MergeIter) down(i int) {
	n := len(m.heap)
	for {
		l, small := 2*i+1, i
		if l < n && m.less(m.heap[l], m.heap[small]) {
			small = l
		}
		if r := l + 1; r < n && m.less(m.heap[r], m.heap[small]) {
			small = r
		}
		if small == i {
			return
		}
		m.heap[i], m.heap[small] = m.heap[small], m.heap[i]
		i = small
	}
}

func (m *MergeIter) Key() []byte   { return m.keys[m.cur] }
func (m *MergeIter) Kind() Kind    { return m.its[m.cur].Kind() }
func (m *MergeIter) Value() []byte { return m.its[m.cur].Value() }
func (m *MergeIter) Err() error    { return m.err }

// ResolveIter turns a newest-first stream of versions into one entry per
// key. It emits KindPut with the merged value; KindDelete when a key is
// absent but a table beneath the stream may still hold it (never at the
// bottom); KindMerge, one collapsed operand, when operands have no base in
// the stream and the stream is not the bottom.
type ResolveIter struct {
	in     Iterator
	merger Merger
	bottom bool
	valid  bool // in is positioned on the next unread key group
	key    []byte
	value  []byte
	kind   Kind
	ops    [][]byte
	arena  []byte
	err    error
}

// Resolve wraps in. bottom means nothing older exists beneath the stream:
// tombstones are dropped and operands are folded onto an empty base.
func Resolve(in Iterator, merger Merger, bottom bool) *ResolveIter {
	return &ResolveIter{in: in, merger: merger, bottom: bottom}
}

func (r *ResolveIter) SeekGE(target []byte) bool {
	r.valid = r.in.SeekGE(target)
	r.err = r.in.Err()
	return r.Next()
}

// Next resolves the next key group.
func (r *ResolveIter) Next() bool {
	for r.err == nil && r.valid {
		r.key = append(r.key[:0], r.in.Key()...)
		r.ops = r.ops[:0]
		r.arena = r.arena[:0]
		var base []byte
		hasBase, deleted, done := false, false, false
		for {
			if !done {
				switch r.in.Kind() {
				case KindPut:
					base = r.copy(r.in.Value())
					hasBase, done = true, true
				case KindDelete:
					deleted, done = true, true
				case KindMerge:
					r.ops = append(r.ops, r.copy(r.in.Value()))
				}
			}
			r.valid = r.in.Next()
			if !r.valid || !bytes.Equal(r.in.Key(), r.key) {
				break
			}
		}
		if r.err = r.in.Err(); r.err != nil {
			return false
		}
		if len(r.ops) == 0 {
			if hasBase {
				r.kind, r.value = KindPut, base
				return true
			}
			if r.bottom { // deleted, or nothing at all
				continue
			}
			r.kind, r.value = KindDelete, nil
			return true
		}
		slices.Reverse(r.ops) // oldest first for the merger
		if r.merger == nil {
			r.err = ErrNoMerger
			return false
		}
		if !hasBase && !deleted && !r.bottom {
			var err error
			r.value, err = r.merger.Partial(r.ops)
			if r.err = mergeErr(r.key, err); r.err != nil {
				return false
			}
			r.kind = KindMerge
			return true
		}
		if !hasBase {
			base = nil
		}
		v, keep, err := r.merger.Full(base, r.ops)
		if r.err = mergeErr(r.key, err); r.err != nil {
			return false
		}
		if keep {
			r.kind, r.value = KindPut, v
			return true
		}
		if r.bottom {
			continue
		}
		r.kind, r.value = KindDelete, nil
		return true
	}
	return false
}

// copy keeps b in the arena so the input can advance. The arena is one
// allocation per key group at most, reused across groups.
func (r *ResolveIter) copy(b []byte) []byte {
	start := len(r.arena)
	r.arena = append(r.arena, b...)
	return r.arena[start:len(r.arena):len(r.arena)]
}

func (r *ResolveIter) Key() []byte   { return r.key }
func (r *ResolveIter) Kind() Kind    { return r.kind }
func (r *ResolveIter) Value() []byte { return r.value }
func (r *ResolveIter) Err() error    { return r.err }

// mergeErr is the read path's error for a Merger failure: ErrCorrupt and
// the Merger's own error, both in the chain, with the key. nil for nil.
func mergeErr(key []byte, err error) error {
	if err != nil {
		return fmt.Errorf("%w: lsm: merge %x: %w", ErrCorrupt, key, err)
	}
	return nil
}

// boundIter holds an iterator to [lo, hi); nil bounds are open. A seek
// below lo lands on lo.
type boundIter struct {
	Iterator
	lo, hi []byte
	done   bool
}

func (b *boundIter) check(ok bool) bool {
	if !ok {
		return false
	}
	if b.hi != nil && bytes.Compare(b.Iterator.Key(), b.hi) >= 0 {
		b.done = true
		return false
	}
	return true
}

func (b *boundIter) SeekGE(target []byte) bool {
	b.done = false
	if b.lo != nil && bytes.Compare(target, b.lo) < 0 {
		target = b.lo
	}
	return b.check(b.Iterator.SeekGE(target))
}

func (b *boundIter) Next() bool {
	if b.done {
		return false
	}
	return b.check(b.Iterator.Next())
}

// Bound limits it to keys in [lo, hi): a seek below lo, nil included,
// seeks to lo, and iteration stops before hi.
func Bound(it Iterator, lo, hi []byte) Iterator {
	if lo == nil && hi == nil {
		return it
	}
	return &boundIter{Iterator: it, lo: lo, hi: hi}
}

// entriesIter iterates sorted entries already in memory: a point read's
// versions of one key, newest first, or a test's hand-built case.
type entriesIter struct {
	entries []Entry
	i       int
}

func (s *entriesIter) SeekGE(target []byte) bool {
	s.i = sort.Search(len(s.entries), func(i int) bool { return bytes.Compare(s.entries[i].Key, target) >= 0 })
	return s.i < len(s.entries)
}
func (s *entriesIter) Next() bool    { s.i++; return s.i < len(s.entries) }
func (s *entriesIter) Key() []byte   { return s.entries[s.i].Key }
func (s *entriesIter) Kind() Kind    { return s.entries[s.i].Kind }
func (s *entriesIter) Value() []byte { return s.entries[s.i].Value }
func (s *entriesIter) Err() error    { return nil }

// named is an open table with its object key, so an error read through
// its iterator names the object.
type named struct {
	key string
	t   *Table
}

func (n named) iter(ctx context.Context) Iterator { return &namedIter{n.t.Iter(ctx), n.key} }

// scan is iter for a compaction's pass over keys below hi, read window
// bytes at a time (Table.scan); a negative window reads block by block.
func (n named) scan(ctx context.Context, window int64, hi []byte) *namedIter {
	if window < 0 {
		return &namedIter{n.t.Iter(ctx), n.key}
	}
	return &namedIter{n.t.scan(ctx, window, hi), n.key}
}

type namedIter struct {
	*TableIter
	key string
}

func (it *namedIter) Err() error {
	if err := it.TableIter.Err(); err != nil {
		return fmt.Errorf("lsm: %s: %w", it.key, err)
	}
	return nil
}
