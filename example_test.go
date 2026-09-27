package lsm_test

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/axiomhq/lsm"
	"github.com/axiomhq/lsm/setmerge"
)

// build flushes two batches over a map, compacts them into level 1, and
// flushes a third: an operand with no base yet.
func build(ctx context.Context) (lsm.Version, lsm.Reader) {
	objects := map[string][]byte{}
	put := func(_ context.Context, level int, seq uint64, data []byte) (string, error) {
		key := fmt.Sprintf("%d-%d", level, seq)
		objects[key] = data
		return key, nil
	}
	open := func(ctx context.Context, f lsm.FileRef) (*lsm.Table, error) {
		return lsm.OpenTableAt(ctx, lsm.BytesSource(objects[f.Key]), f.Meta())
	}
	r := lsm.Reader{Open: open, Merger: setmerge.Merger{}}
	o := lsm.DefaultOptions()
	o.L0Trigger, o.Workers = 2, 1 // one worker: put needs no lock
	e := func(k string, kind lsm.Kind, v []byte) lsm.Entry {
		return lsm.Entry{Key: []byte(k), Kind: kind, Value: v}
	}
	v, _, _ := lsm.Flush(ctx, lsm.Version{}, []lsm.Entry{e("k1", lsm.KindPut, setmerge.Value(roaring.BitmapOf(1, 2))),
		e("k2", lsm.KindPut, setmerge.Value(roaring.BitmapOf(7)))}, o, put)
	v, _, _ = lsm.Flush(ctx, v, []lsm.Entry{e("k1", lsm.KindMerge, setmerge.Operand(roaring.BitmapOf(3), roaring.BitmapOf(1))),
		e("k2", lsm.KindDelete, nil)}, o, put)
	if job, ok := lsm.Pick(v, o); ok {
		v, _, _ = lsm.Compact(ctx, v, job, o, r, put)
	}
	v, _, _ = lsm.Flush(ctx, v, []lsm.Entry{e("k3", lsm.KindMerge, setmerge.Operand(roaring.BitmapOf(5), nil))}, o, put)
	return v, r
}

func ExampleFlush() {
	ctx := context.Background()
	v, r := build(ctx)
	fmt.Println("files per level:", len(v.Levels[0]), len(v.Levels[1]))
	for _, k := range []string{"k1", "k2", "k3"} {
		val, ok, _ := r.Get(ctx, v, []byte(k))
		set, _ := setmerge.Decode(val)
		fmt.Println(k, ok, set.ToArray())
	}
	// Output:
	// files per level: 1 1
	// k1 true [2 3]
	// k2 false []
	// k3 true [5]
}

func ExampleReader_Iter() {
	ctx := context.Background()
	v, r := build(ctx)
	it, _ := r.Iter(ctx, v, nil, nil)
	for ok := it.SeekGE(nil); ok; ok = it.Next() {
		set, _ := setmerge.Decode(it.Value())
		fmt.Println(string(it.Key()), set.ToArray())
	}
	// Output:
	// k1 [2 3]
	// k3 [5]
}
