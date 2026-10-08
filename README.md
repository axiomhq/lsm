# lsm

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/axiomhq/lsm)
[![Latest Release](https://img.shields.io/github/v/release/axiomhq/lsm?style=flat-square)](https://github.com/axiomhq/lsm/releases/latest)

lsm is a log-structured merge tree for object storage. It does no I/O.

- You give it sorted entries. It gives you table bytes to store.
- You give it byte ranges. It answers reads.
- You keep the `Version` (the list of live tables) in your own manifest and
  update it with a compare-and-swap.
- Tables are written once and never modified, which suits S3-style stores.

A key's history is a stack of `Put`, `Delete`, and `Merge` versions. Reads fold
it into one value per key. Compaction does the same fold and writes the result
one level deeper.

```sh
go get github.com/axiomhq/lsm
```

## Usage

Flush, compact, read. A map stands in for the object store. This is
`ExampleCompact` in [example_test.go](example_test.go), so `go test` checks it.

```go
ctx := context.Background()

// A map stands in for your object store.
objects := map[string][]byte{}

// put stores a table and returns the object key it lives under.
put := func(ctx context.Context, level int, seq uint64, data []byte) (string, error) {
	key := fmt.Sprintf("%d-%d", level, seq)
	objects[key] = data
	return key, nil
}

// open reads a table back. A real Source reads byte ranges of an object.
open := func(ctx context.Context, f lsm.FileRef) (*lsm.Table, error) {
	return lsm.OpenTableAt(ctx, lsm.BytesSource(objects[f.Key]), f.TableMeta)
}

opts := lsm.DefaultOptions()
opts.L0Trigger = 2 // compact early, for the demo
opts.Workers = 1   // put isn't safe for concurrent use

// Two flushes, each a sorted batch with one entry per key.
var v lsm.Version
v, _, err := lsm.Flush(ctx, v, []lsm.Entry{
	{Key: []byte("a"), Kind: lsm.KindPut, Value: []byte("1")},
	{Key: []byte("b"), Kind: lsm.KindPut, Value: []byte("2")},
}, opts, put)
if err != nil {
	log.Fatal(err)
}
v, _, err = lsm.Flush(ctx, v, []lsm.Entry{
	{Key: []byte("a"), Kind: lsm.KindDelete},
	{Key: []byte("c"), Kind: lsm.KindPut, Value: []byte("3")},
}, opts, put)
if err != nil {
	log.Fatal(err)
}

// Compact until Pick has nothing left to do.
r := lsm.Reader{Open: open}
for job, ok := lsm.Pick(v, opts); ok; job, ok = lsm.Pick(v, opts) {
	if v, _, err = lsm.Compact(ctx, v, job, opts, r, put); err != nil {
		log.Fatal(err)
	}
}
fmt.Printf("level 0: %d files, level 1: %d files\n", len(v.Levels[0]), len(v.Levels[1]))

// Read it all back.
it, err := r.Iter(ctx, v, nil, nil)
if err != nil {
	log.Fatal(err)
}
for ok := it.SeekGE(nil); ok; ok = it.Next() {
	fmt.Printf("%s=%s\n", it.Key(), it.Value())
}
if err := it.Err(); err != nil {
	log.Fatal(err)
}
```

```
level 0: 0 files, level 1: 2 files
b=2
c=3
```

The delete hid `a`, and compaction into the bottom level dropped it. Level 1
has two files because `b` and `c` sit in different [key spaces](docs/guide.md#key-spaces).

## Docs

- [docs/guide.md](docs/guide.md): writing, reading, merging, compacting, options, concurrency.
- [docs/design.md](docs/design.md): comparisons, compaction policy, table format, compatibility.
- [docs/tiered.md](docs/tiered.md): tiered compaction in detail.
- [pkg.go.dev](https://pkg.go.dev/github.com/axiomhq/lsm): API reference.

## Testing

```sh
go test -race ./...
go test -run '^$' -bench . -count 10 .
```

## License

MIT, see [LICENSE](LICENSE).
