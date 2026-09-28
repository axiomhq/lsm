package lsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand/v2"
	"testing"
)

// TestLargeValuesGetTheirOwnBlocks: a value of LargeValueBytes or more is
// a block of its own, and a block zstd cannot shrink is stored raw; both
// round-trip through the table reader, and a stored block's cache charge
// is its offsets alone.
func TestLargeValuesGetTheirOwnBlocks(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	w := NewTableWriter(0)
	var keys [][]byte
	for i := 0; i < 20; i++ {
		key := binary.BigEndian.AppendUint32([]byte{'F'}, uint32(i))
		keys = append(keys, key)
		var val []byte
		if i%2 == 0 {
			val = make([]byte, 8<<10) // random: incompressible, large
			for j := range val {
				val[j] = byte(rng.IntN(256))
			}
		} else {
			val = bytes.Repeat([]byte("abcd"), 64) // small and compressible
		}
		if err := w.Add(Entry{Key: key, Kind: KindPut, Value: val}); err != nil {
			t.Fatal(err)
		}
	}
	data, meta, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	tab, err := OpenTableAt(context.Background(), BytesSource(data), meta)
	if err != nil {
		t.Fatal(err)
	}
	// Ten large values: ten blocks of their own; the ten small ones share
	// the gaps between them.
	if n := len(tab.index); n < 10 || n > 20 {
		t.Fatalf("%d blocks for 10 large and 10 small values", n)
	}
	stored, zstd := 0, 0
	for i, bi := range tab.index {
		blk, err := tab.readBlock(context.Background(), i)
		if err != nil {
			t.Fatal(err)
		}
		if data[bi.off+4] == blockStored {
			stored++
			if !blk.stored || blk.Bytes() != 4*len(blk.offs) {
				t.Fatalf("stored block charged %d bytes", blk.Bytes())
			}
		} else {
			zstd++
			if blk.stored || blk.Bytes() != len(blk.raw)+4*len(blk.offs) {
				t.Fatalf("zstd block charged %d bytes", blk.Bytes())
			}
		}
		if int64(len(blk.raw)) != bi.rawLen {
			t.Fatalf("block %d raw %d want %d", i, len(blk.raw), bi.rawLen)
		}
	}
	if stored == 0 || zstd == 0 {
		t.Fatalf("stored=%d zstd=%d: want both modes", stored, zstd)
	}
	for i, key := range keys {
		e, ok, err := tab.Get(context.Background(), key)
		if err != nil || !ok {
			t.Fatalf("key %d: %v %v", i, ok, err)
		}
		if i%2 == 0 && len(e.Value) != 8<<10 || i%2 == 1 && len(e.Value) != 256 {
			t.Fatalf("key %d: %d bytes", i, len(e.Value))
		}
	}
}
