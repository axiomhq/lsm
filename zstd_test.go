package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"strings"
	"testing"
)

func TestDecompressBoundedRoundTrip(t *testing.T) {
	raw := bytes.Repeat([]byte("lsm round trip "), 1000)
	out, err := DecompressBounded(blockEncoder.EncodeAll(raw, nil), uint64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, raw) {
		t.Fatalf("round trip: got %d bytes, want %d", len(out), len(raw))
	}
}

func TestDecompressBoundedRefusesClaim(t *testing.T) {
	// A single-segment frame header claiming 512 MiB, then an empty last raw block.
	frame := binary.LittleEndian.AppendUint32(nil, 0xFD2FB528)
	frame = append(frame, 0xE0) // 8-byte content size, single segment
	frame = binary.LittleEndian.AppendUint64(frame, 512<<20)
	frame = append(frame, 0x01, 0x00, 0x00)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := DecompressBounded(frame, 1<<10)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "claims") {
		t.Fatalf("err = %v, want ErrCorrupt from the header check", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
		t.Fatalf("refusal allocated %d bytes", n)
	}
}

func TestDecompressBoundedRefusesLargeOutput(t *testing.T) {
	// A frame with no content size and one raw last block, so only the
	// output check sees how large it is.
	const n = 4096
	frame := binary.LittleEndian.AppendUint32(nil, 0xFD2FB528)
	frame = append(frame, 0x00, 0x58) // no content size; 2 MiB window
	frame = append(frame, byte((n<<3|1)&0xFF), byte(n>>5), byte(n>>13))
	frame = append(frame, bytes.Repeat([]byte{7}, n)...)

	if _, err := DecompressBounded(frame, n-1); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "decoded to") {
		t.Fatalf("err = %v, want ErrCorrupt from the output check", err)
	}
	out, err := DecompressBounded(frame, n)
	if err != nil || len(out) != n {
		t.Fatalf("at max: %d bytes, err %v", len(out), err)
	}
}

func TestDecompressBoundedRefusesGarbage(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("not a zstd frame"), bytes.Repeat([]byte{0xFF}, 64)} {
		if _, err := DecompressBounded(data, 1<<20); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%q: err = %v, want ErrCorrupt", data, err)
		}
	}
}
