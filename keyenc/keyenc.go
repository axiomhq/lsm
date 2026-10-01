package keyenc

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
)

// AppendString appends s escaped: 0x00 becomes 0x00 0xFF, and 0x00 0x00
// ends it. The byte order of encodings equals the byte order of the
// strings, and a component that follows is unambiguous.
func AppendString(dst []byte, s string) []byte {
	for {
		i := strings.IndexByte(s, 0)
		if i < 0 {
			break
		}
		dst = append(append(dst, s[:i]...), 0, 0xFF)
		s = s[i+1:]
	}
	return append(append(dst, s...), 0, 0)
}

// String reads a string written by AppendString and returns the bytes
// after it. ok is false for a truncated or invalid escape.
func String(b []byte) (s string, rest []byte, ok bool) {
	var out []byte // nil until the first escape: most strings have none
	for {
		i := bytes.IndexByte(b, 0)
		if i < 0 || i+1 == len(b) {
			return "", nil, false
		}
		switch b[i+1] {
		case 0:
			if out == nil {
				return string(b[:i]), b[i+2:], true
			}
			return string(append(out, b[:i]...)), b[i+2:], true
		case 0xFF:
			out = append(append(out, b[:i]...), 0)
			b = b[i+2:]
		default:
			return "", nil, false
		}
	}
}

// AppendFloat64 appends f as 8 big-endian bytes whose byte order is the
// numeric order, negative through positive: the sign bit is flipped for
// non-negative values and every bit for negative ones. -0 is written as
// +0, so the two are one key and read back as +0. NaN is not ordered and
// does not round-trip in general: math.NaN() (sign bit clear) reads back
// as a tiny positive number, and only NaNs with the sign bit set come back
// bit for bit. Reject NaN before encoding.
func AppendFloat64(dst []byte, f float64) []byte {
	if f == 0 {
		f = 0
	}
	bits := math.Float64bits(f)
	if f >= 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return binary.BigEndian.AppendUint64(dst, bits)
}

// Float64 reads a float written by AppendFloat64 and returns the bytes
// after it. ok is false when fewer than 8 bytes remain.
func Float64(b []byte) (f float64, rest []byte, ok bool) {
	if len(b) < 8 {
		return 0, nil, false
	}
	bits := binary.BigEndian.Uint64(b)
	if bits&(1<<63) != 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits), b[8:], true
}

// PrefixEnd returns the exclusive upper bound of every key that starts
// with p, or nil when there is none (p is empty or all 0xFF).
func PrefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
