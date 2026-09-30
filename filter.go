package lsm

import "encoding/binary"

// A table carries a bloom filter over its keys, after the index's block
// entries. A point lookup of a key the table does not hold then reads no
// block about 99 times in 100: a key written once is looked up across
// every level-0 file and one file per deeper level, and without the filter
// each of those whose key range covers it costs a block.
//
// The filter is blocked: a key's probes all fall in one 64-byte block,
// so a lookup that walks forty tables takes one cache miss per table,
// not one per probe.
const (
	filterBitsPerKey = 10 // about 1% false positives at filterProbes
	filterProbes     = 7  // ln 2 × filterBitsPerKey
	filterBlockBytes = 64 // a cache line
	filterBlockBits  = filterBlockBytes * 8
)

// filterBytes is the size of the filter over n keys, in whole blocks.
func filterBytes(n int64) int64 {
	return max(1, (n*filterBitsPerKey+filterBlockBits-1)/filterBlockBits) * filterBlockBytes
}

// keyHash is 64-bit FNV-1a with murmur3's finalizer: its high half picks
// the filter block, its low half the probes in it. It is part of the
// table format.
func keyHash(key []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range key {
		h ^= uint64(c)
		h *= 1099511628211
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// filter is a blocked bloom filter; nil (a table written before v0.7.0)
// holds every key.
type filter []byte

// block is the filter block of h, and the first probe and the stride of
// the rest in it.
func (f filter) block(h uint64) (blk []byte, a, delta uint32) {
	i := (h >> 32) % uint64(len(f)/filterBlockBytes) * filterBlockBytes
	a = uint32(h)
	return f[i : i+filterBlockBytes], a, a>>17 | a<<15
}

func buildFilter(hashes []uint64) filter {
	f := make(filter, filterBytes(int64(len(hashes))))
	for _, h := range hashes {
		blk, a, delta := f.block(h)
		for range filterProbes {
			p := a % filterBlockBits
			blk[p/8] |= 1 << (p % 8)
			a += delta
		}
	}
	return f
}

// mayHold reports whether the table may hold the key hashing to h.
func (f filter) mayHold(h uint64) bool {
	if f == nil {
		return true
	}
	blk, a, delta := f.block(h)
	for range filterProbes {
		p := a % filterBlockBits
		if blk[p/8]&(1<<(p%8)) == 0 {
			return false
		}
		a += delta
	}
	return true
}

// filterFrame is the most the filter over n keys adds to the index: its
// length varint and its bytes.
func filterFrame(n int64) int64 { return binary.MaxVarintLen64 + filterBytes(n) }
