package lsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// Table layout:
//
//	block*  index  footer
//
// A block is crc32c(payload) as 4 bytes, then payload: a mode byte
// (blockZstd: zstd(raw); blockStored: raw itself, for a block zstd could
// not shrink by an eighth, which is what a block of vectors is), then the
// bytes. raw is uvarint(count), then per entry uvarint(len key) key, kind
// byte, uvarint(len value) value, keys strictly increasing. A value of
// LargeValueBytes or more is a block of its own, so a point read of a
// vector block decodes that block and nothing else. The index is
// uvarint(blocks), then per block uvarint(offset) uvarint(length)
// uvarint(raw length) uvarint(len first) first uvarint(len last) last. The
// footer is 32 bytes: index offset u64, index length u64, entry count u64,
// crc32c(index) u32, magic.
const (
	tableMagic  = "DWL1"
	footerBytes = 32
	// DefaultBlockBytes is the raw size a block is closed at.
	DefaultBlockBytes = 64 << 10
	// LargeValueBytes is the value size from which an entry is a block of
	// its own: a point read of one such value must not decode 64 KiB of
	// its neighbours to get it.
	LargeValueBytes = 4 << 10

	blockZstd   byte = 0
	blockStored byte = 1
	// MaxTableBytes bounds one table object.
	MaxTableBytes = 64 << 20
	// maxBlockBytes bounds one decoded block, whatever its index claims.
	maxBlockBytes = 1 << 30
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockEncoder compresses the blocks zstd still pays for at the fastest
// level: a flush compresses every block it writes and a compaction every
// block it rewrites, and the default level cost a write-heavy load an
// eighth of its CPU for a few percent of size.
var blockEncoder = func() *zstd.Encoder {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithZeroFrames(true))
	if err != nil {
		panic(err)
	}
	return e
}()

// blockDecoder decodes zstd blocks; its memory cap matches maxBlockBytes.
var blockDecoder = func() *zstd.Decoder {
	d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxBlockBytes))
	if err != nil {
		panic(err)
	}
	return d
}()

// decompressBounded decodes one zstd frame of at most max bytes, and
// refuses a frame whose header or output claims more: a corrupt index must
// not make a read allocate gigabytes.
func decompressBounded(data []byte, max uint64) ([]byte, error) {
	max = min(max, maxBlockBytes)
	var h zstd.Header
	if err := h.Decode(data); err != nil {
		return nil, fmt.Errorf("%w: lsm: block zstd: %v", ErrCorrupt, err)
	}
	size := uint64(0)
	if h.HasFCS {
		if h.FrameContentSize > max {
			return nil, fmt.Errorf("%w: lsm: block claims %d bytes, max %d", ErrCorrupt, h.FrameContentSize, max)
		}
		size = h.FrameContentSize
	}
	out, err := blockDecoder.DecodeAll(data, make([]byte, 0, size))
	if err != nil {
		return nil, fmt.Errorf("%w: lsm: block zstd: %v", ErrCorrupt, err)
	}
	if uint64(len(out)) > max {
		return nil, fmt.Errorf("%w: lsm: block decoded to %d bytes, max %d", ErrCorrupt, len(out), max)
	}
	return out, nil
}

var errBlockEntry = fmt.Errorf("%w: lsm: block entry", ErrCorrupt)

type blockIndex struct {
	off, length, rawLen int64
	first, last         []byte
}

// SpaceRange is the key range a table holds in one key space.
type SpaceRange struct {
	Space byte   `json:"space"`
	Min   []byte `json:"min"`
	Max   []byte `json:"max"`
}

// TableMeta is what the writer learned about the table; FileRef carries it
// in the manifest so a reader opens the table with one range read.
type TableMeta struct {
	Min, Max           []byte
	Count              uint64
	Bytes              int64
	IndexOff, IndexLen int64
	Spaces             []SpaceRange
}

// TableWriter builds one table in memory.
type TableWriter struct {
	blockBytes int
	raw        []byte // current block
	n          int    // entries in current block
	first      []byte
	out        []byte
	index      []blockIndex
	meta       TableMeta
	last       []byte
	spaces     []SpaceRange
}

// NewTableWriter returns a writer closing blocks at blockBytes of raw
// entries (DefaultBlockBytes when 0).
func NewTableWriter(blockBytes int) *TableWriter {
	if blockBytes <= 0 {
		blockBytes = DefaultBlockBytes
	}
	return &TableWriter{blockBytes: blockBytes}
}

// Add appends e. Keys must be strictly increasing and non-empty.
func (w *TableWriter) Add(e Entry) error {
	if len(e.Key) == 0 {
		return fmt.Errorf("lsm: empty key")
	}
	if w.last != nil && bytes.Compare(e.Key, w.last) <= 0 {
		return fmt.Errorf("lsm: key %x not after %x", e.Key, w.last)
	}
	if e.Kind < KindPut || e.Kind > KindMerge {
		return fmt.Errorf("lsm: bad kind %d", e.Kind)
	}
	large := len(e.Value) >= LargeValueBytes
	if w.n > 0 && (large || len(w.raw)+len(e.Value) > w.blockBytes) {
		w.flushBlock()
	}
	if w.n == 0 {
		w.first = append(w.first[:0], e.Key...)
	}
	w.raw = binary.AppendUvarint(w.raw, uint64(len(e.Key)))
	w.raw = append(w.raw, e.Key...)
	w.raw = append(w.raw, byte(e.Kind))
	w.raw = binary.AppendUvarint(w.raw, uint64(len(e.Value)))
	w.raw = append(w.raw, e.Value...)
	w.n++
	if w.meta.Count == 0 {
		w.meta.Min = bytes.Clone(e.Key)
	}
	w.meta.Count++
	space := e.Key[0]
	if k := len(w.spaces); k == 0 || w.spaces[k-1].Space != space {
		if k > 0 {
			w.spaces[k-1].Max = bytes.Clone(w.last)
		}
		w.spaces = append(w.spaces, SpaceRange{Space: space, Min: bytes.Clone(e.Key)})
	}
	w.last = append(w.last[:0], e.Key...)
	if large || len(w.raw) >= w.blockBytes {
		w.flushBlock()
	}
	return nil
}

// Bytes is the size written so far plus the open block, for splitting
// output files.
func (w *TableWriter) Bytes() int64 { return int64(len(w.out) + len(w.raw)) }

func (w *TableWriter) flushBlock() {
	if w.n == 0 {
		return
	}
	raw := binary.AppendUvarint(make([]byte, 0, len(w.raw)+8), uint64(w.n))
	raw = append(raw, w.raw...)
	// A block that is one large value (dense or already-encoded bytes,
	// such as vectors, read by the thousand) is stored as it is; the fifth
	// zstd saves on them is not worth decoding on every read. The rest is stored only when zstd
	// saves less than an eighth. A stored block's cached decode aliases the
	// table bytes and costs a checksum and nothing else.
	var payload []byte
	if w.n == 1 && len(w.raw) >= LargeValueBytes {
		payload = append([]byte{blockStored}, raw...)
	} else {
		payload = append([]byte{blockZstd}, blockEncoder.EncodeAll(raw, nil)...)
		if len(payload)-1 > len(raw)-len(raw)/8 {
			payload = append(payload[:0], blockStored)
			payload = append(payload, raw...)
		}
	}
	off := int64(len(w.out))
	w.out = binary.BigEndian.AppendUint32(w.out, crc32.Checksum(payload, castagnoli))
	w.out = append(w.out, payload...)
	w.index = append(w.index, blockIndex{off: off, length: int64(len(w.out)) - off, rawLen: int64(len(raw)),
		first: bytes.Clone(w.first), last: bytes.Clone(w.last)})
	w.raw, w.n = w.raw[:0], 0
}

// Finish closes the table and returns its bytes and metadata.
func (w *TableWriter) Finish() ([]byte, TableMeta, error) {
	if w.meta.Count == 0 {
		return nil, TableMeta{}, fmt.Errorf("lsm: empty table")
	}
	w.flushBlock()
	w.meta.Max = bytes.Clone(w.last)
	w.spaces[len(w.spaces)-1].Max = bytes.Clone(w.last)
	w.meta.Spaces = w.spaces
	idx := binary.AppendUvarint(nil, uint64(len(w.index)))
	for _, b := range w.index {
		idx = binary.AppendUvarint(idx, uint64(b.off))
		idx = binary.AppendUvarint(idx, uint64(b.length))
		idx = binary.AppendUvarint(idx, uint64(b.rawLen))
		idx = binary.AppendUvarint(idx, uint64(len(b.first)))
		idx = append(idx, b.first...)
		idx = binary.AppendUvarint(idx, uint64(len(b.last)))
		idx = append(idx, b.last...)
	}
	w.meta.IndexOff, w.meta.IndexLen = int64(len(w.out)), int64(len(idx))
	w.out = append(w.out, idx...)
	w.out = binary.BigEndian.AppendUint64(w.out, uint64(w.meta.IndexOff))
	w.out = binary.BigEndian.AppendUint64(w.out, uint64(w.meta.IndexLen))
	w.out = binary.BigEndian.AppendUint64(w.out, w.meta.Count)
	w.out = binary.BigEndian.AppendUint32(w.out, crc32.Checksum(idx, castagnoli))
	w.out = append(w.out, tableMagic...)
	w.meta.Bytes = int64(len(w.out))
	if w.meta.Bytes > MaxTableBytes {
		return nil, TableMeta{}, fmt.Errorf("lsm: table of %d bytes exceeds %d", w.meta.Bytes, MaxTableBytes)
	}
	return w.out, w.meta, nil
}

// BuildTable writes sorted, unique entries as one table.
func BuildTable(entries []Entry, blockBytes int) ([]byte, TableMeta, error) {
	w := NewTableWriter(blockBytes)
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			return nil, TableMeta{}, err
		}
	}
	return w.Finish()
}

// Source is where a table's bytes come from: an object read by range.
type Source interface {
	ReadAt(ctx context.Context, off, length int64) ([]byte, error)
}

// BytesSource is a Source over a byte slice.
type BytesSource []byte

func (b BytesSource) ReadAt(_ context.Context, off, length int64) ([]byte, error) {
	if off < 0 || length < 0 || off+length > int64(len(b)) {
		return nil, fmt.Errorf("%w: lsm: range %d+%d beyond %d bytes", ErrCorrupt, off, length, len(b))
	}
	return b[off : off+length], nil
}

// BlockCache keeps decoded blocks across reads of one table; a query's
// point lookups and a compaction's rereads then decode a block once. Get
// and Put take the table's Name and the block's stored extent, so a cache
// keyed by object ranges can hang the decode on the bytes it holds.
type BlockCache interface {
	Get(name string, off, length int64) (*Block, bool)
	Put(name string, off, length int64, b *Block)
}

// Table is an open table: its index in memory, its blocks read on demand.
// Name and Cache are optional; set by the opener when blocks should be
// cached.
type Table struct {
	src   Source
	index []blockIndex
	count uint64
	Name  string
	Cache BlockCache
}

// OpenTable reads the footer and index: two range reads. OpenTableAt is
// one when the manifest carries the index position.
func OpenTable(ctx context.Context, src Source, size int64) (*Table, error) {
	if size < footerBytes {
		return nil, fmt.Errorf("%w: lsm: table of %d bytes", ErrCorrupt, size)
	}
	f, err := src.ReadAt(ctx, size-footerBytes, footerBytes)
	if err != nil {
		return nil, err
	}
	if len(f) != footerBytes || string(f[28:]) != tableMagic {
		return nil, fmt.Errorf("%w: lsm: table footer", ErrCorrupt)
	}
	off, n := int64(binary.BigEndian.Uint64(f[0:8])), int64(binary.BigEndian.Uint64(f[8:16]))
	if off < 0 || n < 0 || off+n != size-footerBytes {
		return nil, fmt.Errorf("%w: lsm: table index position", ErrCorrupt)
	}
	return openTable(ctx, src, off, n, binary.BigEndian.Uint64(f[16:24]), binary.BigEndian.Uint32(f[24:28]), true)
}

// OpenTableAt opens a table from its FileRef metadata with one range read.
func OpenTableAt(ctx context.Context, src Source, meta TableMeta) (*Table, error) {
	return openTable(ctx, src, meta.IndexOff, meta.IndexLen, meta.Count, 0, false)
}

func openTable(ctx context.Context, src Source, off, n int64, count uint64, crc uint32, checkCRC bool) (*Table, error) {
	if n <= 0 || n > MaxTableBytes {
		return nil, fmt.Errorf("%w: lsm: table index length %d", ErrCorrupt, n)
	}
	idx, err := src.ReadAt(ctx, off, n)
	if err != nil {
		return nil, err
	}
	if int64(len(idx)) != n || (checkCRC && crc32.Checksum(idx, castagnoli) != crc) {
		return nil, fmt.Errorf("%w: lsm: table index checksum", ErrCorrupt)
	}
	t := &Table{src: src, count: count}
	d := decoder{b: idx}
	blocks := d.uvarint()
	if blocks == 0 || blocks > uint64(n) {
		return nil, fmt.Errorf("%w: lsm: table index blocks", ErrCorrupt)
	}
	t.index = make([]blockIndex, 0, blocks)
	var prevEnd int64
	var prevLast []byte
	for range blocks {
		b := blockIndex{off: int64(d.uvarint()), length: int64(d.uvarint()), rawLen: int64(d.uvarint())}
		b.first = bytes.Clone(d.bytes())
		b.last = bytes.Clone(d.bytes())
		if d.err != nil {
			return nil, fmt.Errorf("%w: lsm: table index: %v", ErrCorrupt, d.err)
		}
		if b.off != prevEnd || b.length <= 4 || b.rawLen <= 0 || b.rawLen > MaxTableBytes || b.off+b.length > off ||
			len(b.first) == 0 || bytes.Compare(b.first, b.last) > 0 || (prevLast != nil && bytes.Compare(prevLast, b.first) >= 0) {
			return nil, fmt.Errorf("%w: lsm: table index block", ErrCorrupt)
		}
		prevEnd, prevLast = b.off+b.length, b.last
		t.index = append(t.index, b)
	}
	if d.rest() != 0 {
		return nil, fmt.Errorf("%w: lsm: table index trailing bytes", ErrCorrupt)
	}
	return t, nil
}

// Blocks is the block count, for tests and accounting.
func (t *Table) Blocks() int { return len(t.index) }

// Min and Max are the table's key range.
func (t *Table) Min() []byte { return t.index[0].first }
func (t *Table) Max() []byte { return t.index[len(t.index)-1].last }

// Span is the index range [first, last] of the blocks that may hold keys
// in [lo, hi) (nil hi: unbounded), or ok false when none does. It is the
// table's own account of which of its bytes a range read touches, which
// is what a cache key for a value derived from that range hashes.
func (t *Table) Span(lo, hi []byte) (first, last int, ok bool) {
	first = sort.Search(len(t.index), func(i int) bool { return bytes.Compare(t.index[i].last, lo) >= 0 })
	if first == len(t.index) {
		return 0, 0, false
	}
	if hi == nil {
		return first, len(t.index) - 1, true
	}
	last = sort.Search(len(t.index), func(i int) bool { return bytes.Compare(t.index[i].first, hi) >= 0 }) - 1
	if last < first {
		return 0, 0, false
	}
	return first, last, true
}

// BlockModes reads the table's blocks once and reports how many, and how
// many bytes, are stored raw versus zstd: where compression still pays.
func (t *Table) BlockModes(ctx context.Context) (stored, zstd int, storedBytes, zstdBytes int64, err error) {
	if len(t.index) == 0 {
		return 0, 0, 0, 0, nil
	}
	last := t.index[len(t.index)-1]
	data, err := t.src.ReadAt(ctx, 0, last.off+last.length)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	for _, bi := range t.index {
		if bi.off+5 > int64(len(data)) {
			return 0, 0, 0, 0, fmt.Errorf("%w: lsm: block header past the table", ErrCorrupt)
		}
		switch data[bi.off+4] {
		case blockStored:
			stored++
			storedBytes += bi.length
		default:
			zstd++
			zstdBytes += bi.length
		}
	}
	return stored, zstd, storedBytes, zstdBytes, nil
}

// Block is one decoded block: entry start offsets into raw.
type Block struct {
	raw  []byte
	offs []int32
	// stored says raw aliases the table bytes the cache already holds, so
	// the block's own charge is its offsets.
	stored bool
}

// Bytes is the block's memory, for cache accounting.
func (b *Block) Bytes() int {
	if b.stored {
		return 4 * len(b.offs)
	}
	return len(b.raw) + 4*len(b.offs)
}

func (t *Table) readBlock(ctx context.Context, i int) (*Block, error) {
	bi := t.index[i]
	if t.Cache != nil {
		if b, ok := t.Cache.Get(t.Name, bi.off, bi.length); ok {
			return b, nil
		}
	}
	stored, err := t.src.ReadAt(ctx, bi.off, bi.length)
	if err != nil {
		return nil, err
	}
	b, err := decodeBlock(stored, bi.rawLen)
	if err == nil && t.Cache != nil {
		t.Cache.Put(t.Name, bi.off, bi.length, b)
	}
	return b, err
}

func decodeBlock(stored []byte, rawLen int64) (*Block, error) {
	if len(stored) < 6 || crc32.Checksum(stored[4:], castagnoli) != binary.BigEndian.Uint32(stored) {
		return nil, fmt.Errorf("%w: lsm: block checksum", ErrCorrupt)
	}
	var raw []byte
	aliased := false
	switch stored[4] {
	case blockZstd:
		var err error
		if raw, err = decompressBounded(stored[5:], uint64(rawLen)); err != nil {
			return nil, err
		}
	case blockStored:
		raw, aliased = stored[5:], true
	default:
		return nil, fmt.Errorf("%w: lsm: block mode %d", ErrCorrupt, stored[4])
	}
	if int64(len(raw)) != rawLen {
		return nil, fmt.Errorf("%w: lsm: block raw length", ErrCorrupt)
	}
	n, at := binary.Uvarint(raw)
	if at <= 0 || n == 0 || n > uint64(len(raw)) {
		return nil, fmt.Errorf("%w: lsm: block count", ErrCorrupt)
	}
	b := &Block{raw: raw, offs: make([]int32, 0, n), stored: aliased}
	// One pass proves every entry is in bounds, in order and of a known
	// kind, so entry() and key() need no checks of their own.
	var prev []byte
	bad := errBlockEntry
	for range n {
		start := at
		kl, k := binary.Uvarint(raw[at:])
		if k <= 0 || kl == 0 || kl > uint64(len(raw)-at-k) {
			return nil, bad
		}
		at += k
		key := raw[at : at+int(kl)]
		at += int(kl)
		if at >= len(raw) || Kind(raw[at]) < KindPut || Kind(raw[at]) > KindMerge {
			return nil, bad
		}
		at++
		vl, j := binary.Uvarint(raw[at:])
		if j <= 0 || vl > uint64(len(raw)-at-j) {
			return nil, bad
		}
		at += j + int(vl)
		if prev != nil && bytes.Compare(prev, key) >= 0 {
			return nil, bad
		}
		b.offs = append(b.offs, int32(start))
		prev = key
	}
	if at != len(raw) {
		return nil, fmt.Errorf("%w: lsm: block trailing bytes", ErrCorrupt)
	}
	return b, nil
}

// entry decodes entry i; the block was validated, so no error path.
func (b *Block) entry(i int) Entry {
	p := b.raw[b.offs[i]:]
	n, k := binary.Uvarint(p)
	key := p[k : k+int(n)]
	p = p[k+int(n):]
	kind := Kind(p[0])
	m, j := binary.Uvarint(p[1:])
	return Entry{Key: key, Kind: kind, Value: p[1+j : 1+j+int(m)]}
}

func (b *Block) key(i int) []byte {
	p := b.raw[b.offs[i]:]
	n, k := binary.Uvarint(p)
	return p[k : k+int(n)]
}

// search is the first entry with key >= target.
func (b *Block) search(target []byte) int {
	return sort.Search(len(b.offs), func(i int) bool { return bytes.Compare(b.key(i), target) >= 0 })
}

// Get is a point lookup: the newest version of key in this table.
func (t *Table) Get(ctx context.Context, key []byte) (Entry, bool, error) {
	i := sort.Search(len(t.index), func(i int) bool { return bytes.Compare(t.index[i].last, key) >= 0 })
	if i == len(t.index) || bytes.Compare(t.index[i].first, key) > 0 {
		return Entry{}, false, nil
	}
	b, err := t.readBlock(ctx, i)
	if err != nil {
		return Entry{}, false, err
	}
	j := b.search(key)
	if j == len(b.offs) || !bytes.Equal(b.key(j), key) {
		return Entry{}, false, nil
	}
	return b.entry(j), true, nil
}

// Single locates key's value when key is the only entry of a block stored
// raw: the value's extent in the table, computed from the index alone, so a
// reader of a slice of a large value (one row of a vectors block) fetches
// the bytes it needs and not the block. ok is false when the table has no
// block for key, the block holds other keys too, or it is compressed.
//
// The extent is the entry's value whatever its kind: the kind byte is not
// read. Callers use Single for key spaces written only with puts. A block's
// checksum covers the whole block, so bytes read through Single are not
// verified; the whole-block read path (Get, Iter) is.
func (t *Table) Single(key []byte) (off, length int64, ok bool) {
	i := sort.Search(len(t.index), func(i int) bool { return bytes.Compare(t.index[i].last, key) >= 0 })
	if i == len(t.index) || !bytes.Equal(t.index[i].first, key) || !bytes.Equal(t.index[i].last, key) {
		return 0, 0, false
	}
	bi := t.index[i]
	// flushBlock stores a block raw only when zstd would not save an eighth,
	// so a stored block is exactly crc + mode + raw and a compressed one is
	// shorter.
	if bi.length != bi.rawLen+5 {
		return 0, 0, false
	}
	// raw = uvarint(1) | uvarint(len(key)) | key | kind | uvarint(vl) | value;
	// vl is what remains after a header whose only unknown is the width of
	// vl's own varint. The width is monotone in vl, so one width fits.
	hdr := int64(1 + uvarintLen(uint64(len(key))) + len(key) + 1)
	for w := int64(1); w <= binary.MaxVarintLen64; w++ {
		vl := bi.rawLen - hdr - w
		if vl >= 0 && int64(uvarintLen(uint64(vl))) == w {
			return bi.off + 5 + hdr + w, vl, true
		}
	}
	return 0, 0, false
}

// ReadAt reads an extent of the table's bytes: what a Single extent is
// read with.
func (t *Table) ReadAt(ctx context.Context, off, length int64) ([]byte, error) {
	return t.src.ReadAt(ctx, off, length)
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// Iter returns an iterator over the table. It is not positioned until
// SeekGE.
func (t *Table) Iter(ctx context.Context) *TableIter {
	return &TableIter{ctx: ctx, t: t, bi: -1}
}

// TableIter iterates one table in key order.
type TableIter struct {
	ctx context.Context
	t   *Table
	bi  int
	blk *Block
	ei  int
	cur Entry
	err error
}

// SeekGE positions at the first entry with key >= target (nil: the first
// entry) and reports whether one exists.
func (it *TableIter) SeekGE(target []byte) bool {
	if it.err != nil {
		return false
	}
	bi := 0
	if target != nil {
		bi = sort.Search(len(it.t.index), func(i int) bool { return bytes.Compare(it.t.index[i].last, target) >= 0 })
	}
	if bi == len(it.t.index) {
		it.blk = nil
		return false
	}
	if !it.load(bi) {
		return false
	}
	it.ei = 0
	if target != nil {
		it.ei = it.blk.search(target)
	}
	if it.ei == len(it.blk.offs) { // target is between blocks
		return it.Next()
	}
	it.cur = it.blk.entry(it.ei)
	return true
}

func (it *TableIter) load(bi int) bool {
	if bi == it.bi && it.blk != nil {
		return true
	}
	blk, err := it.t.readBlock(it.ctx, bi)
	if err != nil {
		it.err, it.blk = err, nil
		return false
	}
	it.bi, it.blk = bi, blk
	return true
}

// Next advances and reports whether an entry is available.
func (it *TableIter) Next() bool {
	if it.err != nil || it.blk == nil {
		return false
	}
	it.ei++
	if it.ei >= len(it.blk.offs) {
		if it.bi+1 == len(it.t.index) {
			it.blk = nil
			return false
		}
		if !it.load(it.bi + 1) {
			return false
		}
		it.ei = 0
	}
	it.cur = it.blk.entry(it.ei)
	return true
}

func (it *TableIter) Key() []byte   { return it.cur.Key }
func (it *TableIter) Kind() Kind    { return it.cur.Kind }
func (it *TableIter) Value() []byte { return it.cur.Value }
func (it *TableIter) Err() error    { return it.err }

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = fmt.Errorf("uvarint")
		d.b = nil
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil || n > uint64(len(d.b)) {
		d.err = fmt.Errorf("bytes")
		d.b = nil
		return nil
	}
	out := d.b[:n]
	d.b = d.b[n:]
	return out
}

func (d *decoder) byte() byte {
	if len(d.b) == 0 {
		d.err = fmt.Errorf("byte")
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) rest() int { return len(d.b) }
