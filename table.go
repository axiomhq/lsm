package lsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
// not shrink by an eighth, as a block of large incompressible values),
// then the bytes. raw is uvarint(count), then per entry uvarint(len key)
// key, kind byte, uvarint(len value) value, keys strictly increasing. A
// value of LargeValueBytes or more is a block of its own, so a point read
// of a large value decodes that block and nothing else. The index is
// uvarint(blocks), then per block uvarint(offset) uvarint(length)
// uvarint(raw length) uvarint(len first) first uvarint(len last) last. The
// footer is 32 bytes: index offset u64, index length u64, entry count u64,
// crc32c(index) u32, magic.
const (
	tableMagic  = "LSM1"
	footerBytes = 32
	// DefaultBlockBytes is the raw size a block is closed at.
	DefaultBlockBytes = 64 << 10
	// LargeValueBytes is the value size from which an entry is a block of
	// its own: a point read of one such value must not decode 64 KiB of
	// its neighbours to get it.
	LargeValueBytes = 4 << 10

	blockZstd   byte = 0
	blockStored byte = 1
	// MaxTableBytes bounds one table object, and so one decoded block and
	// one index, whatever the bytes claim.
	MaxTableBytes = 64 << 20
)

// legacyMagic is the footer magic tables carried through v0.3.0; the
// layout is the same, so a reader accepts both.
const legacyMagic = "DWL1"

// ErrUnsupportedFormat is a table whose footer magic is neither the
// current nor the legacy one: bytes from a newer format, not corruption.
var ErrUnsupportedFormat = errors.New("lsm: unsupported table format")

// MaxEntryBytes bounds one entry: its value plus three times its key (the
// key is stored once in its block and twice, as first and last, in the
// index) must fit a table of its own, so a compaction can always start a
// file with it. Writers before v0.4.0 had no per-entry check, only
// Finish's table check, so a table they wrote could hold a key larger
// than about half MaxTableBytes when earlier keys shared its block; such
// a table still reads, but a compaction over it fails with
// ErrEntryTooLarge. No such key is known to exist.
const MaxEntryBytes = MaxTableBytes - entryOverhead

// entryOverhead is the most a table of one entry adds around it: footer,
// index count, block crc, mode and count, the entry's varints and kind,
// and an index entry naming the key twice.
const entryOverhead = footerBytes + binary.MaxVarintLen64 + blockOverhead + 2*binary.MaxVarintLen64 + 1 + 5*binary.MaxVarintLen64

// blockOverhead is a block's crc, mode byte and count varint.
const blockOverhead = 5 + binary.MaxVarintLen64

// ErrEntryTooLarge is Add's refusal of an entry over MaxEntryBytes.
var ErrEntryTooLarge = errors.New("lsm: entry too large")

// entryRaw is an entry's encoded size in a block: length varints, key,
// kind and value.
func entryRaw(e Entry) int64 {
	return int64(uvarintLen(uint64(len(e.Key))) + len(e.Key) + 1 + uvarintLen(uint64(len(e.Value))) + len(e.Value))
}

// indexEntry bounds the index bytes of a block with these keys.
func indexEntry(first, last []byte) int64 {
	return int64(3*binary.MaxVarintLen64 + uvarintLen(uint64(len(first))) + len(first) + uvarintLen(uint64(len(last))) + len(last))
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockEncoder compresses the blocks zstd still pays for at the fastest
// level: a flush compresses every block it writes and a compaction every
// block it rewrites, and the default level cost a write-heavy load an
// eighth of its CPU for a few percent of size. EncodeAll is safe for
// concurrent use and runs up to GOMAXPROCS encodes at once, so
// Options.Workers is the only bound on a compaction's parallelism.
var blockEncoder = func() *zstd.Encoder {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithZeroFrames(true))
	if err != nil {
		panic(err)
	}
	return e
}()

// blockDecoder decodes zstd blocks, up to GOMAXPROCS at once (the library
// default caps at four), never more than MaxTableBytes of output per call.
var blockDecoder = func() *zstd.Decoder {
	d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(MaxTableBytes), zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic(err)
	}
	return d
}()

// DecompressBounded decodes zstd frames to at most max bytes. max is a
// ceiling, not a size the stream dictates: a frame whose header claims more
// is refused before anything is allocated, and output past max is refused
// after decoding. A frame without a content size, or trailing frames, can
// allocate up to MaxTableBytes before that check: that, not max, is the
// allocation bound. max is capped at MaxTableBytes. Every failure wraps
// ErrCorrupt.
func DecompressBounded(data []byte, max uint64) ([]byte, error) {
	max = min(max, MaxTableBytes)
	var h zstd.Header
	if err := h.Decode(data); err != nil {
		return nil, fmt.Errorf("%w: lsm: zstd: %w", ErrCorrupt, err)
	}
	size := uint64(0)
	if h.HasFCS {
		if h.FrameContentSize > max {
			return nil, fmt.Errorf("%w: lsm: zstd frame claims %d bytes, max %d", ErrCorrupt, h.FrameContentSize, max)
		}
		size = h.FrameContentSize
	}
	out, err := blockDecoder.DecodeAll(data, make([]byte, 0, size))
	if err != nil {
		return nil, fmt.Errorf("%w: lsm: zstd: %w", ErrCorrupt, err)
	}
	if uint64(len(out)) > max {
		return nil, fmt.Errorf("%w: lsm: zstd frame decoded to %d bytes, max %d", ErrCorrupt, len(out), max)
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
// in the manifest so a reader opens the table with one range read. The
// json names are the manifest encoding and are stable.
type TableMeta struct {
	Min      []byte       `json:"min"`
	Max      []byte       `json:"max"`
	Count    int64        `json:"count"`
	Bytes    int64        `json:"bytes"`
	IndexOff int64        `json:"index_off"`
	IndexLen int64        `json:"index_len"`
	Spaces   []SpaceRange `json:"spaces,omitempty"`
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
	idx        int64 // index bytes of the closed blocks
}

// NewTableWriter returns a writer closing blocks at blockBytes of raw
// entries (DefaultBlockBytes when 0).
func NewTableWriter(blockBytes int) *TableWriter {
	if blockBytes <= 0 {
		blockBytes = DefaultBlockBytes
	}
	return &TableWriter{blockBytes: blockBytes}
}

// Add appends e. Keys must be strictly increasing and non-empty, and the
// entry must fit a table of its own (ErrEntryTooLarge; see MaxEntryBytes).
// A block's raw bytes never exceed MaxTableBytes, whatever blockBytes says.
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
	if !(&TableWriter{}).fits(e, MaxTableBytes) {
		return fmt.Errorf("%w: lsm: key %.32x: value %d, key %d, max value+3*key %d", ErrEntryTooLarge, e.Key, len(e.Value), len(e.Key), MaxEntryBytes)
	}
	large := len(e.Value) >= LargeValueBytes
	if w.n > 0 && !w.joins(e) {
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

// Bytes is the data written so far plus the open block, for splitting
// output files at a target size.
func (w *TableWriter) Bytes() int64 { return int64(len(w.out) + len(w.raw)) }

// bound is the finished table's size from above: the blocks written, the
// open block as if stored raw, and the index and footer they will need.
func (w *TableWriter) bound() int64 {
	n := int64(len(w.out)) + w.idx + footerBytes + binary.MaxVarintLen64
	if w.n > 0 {
		n += blockOverhead + int64(len(w.raw)) + indexEntry(w.first, w.last)
	}
	return n
}

// joins reports whether Add would put e in the open block rather than
// close it first: the value is small, the block has room for it, and its
// raw bytes stay within MaxTableBytes.
func (w *TableWriter) joins(e Entry) bool {
	return w.n > 0 && len(e.Value) < LargeValueBytes && len(w.raw)+len(e.Value) <= w.blockBytes &&
		int64(len(w.raw))+entryRaw(e)+int64(uvarintLen(uint64(w.n+1))) <= MaxTableBytes
}

// fits reports whether the table can take e, placed as Add would place it,
// and still finish within limit (at most MaxTableBytes). Splitting output
// files on it means a table Finish refuses is never built.
func (w *TableWriter) fits(e Entry, limit int64) bool {
	cost := entryRaw(e)
	if w.joins(e) {
		cost += indexEntry(w.first, e.Key) - indexEntry(w.first, w.last) // e becomes the block's last
	} else {
		cost += blockOverhead + indexEntry(e.Key, e.Key) // a block of its own, until more arrive
	}
	return w.bound()+cost <= min(limit, MaxTableBytes)
}

func (w *TableWriter) flushBlock() {
	if w.n == 0 {
		return
	}
	raw := binary.AppendUvarint(make([]byte, 0, len(w.raw)+8), uint64(w.n))
	raw = append(raw, w.raw...)
	// A block that is one large value (dense or already-encoded bytes,
	// read by the thousand) is stored as it is; the fifth zstd saves on
	// them is not worth decoding on every read. The rest is stored only
	// when zstd saves less than an eighth. A stored block's decode aliases
	// the table bytes and costs a checksum and nothing else.
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
	w.idx += indexEntry(w.first, w.last)
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
	w.out = binary.BigEndian.AppendUint64(w.out, uint64(w.meta.Count))
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

// Source is where a table's bytes come from: an object read by range. A
// Table is read from several goroutines at once (Compact runs
// Options.Workers merges over the same inputs), so ReadAt must be safe for
// concurrent use.
type Source interface {
	ReadAt(ctx context.Context, off, length int64) ([]byte, error)
}

// BytesSource is a Source over a byte slice.
type BytesSource []byte

func (b BytesSource) ReadAt(_ context.Context, off, length int64) ([]byte, error) {
	// Subtraction, not a sum: off+length can wrap.
	if off < 0 || length < 0 || off > int64(len(b)) || length > int64(len(b))-off {
		return nil, fmt.Errorf("%w: lsm: range %d+%d beyond %d bytes", ErrCorrupt, off, length, len(b))
	}
	return b[off : off+length], nil
}

// Table is an open table: its index in memory, its blocks read on demand.
// A Table is safe for concurrent reads; a TableIter is not.
type Table struct {
	src   Source
	index []blockIndex
}

// OpenTableAt opens a table from its manifest metadata with one range read
// of the index and the footer behind it. The footer's magic must be the
// current or the legacy one, its index position must agree with meta, and
// its crc32c must match the index.
func OpenTableAt(ctx context.Context, src Source, meta TableMeta) (*Table, error) {
	off, n := meta.IndexOff, meta.IndexLen
	if off < 0 || off > MaxTableBytes || n <= 0 || n > MaxTableBytes {
		return nil, fmt.Errorf("%w: lsm: table index at %d+%d", ErrCorrupt, off, n)
	}
	buf, err := src.ReadAt(ctx, off, n+footerBytes)
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) != n+footerBytes {
		return nil, fmt.Errorf("%w: lsm: table index short read", ErrCorrupt)
	}
	idx, f := buf[:n], buf[n:]
	// The footer's index position and the magic's place are fixed across
	// formats, so a footer that disagrees with the manifest is corruption or
	// a wrong object, checked before the magic says which format this is.
	if binary.BigEndian.Uint64(f[0:8]) != uint64(off) || binary.BigEndian.Uint64(f[8:16]) != uint64(n) {
		return nil, fmt.Errorf("%w: lsm: table footer disagrees with the manifest", ErrCorrupt)
	}
	if m := string(f[28:]); m != tableMagic && m != legacyMagic {
		return nil, fmt.Errorf("%w: magic %q", ErrUnsupportedFormat, m)
	}
	if crc32.Checksum(idx, castagnoli) != binary.BigEndian.Uint32(f[24:28]) {
		return nil, fmt.Errorf("%w: lsm: table index checksum", ErrCorrupt)
	}
	t := &Table{src: src}
	d := decoder{b: idx}
	blocks := d.uvarint()
	if blocks == 0 || blocks > uint64(n)/7 { // an index entry is at least 7 bytes
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
		// b.length is bounded by subtraction: b.off+b.length can wrap.
		if b.off != prevEnd || b.length <= 4 || b.rawLen <= 0 || b.rawLen > MaxTableBytes || b.length > off-b.off ||
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

// Block is one decoded block: entry start offsets into raw.
type Block struct {
	raw  []byte
	offs []int32
}

func (t *Table) readBlock(ctx context.Context, i int) (*Block, error) {
	bi := t.index[i]
	stored, err := t.src.ReadAt(ctx, bi.off, bi.length)
	if err == nil {
		var b *Block
		if b, err = decodeBlock(stored, bi.rawLen); err == nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("lsm: block %d at %d+%d: %w", i, bi.off, bi.length, err)
}

func decodeBlock(stored []byte, rawLen int64) (*Block, error) {
	if len(stored) < 6 || crc32.Checksum(stored[4:], castagnoli) != binary.BigEndian.Uint32(stored) {
		return nil, fmt.Errorf("%w: lsm: block checksum", ErrCorrupt)
	}
	var raw []byte
	switch stored[4] {
	case blockZstd:
		var err error
		if raw, err = DecompressBounded(stored[5:], uint64(rawLen)); err != nil {
			return nil, err
		}
	case blockStored:
		raw = stored[5:]
	default:
		return nil, fmt.Errorf("%w: lsm: block mode %d", ErrCorrupt, stored[4])
	}
	if int64(len(raw)) != rawLen {
		return nil, fmt.Errorf("%w: lsm: block raw length", ErrCorrupt)
	}
	n, at := binary.Uvarint(raw)
	if at <= 0 || n == 0 || n > uint64(len(raw))/4 { // an entry is at least 4 bytes
		return nil, fmt.Errorf("%w: lsm: block count", ErrCorrupt)
	}
	b := &Block{raw: raw, offs: make([]int32, 0, n)}
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
// raw and the value is LargeValueBytes or more: the value's extent in the
// table, computed from the index alone, so a reader of a slice of a large
// value fetches the bytes it needs and not the block. ok is false when the
// table has no block for key, the block holds other keys too, it is
// compressed, or the value is small (a delete or a small put alone in a
// block, which Get reads as cheaply and with its kind).
//
// The extent is the entry's value whatever its kind: the kind byte is not
// read, so a merge operand of LargeValueBytes or more is located as if it
// were a put. Callers use Single for key spaces written only with puts. A
// block's checksum covers the whole block, so bytes read through Single are
// not verified; the whole-block read path (Get, Iter) is.
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
			if vl < LargeValueBytes {
				return 0, 0, false
			}
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

// TableIter iterates one table in key order. It stores its context
// because Next reads blocks and the Iterator interface takes no context.
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

func (d *decoder) rest() int { return len(d.b) }
