package cdc

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The disk queue sits between the capture and apply workers of one live
// migration. Capture appends decoded changes and acks the source only once
// they are fsynced here, so the source frees its log as soon as XferDB holds
// the changes; pausing apply then grows this queue, not the source's WAL.
//
// Layout of the queue directory:
//
//	00000000000000000001.seg   changes from seq 1
//	00000000000000052113.seg   changes from seq 52113 (the name is its first seq)
//	commit                     8 bytes: the last seq fsynced, readers stop there
//
// Each record is [len uint32][crc32c uint32][change encoding]. A process
// that dies mid-write leaves a torn last record; Open drops it, and drops any
// whole records after the last one carrying a Pos, since the source resends
// the transaction they belong to.

const (
	segSuffix      = ".seg"
	commitFile     = "commit"
	frameHeader    = 8
	maxRecord      = 1 << 30
	defaultSegSize = 256 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrNotFound is returned by a reader asked to start after a seq whose
// segment has already been removed.
var ErrNotFound = errors.New("cdc: queue position no longer held")

// Writer appends to a queue. There must be only one per queue directory.
type Writer struct {
	dir     string
	segSize int64

	mu      sync.Mutex
	f       *os.File
	w       *bufio.Writer
	size    int64 // bytes in the current segment
	lastSeq uint64
	lastPos Position
	posSeq  uint64 // seq of the change carrying lastPos
	synced  uint64
	commit  *os.File
	buf     []byte
	newSeg  bool // a segment was created since the last sync
}

// OpenWriter opens the queue in dir, creating it if needed, and recovers its
// end: the last record carrying a Pos. segSize is the size at which a new
// segment file starts; 0 means 256 MiB.
func OpenWriter(dir string, segSize int64) (*Writer, error) {
	if segSize <= 0 {
		segSize = defaultSegSize
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	w := &Writer{dir: dir, segSize: segSize}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	// Recover from the newest segment that still holds a positioned record;
	// segments after it hold only a transaction's unfinished tail.
	for i := len(segs) - 1; i >= 0; i-- {
		end, seq, pos, err := scanSegment(filepath.Join(dir, segName(segs[i])))
		if err != nil {
			return nil, err
		}
		if pos == nil && i > 0 {
			if err := os.Remove(filepath.Join(dir, segName(segs[i]))); err != nil {
				return nil, err
			}
			segs = segs[:i]
			continue
		}
		path := filepath.Join(dir, segName(segs[i]))
		if err := os.Truncate(path, end); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}
		w.f, w.size = f, end
		if pos != nil {
			w.lastSeq, w.lastPos, w.posSeq = seq, pos, seq
		} else {
			// The first segment with no complete transaction: it
			// restarts numbering where its name says.
			w.lastSeq = segs[i] - 1
		}
		break
	}
	if w.f == nil {
		if err := w.startSegment(1); err != nil {
			return nil, err
		}
	} else {
		w.w = bufio.NewWriterSize(w.f, 1<<20)
	}
	if w.commit, err = os.OpenFile(filepath.Join(dir, commitFile), os.O_RDWR|os.O_CREATE, 0o600); err != nil {
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	if err := w.writeCommit(w.posSeq); err != nil {
		return nil, err
	}
	w.synced = w.lastSeq
	return w, nil
}

// Last returns the seq and Pos of the last complete transaction in the
// queue; capture resumes the source from that Pos. Both are zero for an
// empty queue.
func (w *Writer) Last() (uint64, Position) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.posSeq, w.lastPos
}

// Append assigns the next seqs to changes and writes them. They are not
// visible to readers, or safe to ack to the source, until Sync returns.
func (w *Writer) Append(changes ...*Change) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range changes {
		if w.size >= w.segSize {
			if err := w.w.Flush(); err != nil {
				return err
			}
			if err := w.f.Sync(); err != nil {
				return err
			}
			if err := w.f.Close(); err != nil {
				return err
			}
			if err := w.startSegment(w.lastSeq + 1); err != nil {
				return err
			}
		}
		c.Seq = w.lastSeq + 1
		var err error
		w.buf, err = encodeChange(append(w.buf[:0], make([]byte, frameHeader)...), c)
		if err != nil {
			return err
		}
		payload := w.buf[frameHeader:]
		if len(payload) > maxRecord {
			return fmt.Errorf("cdc: change to %s is %d bytes, over the %d limit", c.Table, len(payload), maxRecord)
		}
		binary.BigEndian.PutUint32(w.buf[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(w.buf[4:8], crc32.Checksum(payload, crcTable))
		if _, err := w.w.Write(w.buf); err != nil {
			return err
		}
		w.size += int64(len(w.buf))
		w.lastSeq = c.Seq
		if c.Pos != nil {
			w.lastPos, w.posSeq = c.Pos, c.Seq
		}
	}
	return nil
}

// Sync makes everything appended so far durable, and visible to readers up
// to the last change carrying a Pos, so a reader never sees part of a
// transaction. It returns the last seq readers may now read.
func (w *Writer) Sync() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.synced == w.lastSeq {
		return w.posSeq, nil
	}
	if err := w.w.Flush(); err != nil {
		return 0, err
	}
	if err := w.f.Sync(); err != nil {
		return 0, err
	}
	if w.newSeg {
		if err := syncDir(w.dir); err != nil {
			return 0, err
		}
		w.newSeg = false
	}
	if err := w.writeCommit(w.posSeq); err != nil {
		return 0, err
	}
	w.synced = w.lastSeq
	return w.posSeq, nil
}

// Close syncs and closes the queue.
func (w *Writer) Close() error {
	_, err := w.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	if cerr := w.commit.Close(); err == nil {
		err = cerr
	}
	return err
}

func (w *Writer) startSegment(first uint64) error {
	f, err := os.OpenFile(filepath.Join(w.dir, segName(first)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	w.f, w.size, w.newSeg = f, 0, true
	w.w = bufio.NewWriterSize(f, 1<<20)
	return nil
}

// writeCommit records seq as the last one readers may read. The 8 bytes are
// written in place; the segment data they point at is already fsynced, and
// after a crash OpenWriter rewrites them from the data anyway.
func (w *Writer) writeCommit(seq uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	_, err := w.commit.WriteAt(b[:], 0)
	return err
}

// RemoveThrough deletes the segments holding only seqs up to seq, once apply
// has committed them to the target. The segment being written is kept.
func RemoveThrough(dir string, seq uint64) error {
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}
	for i := 0; i+1 < len(segs); i++ {
		if segs[i+1]-1 > seq {
			break
		}
		if err := os.Remove(filepath.Join(dir, segName(segs[i]))); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Stats describes a queue's size on disk.
type Stats struct {
	Segments  int    `json:"segments"`
	Bytes     int64  `json:"bytes"`
	FirstSeq  uint64 `json:"first_seq"`
	Committed uint64 `json:"committed_seq"`
}

// QueueStats reports what dir holds now.
func QueueStats(dir string) (Stats, error) {
	segs, err := listSegments(dir)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{Segments: len(segs)}
	if len(segs) > 0 {
		st.FirstSeq = segs[0]
	}
	for _, s := range segs {
		if fi, err := os.Stat(filepath.Join(dir, segName(s))); err == nil {
			st.Bytes += fi.Size()
		}
	}
	st.Committed, err = readCommit(dir)
	return st, err
}

// Reader reads a queue's committed changes in seq order. Any number of
// readers, in any process, may read a queue while its writer appends.
type Reader struct {
	dir     string
	next    uint64 // seq of the next change to return
	segs    []uint64
	f       *os.File
	r       *bufio.Reader
	segFrom uint64 // first seq of the open segment
	commit  uint64
	buf     []byte
	poll    time.Duration
}

// OpenReader returns a reader positioned after seq (0 = from the start).
func OpenReader(dir string, after uint64) (*Reader, error) {
	r := &Reader{dir: dir, next: after + 1, poll: 50 * time.Millisecond}
	if err := r.refresh(); err != nil {
		return nil, err
	}
	return r, nil
}

// Next returns the next committed change, waiting for one until ctx is done.
func (r *Reader) Next(ctx context.Context) (Change, error) {
	for {
		c, ok, err := r.TryNext()
		if err != nil || ok {
			return c, err
		}
		select {
		case <-ctx.Done():
			return Change{}, ctx.Err()
		case <-time.After(r.poll):
		}
	}
}

// TryNext returns the next committed change, or ok false if there is none yet.
func (r *Reader) TryNext() (Change, bool, error) {
	if r.next > r.commit {
		if err := r.refresh(); err != nil {
			return Change{}, false, err
		}
		if r.next > r.commit {
			return Change{}, false, nil
		}
	}
	for {
		if r.f == nil {
			if err := r.openSegmentFor(r.next); err != nil {
				return Change{}, false, err
			}
		}
		c, err := r.readRecord()
		if err == io.EOF {
			// The segment ended; the next seq is in the following one.
			ended := r.segFrom
			r.f.Close()
			r.f = nil
			if err := r.refresh(); err != nil {
				return Change{}, false, err
			}
			if i := sort.Search(len(r.segs), func(i int) bool { return r.segs[i] > r.next }) - 1; i < 0 || r.segs[i] == ended {
				return Change{}, false, fmt.Errorf("cdc: queue %s: seq %d is committed but missing", r.dir, r.next)
			}
			continue
		}
		if err != nil {
			return Change{}, false, err
		}
		if c.Seq < r.next {
			continue // before the start point, inside the first segment
		}
		if c.Seq != r.next {
			return Change{}, false, fmt.Errorf("cdc: queue %s: expected seq %d, found %d", r.dir, r.next, c.Seq)
		}
		r.next++
		return c, true, nil
	}
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}

func (r *Reader) refresh() error {
	c, err := readCommit(r.dir)
	if err != nil {
		return err
	}
	r.commit = c
	r.segs, err = listSegments(r.dir)
	return err
}

func (r *Reader) openSegmentFor(seq uint64) error {
	i := sort.Search(len(r.segs), func(i int) bool { return r.segs[i] > seq }) - 1
	if i < 0 {
		return ErrNotFound
	}
	f, err := os.Open(filepath.Join(r.dir, segName(r.segs[i])))
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	r.f, r.segFrom = f, r.segs[i]
	r.r = bufio.NewReaderSize(f, 1<<20)
	return nil
}

func (r *Reader) readRecord() (Change, error) {
	var hdr [frameHeader]byte
	if _, err := io.ReadFull(r.r, hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return Change{}, fmt.Errorf("cdc: queue %s: torn record below the commit point", r.dir)
		}
		return Change{}, err
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	if n > maxRecord {
		return Change{}, errCorrupt
	}
	if cap(r.buf) < int(n) {
		r.buf = make([]byte, n)
	}
	r.buf = r.buf[:n]
	if _, err := io.ReadFull(r.r, r.buf); err != nil {
		return Change{}, fmt.Errorf("cdc: queue %s: torn record below the commit point", r.dir)
	}
	if crc32.Checksum(r.buf, crcTable) != binary.BigEndian.Uint32(hdr[4:8]) {
		return Change{}, errCorrupt
	}
	return decodeChange(r.buf)
}

// scanSegment walks a segment's valid records and returns where they end,
// cut back to just after the last record with a Pos, and that record's seq
// and Pos. A segment with no positioned record returns 0 and a nil Pos.
func scanSegment(path string) (end int64, seq uint64, pos Position, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, nil, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	var off int64
	var buf []byte
	for {
		var hdr [frameHeader]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		if n > maxRecord {
			break
		}
		if cap(buf) < int(n) {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(br, buf); err != nil {
			break
		}
		if crc32.Checksum(buf, crcTable) != binary.BigEndian.Uint32(hdr[4:8]) {
			break
		}
		c, err := decodeChange(buf)
		if err != nil {
			break
		}
		off += frameHeader + int64(n)
		if c.Pos != nil {
			end, seq, pos = off, c.Seq, c.Pos
		}
	}
	return end, seq, pos, nil
}

func readCommit(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, commitFile))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(b) < 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(b[:8]), nil
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var segs []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, segSuffix) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, segSuffix), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, n)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

func segName(first uint64) string { return fmt.Sprintf("%020d%s", first, segSuffix) }

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
