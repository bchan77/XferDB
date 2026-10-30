package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// The queue stores changes in a small typed binary encoding rather than JSON,
// which would turn int64 into float64, []byte into base64 strings and
// time.Time into text, so apply would no longer see the values the snapshot
// read. Only these value types are allowed: they are what the database/sql
// drivers XferDB uses return from Scan.
const (
	tagNil     byte = 0
	tagFalse   byte = 1
	tagTrue    byte = 2
	tagInt64   byte = 3
	tagUint64  byte = 4
	tagFloat32 byte = 5
	tagFloat64 byte = 6
	tagString  byte = 7
	tagBytes   byte = 8
	tagTime    byte = 9
)

var errCorrupt = errors.New("cdc: corrupt change record")

// encodeChange appends c's encoding to buf.
func encodeChange(buf []byte, c *Change) ([]byte, error) {
	buf = binary.AppendUvarint(buf, c.Seq)
	buf = append(buf, byte(c.Op))
	buf = appendString(buf, c.Table)
	buf = appendBytes(buf, c.Pos)
	if c.CommitAt.IsZero() {
		buf = binary.AppendVarint(buf, 0)
	} else {
		buf = binary.AppendVarint(buf, c.CommitAt.UnixNano())
	}
	var err error
	for _, m := range []map[string]interface{}{c.Key, c.OldKey, c.After} {
		if buf, err = appendMap(buf, m); err != nil {
			return nil, fmt.Errorf("table %s: %w", c.Table, err)
		}
	}
	buf = binary.AppendUvarint(buf, uint64(len(c.Unchanged)))
	for _, s := range c.Unchanged {
		buf = appendString(buf, s)
	}
	return buf, nil
}

// decodeChange decodes one change from b.
func decodeChange(b []byte) (Change, error) {
	d := decoder{b: b}
	var c Change
	c.Seq = d.uvarint()
	c.Op = Op(d.byte())
	c.Table = d.string()
	if p := d.bytes(); len(p) > 0 {
		c.Pos = Position(p)
	}
	if ns := d.varint(); ns != 0 {
		c.CommitAt = time.Unix(0, ns).UTC()
	}
	c.Key = d.mapValue()
	c.OldKey = d.mapValue()
	c.After = d.mapValue()
	if n := d.uvarint(); n > 0 && d.err == nil {
		if n > uint64(len(d.b)) {
			return Change{}, errCorrupt
		}
		c.Unchanged = make([]string, n)
		for i := range c.Unchanged {
			c.Unchanged[i] = d.string()
		}
	}
	if d.err != nil {
		return Change{}, d.err
	}
	if len(d.b) != 0 {
		return Change{}, errCorrupt
	}
	return c, nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func appendBytes(buf []byte, b []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	return append(buf, b...)
}

// appendMap writes a map with its keys sorted, so equal changes encode to
// equal bytes. A nil map and an empty one are told apart: count 0 is nil,
// count n+1 is n entries.
func appendMap(buf []byte, m map[string]interface{}) ([]byte, error) {
	if m == nil {
		return binary.AppendUvarint(buf, 0), nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf = binary.AppendUvarint(buf, uint64(len(keys))+1)
	var err error
	for _, k := range keys {
		buf = appendString(buf, k)
		if buf, err = appendValue(buf, m[k]); err != nil {
			return nil, fmt.Errorf("column %s: %w", k, err)
		}
	}
	return buf, nil
}

func appendValue(buf []byte, v interface{}) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(buf, tagNil), nil
	case bool:
		if x {
			return append(buf, tagTrue), nil
		}
		return append(buf, tagFalse), nil
	case int64:
		return binary.AppendVarint(append(buf, tagInt64), x), nil
	case uint64:
		return binary.AppendUvarint(append(buf, tagUint64), x), nil
	case float32:
		return binary.BigEndian.AppendUint32(append(buf, tagFloat32), math.Float32bits(x)), nil
	case float64:
		return binary.BigEndian.AppendUint64(append(buf, tagFloat64), math.Float64bits(x)), nil
	case string:
		return appendString(append(buf, tagString), x), nil
	case []byte:
		return appendBytes(append(buf, tagBytes), x), nil
	case time.Time:
		// MarshalBinary keeps the instant, the wall clock's offset and
		// the monotonic-free value, which is all a target writer reads.
		b, err := x.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return appendBytes(append(buf, tagTime), b), nil
	}
	return nil, fmt.Errorf("unsupported value type %T", v)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() {
	if d.err == nil {
		d.err = errCorrupt
	}
	d.b = nil
}

func (d *decoder) byte() byte {
	if len(d.b) < 1 {
		d.fail()
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) take(n uint64) []byte {
	if n > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	out := d.b[:n:n]
	d.b = d.b[n:]
	return out
}

func (d *decoder) string() string { return string(d.take(d.uvarint())) }

// bytes returns a copy, so the decoded change doesn't pin the read buffer.
func (d *decoder) bytes() []byte {
	b := d.take(d.uvarint())
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

func (d *decoder) mapValue() map[string]interface{} {
	n := d.uvarint()
	if n == 0 || d.err != nil {
		return nil
	}
	n--
	if n > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	m := make(map[string]interface{}, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		k := d.string()
		m[k] = d.value()
	}
	return m
}

func (d *decoder) value() interface{} {
	switch d.byte() {
	case tagNil:
		return nil
	case tagFalse:
		return false
	case tagTrue:
		return true
	case tagInt64:
		return d.varint()
	case tagUint64:
		return d.uvarint()
	case tagFloat32:
		b := d.take(4)
		if b == nil {
			return nil
		}
		return math.Float32frombits(binary.BigEndian.Uint32(b))
	case tagFloat64:
		b := d.take(8)
		if b == nil {
			return nil
		}
		return math.Float64frombits(binary.BigEndian.Uint64(b))
	case tagString:
		return d.string()
	case tagBytes:
		b := d.bytes()
		if b == nil && d.err == nil {
			return []byte{}
		}
		return b
	case tagTime:
		var t time.Time
		if err := t.UnmarshalBinary(d.take(d.uvarint())); err != nil {
			d.fail()
			return nil
		}
		return t
	}
	d.fail()
	return nil
}
