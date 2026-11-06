package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

// A hybrid time is microseconds shifted left 12 bits plus a logical
// counter: Behind is in microseconds, and anything behind is at least 1.
func TestYBBehind(t *testing.T) {
	us := func(n uint64) pglogrepl.LSN { return pglogrepl.LSN(n << 12) }
	for _, c := range []struct {
		pos, target pglogrepl.LSN
		want        int64
	}{
		{us(100), us(100), 0},
		{us(200), us(100), 0},
		{us(100), us(100) + 5, 1}, // same microsecond, later logical tick
		{us(100), us(2_500_100), 2_500_000},
	} {
		if got := ybBehind(c.pos, c.target); got != c.want {
			t.Errorf("ybBehind(%d, %d) = %d, want %d", c.pos, c.target, got, c.want)
		}
	}
}
