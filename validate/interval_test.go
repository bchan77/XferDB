package validate

import "testing"

func TestCanonInterval(t *testing.T) {
	same := [][]string{
		{"P1Y2M", "1 year 2 mons", "14 mons"},
		{"P3DT4H5M6.789S", "3 days 04:05:06.789"},
		{"-P0DT0H0M7S", "-00:00:07"},
		{"PT0S", "00:00:00"},
		{"P1W", "7 days"},
		{"-P1Y2M", "-1 years -2 mons"},
	}
	for _, group := range same {
		want := canonInterval(group[0])
		for _, s := range group[1:] {
			if got := canonInterval(s); got != want {
				t.Errorf("canonInterval(%q) = %q, want %q (as %q)", s, got, want, group[0])
			}
		}
	}
	if canonInterval("P1M") == canonInterval("30 days") {
		t.Error("a month compared equal to 30 days")
	}
	if got := canonInterval("next tuesday"); got != "next tuesday" {
		t.Errorf("unparseable interval = %q, want the text", got)
	}
	if kindOf("interval") != kindInterval || kindOf("interval day to second(3)") != kindInterval {
		t.Error("interval types aren't kindInterval")
	}
}
