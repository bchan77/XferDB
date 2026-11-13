package validate

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Intervals compare by what they mean: months, days and microseconds, the
// three parts PostgreSQL keeps apart (a month isn't a fixed number of days).
// Sources and targets spell them differently: the Oracle source reads
// INTERVAL values as ISO 8601 (P1Y2M, P3DT4H5M6.789S), PostgreSQL returns its
// own style (1 year 2 mons, 3 days 04:05:06.789), and a target without an
// interval type keeps whichever text it was given.

var (
	isoIntervalRe = regexp.MustCompile(`^(-)?P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)W)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+(?:\.\d+)?)S)?)?$`)
	pgTimeRe      = regexp.MustCompile(`^([+-])?(\d+):(\d{2}):(\d{2}(?:\.\d+)?)$`)
)

// canonInterval renders an interval as months/days/microseconds, or as its
// text when it is in neither spelling.
func canonInterval(v interface{}) string {
	s := strings.TrimSpace(asString(v))
	if months, days, micros, ok := parseInterval(s); ok {
		return fmt.Sprintf("%dmon %dd %dus", months, days, micros)
	}
	return s
}

func parseInterval(s string) (months, days, micros int64, ok bool) {
	if m := isoIntervalRe.FindStringSubmatch(s); m != nil && s != "P" && s != "-P" {
		n := func(i int) int64 { x, _ := strconv.ParseInt(m[i], 10, 64); return x }
		months = n(2)*12 + n(3)
		days = n(4)*7 + n(5)
		secs, _ := strconv.ParseFloat(orZero(m[8]), 64)
		micros = (n(6)*3600+n(7)*60)*1e6 + int64(math.Round(secs*1e6))
		if m[1] == "-" {
			months, days, micros = -months, -days, -micros
		}
		return months, days, micros, true
	}
	return parsePGInterval(s)
}

func orZero(s string) string {
	if s == "" {
		return "0"
	}
	return s
}

// parsePGInterval reads PostgreSQL's default interval output:
// [N year[s]] [N mon[s]] [N day[s]] [[+-]HH:MM:SS[.ffffff]].
func parsePGInterval(s string) (months, days, micros int64, ok bool) {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0, 0, 0, false
	}
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		if m := pgTimeRe.FindStringSubmatch(f); m != nil {
			h, _ := strconv.ParseInt(m[2], 10, 64)
			mi, _ := strconv.ParseInt(m[3], 10, 64)
			sec, _ := strconv.ParseFloat(m[4], 64)
			t := (h*3600+mi*60)*1e6 + int64(math.Round(sec*1e6))
			if m[1] == "-" {
				t = -t
			}
			micros += t
			continue
		}
		n, err := strconv.ParseInt(f, 10, 64)
		if err != nil || i+1 >= len(fields) {
			return 0, 0, 0, false
		}
		i++
		switch strings.TrimSuffix(fields[i], "s") {
		case "year":
			months += n * 12
		case "mon":
			months += n
		case "day":
			days += n
		default:
			return 0, 0, 0, false
		}
	}
	return months, days, micros, true
}
