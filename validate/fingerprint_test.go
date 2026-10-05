package validate

import "testing"

var fpCols = []column{
	{name: "id", tgtKey: "id", kind: kindInt},
	{name: "name", tgtKey: "name", kind: kindText},
}

func fpOf(rows []map[string]interface{}) fingerprint {
	var f fingerprint
	for _, r := range rows {
		f.add(r, fpCols, false)
	}
	return f
}

func TestFingerprintIgnoresRowOrder(t *testing.T) {
	a := fpOf([]map[string]interface{}{{"id": int64(1), "name": "a"}, {"id": int64(2), "name": "b"}, {"id": int64(3), "name": "c"}})
	b := fpOf([]map[string]interface{}{{"id": int64(3), "name": "c"}, {"id": int64(1), "name": "a"}, {"id": int64(2), "name": "b"}})
	if a != b {
		t.Fatalf("shuffled rows fingerprint differently: %s vs %s", a, b)
	}
}

func TestFingerprintDetectsDifferences(t *testing.T) {
	base := []map[string]interface{}{{"id": int64(1), "name": "a"}, {"id": int64(2), "name": "b"}}
	want := fpOf(base)

	variants := map[string][]map[string]interface{}{
		"changed value":  {{"id": int64(1), "name": "a"}, {"id": int64(2), "name": "B"}},
		"dropped row":    {{"id": int64(1), "name": "a"}},
		"duplicated row": {{"id": int64(1), "name": "a"}, {"id": int64(2), "name": "b"}, {"id": int64(2), "name": "b"}},
		"swapped values": {{"id": int64(1), "name": "b"}, {"id": int64(2), "name": "a"}},
		"null value":     {{"id": int64(1), "name": "a"}, {"id": int64(2), "name": nil}},
	}
	for name, rows := range variants {
		if got := fpOf(rows); got == want {
			t.Errorf("%s: fingerprint unchanged (%s)", name, got)
		}
	}
}

// TestFingerprintDuplicatesAreCounted checks that the same row count with a
// different duplicate ({x,x,y} vs {x,y,y}) is still caught.
func TestFingerprintDuplicatesAreCounted(t *testing.T) {
	a := fpOf([]map[string]interface{}{{"id": int64(1), "name": "x"}, {"id": int64(1), "name": "x"}, {"id": int64(2), "name": "y"}})
	b := fpOf([]map[string]interface{}{{"id": int64(1), "name": "x"}, {"id": int64(2), "name": "y"}, {"id": int64(2), "name": "y"}})
	if a == b {
		t.Fatalf("different duplicate multisets fingerprint the same: %s", a)
	}
}
