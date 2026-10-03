package collector

import (
	"reflect"
	"testing"
)

func TestPathStringParse(t *testing.T) {
	cases := []struct {
		path []uint64
		s    string
	}{
		{nil, ""},
		{[]uint64{}, ""},
		{[]uint64{1}, "1"},
		{[]uint64{390, 4, 74}, "390.4.74"},
	}
	for _, c := range cases {
		if got := PathString(c.path); got != c.s {
			t.Errorf("PathString(%v) = %q, want %q", c.path, got, c.s)
		}
		back, err := PathParse(c.s)
		if err != nil {
			t.Fatalf("PathParse(%q): %v", c.s, err)
		}
		if c.s == "" {
			if back != nil {
				t.Errorf("PathParse(%q) = %v, want nil", c.s, back)
			}
			continue
		}
		if !reflect.DeepEqual(back, c.path) {
			t.Errorf("PathParse(%q) = %v, want %v", c.s, back, c.path)
		}
	}
}

func TestPathPrefixes(t *testing.T) {
	if got := PathPrefixes(""); got != nil {
		t.Errorf("PathPrefixes(\"\") = %v, want nil", got)
	}
	got := PathPrefixes("1.2.3")
	want := []string{"1", "1.2", "1.2.3"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PathPrefixes(\"1.2.3\") = %v, want %v", got, want)
	}
	got = PathPrefixes("7")
	if !reflect.DeepEqual(got, []string{"7"}) {
		t.Errorf("PathPrefixes(\"7\") = %v", got)
	}
}

func TestIPv6ForKey(t *testing.T) {
	// Regression vector: derived with yggdrasil-go's own address package and
	// cross-checked against the live getTree dump (2026-10-03).
	const key = "00000000000086e1e5369f2b5bef5408c1f370df5a647cdc5eb770941167f879"
	const want = "230:f23c:3592:c1a9:4821:57ee:7c19:1e41"
	got, err := IPv6ForKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("IPv6ForKey = %s, want %s", got, want)
	}
	if _, err := IPv6ForKey("zz"); err == nil {
		t.Error("IPv6ForKey accepted garbage")
	}
}
