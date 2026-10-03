package collector

import (
	"strconv"
	"strings"
)

// PathString renders tree coordinates as "1.2.3" (dot-separated port path
// from the root). Empty path renders as "".
func PathString(path []uint64) string {
	parts := make([]string, len(path))
	for i, p := range path {
		parts[i] = strconv.FormatUint(p, 10)
	}
	return strings.Join(parts, ".")
}

// PathParse is the inverse of PathString; empty string yields nil.
func PathParse(s string) ([]uint64, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ".")
	out := make([]uint64, len(parts))
	for i, p := range parts {
		v, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// PathPrefixes returns every ancestor prefix of a coords string, shortest
// first: "1.2.3" -> ["1", "1.2", "1.2.3"]. Used to place "dot" nodes on the
// tree map.
func PathPrefixes(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ".")
	out := make([]string, len(parts))
	for i := range parts {
		out[i] = strings.Join(parts[:i+1], ".")
	}
	return out
}
