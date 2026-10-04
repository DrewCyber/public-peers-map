package collector

import "testing"

func TestCountryFromFile(t *testing.T) {
	cases := map[string]string{
		"armenia.md":       "Armenia",
		"hong-kong.md":     "Hong Kong",
		"saudi-arabia.md":  "Saudi Arabia",
		"united-states.md": "United States",
		"czechia.md":       "Czechia",
		"weird_name.md":    "Weird Name",
	}
	for file, want := range cases {
		if got := CountryFromFile(file); got != want {
			t.Errorf("CountryFromFile(%q) = %q, want %q", file, got, want)
		}
	}
}
