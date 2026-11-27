package adit

import (
	"encoding/hex"
	"testing"
)

func TestChecksumCheckValues(t *testing.T) {
	// Standard CRC check values for "123456789".
	want := map[string]string{
		"crc32":     "cbf43926",
		"crc32c":    "e3069283",
		"crc64nvme": "ae8b14860a799888",
	}
	for name, hexWant := range want {
		h := checksumAlgorithms[name]()
		h.Write([]byte("123456789"))
		if got := hex.EncodeToString(h.Sum(nil)); got != hexWant {
			t.Errorf("%s = %s, want %s", name, got, hexWant)
		}
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		h        string
		size     int64
		want     byteRange
		ok, fail bool
	}{
		{"bytes=0-9", 100, byteRange{0, 9}, true, false},
		{"bytes=90-200", 100, byteRange{90, 99}, true, false},
		{"bytes=95-", 100, byteRange{95, 99}, true, false},
		{"bytes=-10", 100, byteRange{90, 99}, true, false},
		{"bytes=-500", 100, byteRange{0, 99}, true, false},
		{"bytes=100-", 100, byteRange{}, false, true},
		{"bytes=-0", 100, byteRange{}, false, true},
		{"bytes=5-2", 100, byteRange{}, false, false},
		{"bytes=0-1,5-6", 100, byteRange{}, false, false},
		{"items=0-1", 100, byteRange{}, false, false},
	}
	for _, c := range cases {
		got, ok, err := parseRange(c.h, c.size)
		if (err != nil) != c.fail || ok != c.ok || got != c.want {
			t.Errorf("parseRange(%q) = %v %v %v", c.h, got, ok, err)
		}
	}
}
