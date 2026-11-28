package adit

import (
	"encoding/base64"
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

func TestCRCCombine(t *testing.T) {
	a, b := []byte("the quick brown fox "), []byte("jumps over the lazy dog, many times over")
	for name, c := range crcPolys {
		h := checksumAlgorithms[name]()
		h.Write(a)
		ca, _ := decodeCRC(base64.StdEncoding.EncodeToString(h.Sum(nil)), c.width)
		h = checksumAlgorithms[name]()
		h.Write(b)
		cb, _ := decodeCRC(base64.StdEncoding.EncodeToString(h.Sum(nil)), c.width)
		h = checksumAlgorithms[name]()
		h.Write(append(append([]byte(nil), a...), b...))
		want := base64.StdEncoding.EncodeToString(h.Sum(nil))
		if got := encodeCRC(crcCombine(ca, cb, int64(len(b)), c.poly, c.width), c.width); got != want {
			t.Errorf("%s combine = %s, want %s", name, got, want)
		}
	}
}

func TestChecksumTypeDefaults(t *testing.T) {
	cases := map[[2]string]string{
		{"crc64nvme", ""}: checksumFullObject, {"crc32", ""}: checksumComposite,
		{"sha256", ""}: checksumComposite, {"crc32c", "FULL_OBJECT"}: checksumFullObject,
	}
	for in, want := range cases {
		if got, err := checksumTypeFor(in[0], in[1]); err != nil || got != want {
			t.Errorf("%v = %s %v", in, got, err)
		}
	}
	for _, bad := range [][2]string{{"sha256", "FULL_OBJECT"}, {"crc64nvme", "COMPOSITE"}} {
		if _, err := checksumTypeFor(bad[0], bad[1]); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}
