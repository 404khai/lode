package adit

import (
	"crypto/sha1" // #nosec G505 -- S3 checksum algorithm, not used for security
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"net/http"
	"strings"
)

var crc64NVME = crc64.MakeTable(0x9a6c9329ac4bc9b5)

var checksumAlgorithms = map[string]func() hash.Hash{
	"crc32":     func() hash.Hash { return crc32.NewIEEE() },
	"crc32c":    func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
	"crc64nvme": func() hash.Hash { return crc64.New(crc64NVME) },
	"sha1":      sha1.New,
	"sha256":    sha256.New,
}

// checksum is an additional S3 checksum (x-amz-checksum-*) being verified.
type checksum struct {
	algorithm string // lower-case, e.g. "crc32"
	expected  string // base64; empty until the trailer arrives
	trailer   bool
	h         hash.Hash
}

func (c *checksum) header() string { return "x-amz-checksum-" + c.algorithm }

func (c *checksum) sum() string { return base64.StdEncoding.EncodeToString(c.h.Sum(nil)) }

// requestChecksum returns the checksum declared by r's headers or
// x-amz-trailer, or nil if there is none.
func requestChecksum(r *http.Request) (*checksum, *apiError) {
	var found *checksum
	for name, newHash := range checksumAlgorithms {
		if v := r.Header.Get("X-Amz-Checksum-" + name); v != "" {
			if found != nil {
				return nil, errf(http.StatusBadRequest, "InvalidRequest", "Expecting a single x-amz-checksum- header")
			}
			found = &checksum{algorithm: name, expected: v, h: newHash()}
		}
	}
	if t := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Amz-Trailer"))); t != "" {
		name, ok := strings.CutPrefix(t, "x-amz-checksum-")
		newHash, known := checksumAlgorithms[name]
		if !ok || !known || found != nil {
			return nil, errf(http.StatusBadRequest, "InvalidRequest", "Unsupported or conflicting x-amz-trailer %q", t)
		}
		found = &checksum{algorithm: name, trailer: true, h: newHash()}
	}
	return found, nil
}
