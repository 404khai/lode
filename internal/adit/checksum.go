package adit

import (
	"crypto/sha1" // #nosec G505 -- S3 checksum algorithm, not used for security
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"net/http"
	"strconv"
	"strings"

	"github.com/404khai/lode/internal/ledger"
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

// Multipart checksum types.
const (
	checksumComposite  = "COMPOSITE"
	checksumFullObject = "FULL_OBJECT"
)

// crcPolys are the reflected polynomials of the CRCs that support
// FULL_OBJECT multipart checksums, with their widths.
var crcPolys = map[string]struct {
	poly  uint64
	width int
}{
	"crc32":     {0xEDB88320, 32},
	"crc32c":    {0x82F63B78, 32},
	"crc64nvme": {0x9A6C9329AC4BC9B5, 64},
}

// checksumTypeFor validates algorithm+type and fills in S3's default type.
func checksumTypeFor(algo, typ string) (string, *apiError) {
	typ = strings.ToUpper(typ)
	_, isCRC := crcPolys[algo]
	switch {
	case typ == "" && algo == "crc64nvme":
		return checksumFullObject, nil
	case typ == "":
		return checksumComposite, nil
	case typ == checksumFullObject && isCRC, typ == checksumComposite && algo != "crc64nvme":
		return typ, nil
	}
	return "", errf(http.StatusBadRequest, "InvalidRequest", "The %s checksum type cannot be used with the %s checksum algorithm.", typ, algo)
}

func gf2Times(mat []uint64, vec uint64) uint64 {
	var sum uint64
	for i := 0; vec != 0; i, vec = i+1, vec>>1 {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
	}
	return sum
}

func gf2Square(square, mat []uint64) {
	for n := range mat {
		square[n] = gf2Times(mat, mat[n])
	}
}

// crcCombine returns the CRC of A||B from crc(A), crc(B) and len(B), for a
// reflected CRC with pre/post inversion (zlib's crc32_combine, generalised).
func crcCombine(crc1, crc2 uint64, len2 int64, poly uint64, width int) uint64 {
	if len2 <= 0 {
		return crc1
	}
	even, odd := make([]uint64, width), make([]uint64, width)
	odd[0] = poly
	row := uint64(1)
	for n := 1; n < width; n++ {
		odd[n] = row
		row <<= 1
	}
	gf2Square(even, odd) // two zero bits
	gf2Square(odd, even) // four zero bits
	for {
		gf2Square(even, odd)
		if len2&1 != 0 {
			crc1 = gf2Times(even, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
		gf2Square(odd, even)
		if len2&1 != 0 {
			crc1 = gf2Times(odd, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
	}
	return crc1 ^ crc2
}

func decodeCRC(b64 string, width int) (uint64, bool) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(b) != width/8 {
		return 0, false
	}
	var v uint64
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v, true
}

func encodeCRC(v uint64, width int) string {
	b := make([]byte, width/8)
	for i := len(b) - 1; i >= 0; i-- {
		b[i] = byte(v) // #nosec G115 -- intentional truncation to one byte
		v >>= 8
	}
	return base64.StdEncoding.EncodeToString(b)
}

// multipartChecksum combines part checksums into the object's checksum:
// COMPOSITE is the checksum of the concatenated raw part checksums suffixed
// with the part count; FULL_OBJECT is the CRC of the whole object.
func multipartChecksum(algo, typ string, parts []ledger.PartChecksum) (string, *apiError) {
	for _, p := range parts {
		if p.Checksum == "" {
			return "", errf(http.StatusBadRequest, "InvalidRequest", "The upload was created using a %s checksum. The complete request must include the checksum for each part. It was missing for part %d in the request.", strings.ToUpper(algo), p.Number)
		}
	}
	if typ == checksumFullObject {
		c := crcPolys[algo]
		var acc uint64
		for i, p := range parts {
			v, ok := decodeCRC(p.Checksum, c.width)
			if !ok {
				return "", errInvalidDigest
			}
			if i == 0 {
				acc = v
			} else {
				acc = crcCombine(acc, v, p.Size, c.poly, c.width)
			}
		}
		return encodeCRC(acc, c.width), nil
	}
	h := checksumAlgorithms[algo]()
	for _, p := range parts {
		raw, err := base64.StdEncoding.DecodeString(p.Checksum)
		if err != nil {
			return "", errInvalidDigest
		}
		h.Write(raw)
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts)), nil
}
