package adit

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
)

const listBatch = 1000

type listBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Marker                *string        `xml:"Marker"`
	NextMarker            string         `xml:"NextMarker,omitempty"`
	KeyCount              *int           `xml:"KeyCount"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	Contents              []objectXML    `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *owner `xml:"Owner,omitempty"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func (s *Server) listObjects(req *request, v2 bool) error {
	q := req.r.URL.Query()
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	enc := q.Get("encoding-type")
	if enc != "" && enc != "url" {
		return errf(http.StatusBadRequest, "InvalidArgument", "Invalid Encoding Method specified in Request")
	}
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errf(http.StatusBadRequest, "InvalidArgument", "Provided max-keys not an integer or within integer range")
		}
		maxKeys = min(n, 1000)
	}

	out := listBucketResult{Xmlns: s3Namespace, Name: req.bucket, Prefix: prefix, Delimiter: delim, MaxKeys: maxKeys, EncodingType: enc}
	var after string
	if v2 {
		out.StartAfter = q.Get("start-after")
		after = out.StartAfter
		if tok := q.Get("continuation-token"); tok != "" {
			b, err := base64.RawURLEncoding.DecodeString(tok)
			if err != nil {
				return errInvalidContinuation
			}
			out.ContinuationToken = tok
			after = string(b)
		}
	} else {
		marker := q.Get("marker")
		out.Marker = &marker
		after = marker
	}
	if after < prefix {
		after = ""
	}

	// Resuming after a common prefix must skip the rest of that prefix.
	var lastCP string
	if delim != "" && strings.HasPrefix(after, prefix) && strings.HasSuffix(after, delim) &&
		strings.Contains(after[len(prefix):], delim) {
		lastCP = after
	}

	type entry struct {
		key      string
		isPrefix bool
		obj      objectXML
	}
	var entries []entry
	cursor := after
	exhausted := maxKeys == 0
	for !exhausted && len(entries) <= maxKeys {
		batch, err := s.ledger.ListObjects(req.r.Context(), req.bucket, prefix, cursor, listBatch)
		if err != nil {
			return err
		}
		if len(batch) < listBatch {
			exhausted = true
		}
		for _, o := range batch {
			cursor = o.Key
			if delim != "" {
				if i := strings.Index(o.Key[len(prefix):], delim); i >= 0 {
					cp := o.Key[:len(prefix)+i+len(delim)]
					if cp != lastCP {
						lastCP = cp
						entries = append(entries, entry{key: cp, isPrefix: true})
					}
					if len(entries) > maxKeys {
						break
					}
					continue
				}
			}
			entries = append(entries, entry{key: o.Key, obj: objectXML{
				Key: o.Key, LastModified: s3Time(o.LastModified), ETag: o.ETag, Size: o.Size, StorageClass: "STANDARD",
			}})
			if len(entries) > maxKeys {
				break
			}
		}
	}
	if maxKeys == 0 {
		batch, err := s.ledger.ListObjects(req.r.Context(), req.bucket, prefix, after, 1)
		if err != nil {
			return err
		}
		out.IsTruncated = len(batch) > 0
	}
	if len(entries) > maxKeys {
		entries = entries[:maxKeys]
		out.IsTruncated = true
	}

	encode := func(k string) string {
		if enc == "url" {
			return uriEncode(k, false)
		}
		return k
	}
	for _, e := range entries {
		if e.isPrefix {
			out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: encode(e.key)})
			continue
		}
		e.obj.Key = encode(e.obj.Key)
		if !v2 || q.Get("fetch-owner") == "true" {
			o := s.owner(req)
			e.obj.Owner = &o
		}
		out.Contents = append(out.Contents, e.obj)
	}
	if out.IsTruncated && len(entries) > 0 {
		last := entries[len(entries)-1].key
		if v2 {
			out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(last))
		} else if delim != "" {
			out.NextMarker = encode(last)
		}
	}
	if v2 {
		n := len(entries)
		out.KeyCount = &n
		out.Prefix, out.StartAfter = encode(out.Prefix), encode(out.StartAfter)
		out.Delimiter = encode(out.Delimiter)
	} else {
		out.Prefix = encode(out.Prefix)
		out.Delimiter = encode(out.Delimiter)
		m := encode(*out.Marker)
		out.Marker = &m
	}
	s.writeXML(req, http.StatusOK, out)
	return nil
}

type versionXML struct {
	XMLName      xml.Name `xml:"Version"`
	Key          string   `xml:"Key"`
	VersionID    string   `xml:"VersionId"`
	IsLatest     bool     `xml:"IsLatest"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
	Size         int64    `xml:"Size"`
	StorageClass string   `xml:"StorageClass"`
	Owner        owner    `xml:"Owner"`
}

type deleteMarkerXML struct {
	XMLName      xml.Name `xml:"DeleteMarker"`
	Key          string   `xml:"Key"`
	VersionID    string   `xml:"VersionId"`
	IsLatest     bool     `xml:"IsLatest"`
	LastModified string   `xml:"LastModified"`
	Owner        owner    `xml:"Owner"`
}

type listVersionsResult struct {
	XMLName             xml.Name       `xml:"ListVersionsResult"`
	Xmlns               string         `xml:"xmlns,attr"`
	Name                string         `xml:"Name"`
	Prefix              string         `xml:"Prefix"`
	KeyMarker           string         `xml:"KeyMarker"`
	VersionIDMarker     string         `xml:"VersionIdMarker"`
	NextKeyMarker       string         `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string         `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int            `xml:"MaxKeys"`
	Delimiter           string         `xml:"Delimiter,omitempty"`
	EncodingType        string         `xml:"EncodingType,omitempty"`
	IsTruncated         bool           `xml:"IsTruncated"`
	Entries             []any          // versionXML and deleteMarkerXML, in listing order
	CommonPrefixes      []commonPrefix `xml:"CommonPrefixes"`
}

func (s *Server) listObjectVersions(req *request) error {
	q := req.r.URL.Query()
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	enc := q.Get("encoding-type")
	if enc != "" && enc != "url" {
		return errf(http.StatusBadRequest, "InvalidArgument", "Invalid Encoding Method specified in Request")
	}
	maxKeys, err := parseMax(q.Get("max-keys"), 1000, "max-keys")
	if err != nil {
		return err
	}
	keyMarker, versionMarker := q.Get("key-marker"), q.Get("version-id-marker")
	if versionMarker != "" && keyMarker == "" {
		return errf(http.StatusBadRequest, "InvalidArgument", "A version-id marker cannot be specified without a key marker.")
	}
	encode := func(k string) string {
		if enc == "url" {
			return uriEncode(k, false)
		}
		return k
	}
	out := listVersionsResult{
		Xmlns: s3Namespace, Name: req.bucket, Prefix: encode(prefix), KeyMarker: encode(keyMarker), VersionIDMarker: versionMarker,
		MaxKeys: maxKeys, Delimiter: encode(delim), EncodingType: enc,
	}

	var lastCP string
	if delim != "" && strings.HasPrefix(keyMarker, prefix) && strings.HasSuffix(keyMarker, delim) &&
		strings.Contains(keyMarker[len(prefix):], delim) {
		lastCP = keyMarker
	}
	km, vm := keyMarker, versionMarker
	count := 0
	owner := s.owner(req)
	var nextKey, nextVersion string
scan:
	for maxKeys > 0 {
		batch, err := s.ledger.ListVersions(req.r.Context(), req.bucket, prefix, km, vm, listBatch)
		if err != nil {
			return err
		}
		for _, v := range batch {
			km, vm = v.Key, v.VersionID
			if delim != "" {
				if i := strings.Index(v.Key[len(prefix):], delim); i >= 0 {
					cp := v.Key[:len(prefix)+i+len(delim)]
					if cp == lastCP {
						continue
					}
					if count == maxKeys {
						out.IsTruncated = true
						break scan
					}
					lastCP = cp
					out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: encode(cp)})
					nextKey, nextVersion = cp, ""
					count++
					continue
				}
			}
			if count == maxKeys {
				out.IsTruncated = true
				break scan
			}
			if v.IsDeleteMarker {
				out.Entries = append(out.Entries, deleteMarkerXML{Key: encode(v.Key), VersionID: v.VersionID, IsLatest: v.IsLatest,
					LastModified: s3Time(v.LastModified), Owner: owner})
			} else {
				out.Entries = append(out.Entries, versionXML{Key: encode(v.Key), VersionID: v.VersionID, IsLatest: v.IsLatest,
					LastModified: s3Time(v.LastModified), ETag: v.ETag, Size: v.Size, StorageClass: "STANDARD", Owner: owner})
			}
			nextKey, nextVersion = v.Key, v.VersionID
			count++
		}
		if len(batch) < listBatch {
			break
		}
	}
	if out.IsTruncated {
		out.NextKeyMarker, out.NextVersionIDMarker = encode(nextKey), nextVersion
	}
	s.writeXML(req, http.StatusOK, out)
	return nil
}
