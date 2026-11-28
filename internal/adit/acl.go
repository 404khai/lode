package adit

import (
	"encoding/xml"
	"net/http"
	"strings"
)

// Lode is single-tenant: every bucket and object is private to the owner.
// ACL reads report that; writes are accepted only when they ask for exactly
// that, and anything else is refused rather than silently ignored.

type grantee struct {
	XMLNS       string `xml:"xmlns:xsi,attr,omitempty"`
	Type        string `xml:"xsi:type,attr,omitempty"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
}

type grant struct {
	Grantee    grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

type accessControlPolicy struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Owner   owner    `xml:"Owner"`
	Grants  []grant  `xml:"AccessControlList>Grant"`
}

var errACLNotSupported = errf(http.StatusNotImplemented, "NotImplemented",
	"Lode objects and buckets are private to their owner; only the 'private' ACL is supported.")

// checkCannedACL accepts requests that ask for owner-only access.
func checkCannedACL(r *http.Request) *apiError {
	switch strings.ToLower(r.Header.Get("X-Amz-Acl")) {
	case "", "private", "bucket-owner-full-control", "bucket-owner-read":
	default:
		return errACLNotSupported
	}
	for name := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-grant-") {
			return errACLNotSupported
		}
	}
	return nil
}

func (s *Server) ownerPolicy(req *request) accessControlPolicy {
	o := s.owner(req)
	return accessControlPolicy{
		Xmlns: s3Namespace, Owner: o,
		Grants: []grant{{
			Grantee:    grantee{XMLNS: "http://www.w3.org/2001/XMLSchema-instance", Type: "CanonicalUser", ID: o.ID, DisplayName: o.DisplayName},
			Permission: "FULL_CONTROL",
		}},
	}
}

func (s *Server) acl(req *request, object bool) error {
	ctx := req.r.Context()
	if object {
		if _, err := s.ledger.GetObject(ctx, req.bucket, req.key, req.r.URL.Query().Get("versionId")); err != nil {
			return err
		}
	} else if _, err := s.ledger.HeadBucket(ctx, req.bucket); err != nil {
		return err
	}
	switch req.r.Method {
	case http.MethodGet:
		req.op = map[bool]string{true: "GetObjectAcl", false: "GetBucketAcl"}[object]
		s.writeXML(req, http.StatusOK, s.ownerPolicy(req))
		return nil
	case http.MethodPut:
		req.op = map[bool]string{true: "PutObjectAcl", false: "PutBucketAcl"}[object]
		if aerr := checkCannedACL(req.r); aerr != nil {
			return aerr
		}
		body, err := s.readSmallBody(req)
		if err != nil {
			return err
		}
		if len(strings.TrimSpace(string(body))) > 0 {
			var in accessControlPolicy
			if err := xml.Unmarshal(body, &in); err != nil {
				return errMalformedXML
			}
			for _, g := range in.Grants {
				if g.Permission != "FULL_CONTROL" || g.Grantee.ID != req.auth.AccessKey {
					return errACLNotSupported
				}
			}
		}
		req.w.WriteHeader(http.StatusOK)
		return nil
	}
	return errMethodNotAllowed
}

// rejectSSE refuses server-side encryption requests: Lode does not encrypt
// at rest yet, and silently storing plaintext would be worse than failing.
func rejectSSE(r *http.Request) *apiError {
	for name := range r.Header {
		l := strings.ToLower(name)
		if strings.HasPrefix(l, "x-amz-server-side-encryption") || strings.HasPrefix(l, "x-amz-copy-source-server-side-encryption") {
			return errf(http.StatusNotImplemented, "NotImplemented", "Server-side encryption is not supported by Lode yet.")
		}
	}
	return nil
}
