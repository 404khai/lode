package adit

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/404khai/lode/internal/ledger"
)

const (
	maxObjectTags = 10
	maxBucketTags = 50
)

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  []tagXML `xml:"TagSet>Tag"`
}

var errInvalidTag = func(msg string) *apiError { return errf(http.StatusBadRequest, "InvalidTag", "%s", msg) }

func validateTags(tags map[string]string, limit int) *apiError {
	if len(tags) > limit {
		return errf(http.StatusBadRequest, "BadRequest", "Object tags cannot be greater than %d", limit)
	}
	for k, v := range tags {
		if k == "" || len(k) > 128 {
			return errInvalidTag("The TagKey you have provided is invalid")
		}
		if len(v) > 256 {
			return errInvalidTag("The TagValue you have provided is invalid")
		}
		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return errInvalidTag("Your TagKey cannot be prefixed with aws:")
		}
	}
	return nil
}

// parseTagHeader parses x-amz-tagging (URL query encoding).
func parseTagHeader(v string, limit int) (map[string]string, *apiError) {
	q, err := url.ParseQuery(v)
	if err != nil {
		return nil, errInvalidTag("The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.")
	}
	tags := map[string]string{}
	for k, vs := range q {
		if len(vs) > 1 {
			return nil, errInvalidTag("Cannot provide multiple Tags with the same key")
		}
		tags[k] = vs[0]
	}
	return tags, validateTags(tags, limit)
}

func (s *Server) readTagging(req *request, limit int) (map[string]string, error) {
	b, err := s.readSmallBody(req)
	if err != nil {
		return nil, err
	}
	var in tagging
	if err := xml.Unmarshal(b, &in); err != nil {
		return nil, errMalformedXML
	}
	tags := map[string]string{}
	for _, t := range in.TagSet {
		if _, dup := tags[t.Key]; dup {
			return nil, errInvalidTag("Cannot provide multiple Tags with the same key")
		}
		tags[t.Key] = t.Value
	}
	if aerr := validateTags(tags, limit); aerr != nil {
		return nil, aerr
	}
	return tags, nil
}

func tagSet(tags map[string]string) tagging {
	out := tagging{Xmlns: s3Namespace}
	for k, v := range tags {
		out.TagSet = append(out.TagSet, tagXML{k, v})
	}
	sort.Slice(out.TagSet, func(i, j int) bool { return out.TagSet[i].Key < out.TagSet[j].Key })
	return out
}

func (s *Server) objectTagging(req *request) error {
	ctx, vid := req.r.Context(), req.r.URL.Query().Get("versionId")
	switch req.r.Method {
	case http.MethodGet:
		req.op = "GetObjectTagging"
		tags, v, err := s.ledger.GetObjectTags(ctx, req.bucket, req.key, vid)
		if err != nil {
			return err
		}
		if v != ledger.NullVersion {
			req.w.Header().Set("x-amz-version-id", v)
		}
		s.writeXML(req, http.StatusOK, tagSet(tags))
		return nil
	case http.MethodPut, http.MethodDelete:
		var tags map[string]string
		req.op = "DeleteObjectTagging"
		if req.r.Method == http.MethodPut {
			req.op = "PutObjectTagging"
			var err error
			if tags, err = s.readTagging(req, maxObjectTags); err != nil {
				return err
			}
		}
		v, err := s.ledger.SetObjectTags(ctx, req.bucket, req.key, vid, tags)
		if err != nil {
			return err
		}
		if v != ledger.NullVersion {
			req.w.Header().Set("x-amz-version-id", v)
		}
		s.emit("object.tagged", map[string]any{"bucket": req.bucket, "key": req.key, "version_id": v, "tags": len(tags)})
		if req.r.Method == http.MethodDelete {
			req.w.WriteHeader(http.StatusNoContent)
		} else {
			req.w.WriteHeader(http.StatusOK)
		}
		return nil
	}
	return errMethodNotAllowed
}

func (s *Server) bucketTagging(req *request) error {
	ctx := req.r.Context()
	switch req.r.Method {
	case http.MethodGet:
		req.op = "GetBucketTagging"
		tags, err := s.ledger.BucketTags(ctx, req.bucket)
		if err != nil {
			return err
		}
		if tags == nil {
			return errf(http.StatusNotFound, "NoSuchTagSet", "The TagSet does not exist")
		}
		s.writeXML(req, http.StatusOK, tagSet(tags))
		return nil
	case http.MethodPut:
		req.op = "PutBucketTagging"
		tags, err := s.readTagging(req, maxBucketTags)
		if err != nil {
			return err
		}
		if err := s.ledger.SetBucketTags(ctx, req.bucket, tags); err != nil {
			return err
		}
		req.w.WriteHeader(http.StatusNoContent)
		return nil
	case http.MethodDelete:
		req.op = "DeleteBucketTagging"
		if err := s.ledger.SetBucketTags(ctx, req.bucket, nil); err != nil {
			return err
		}
		req.w.WriteHeader(http.StatusNoContent)
		return nil
	}
	return errMethodNotAllowed
}

func setTagCount(h http.Header, tags map[string]string) {
	if len(tags) > 0 {
		h.Set("x-amz-tagging-count", strconv.Itoa(len(tags)))
	}
}
