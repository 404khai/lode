package adit

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/404khai/lode/internal/ec"
	"github.com/404khai/lode/internal/ledger"
)

type lcTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type lcFilter struct {
	Prefix *string `xml:"Prefix"`
	Tag    *lcTag  `xml:"Tag"`
	And    *struct {
		Prefix *string `xml:"Prefix"`
		Tags   []lcTag `xml:"Tag"`
	} `xml:"And"`
	ObjectSizeGreaterThan *int64 `xml:"ObjectSizeGreaterThan"`
	ObjectSizeLessThan    *int64 `xml:"ObjectSizeLessThan"`
}

type lcRule struct {
	ID         string    `xml:"ID,omitempty"`
	Prefix     *string   `xml:"Prefix"` // legacy rule-level prefix
	Filter     *lcFilter `xml:"Filter"`
	Status     string    `xml:"Status"`
	Expiration *struct {
		Days                      *int    `xml:"Days"`
		Date                      *string `xml:"Date"`
		ExpiredObjectDeleteMarker *bool   `xml:"ExpiredObjectDeleteMarker"`
	} `xml:"Expiration"`
	NoncurrentVersionExpiration *struct {
		NoncurrentDays          *int `xml:"NoncurrentDays"`
		NewerNoncurrentVersions *int `xml:"NewerNoncurrentVersions"`
	} `xml:"NoncurrentVersionExpiration"`
	AbortIncompleteMultipartUpload *struct {
		DaysAfterInitiation *int `xml:"DaysAfterInitiation"`
	} `xml:"AbortIncompleteMultipartUpload"`
	Transitions []struct {
		Days         *int    `xml:"Days"`
		Date         *string `xml:"Date"`
		StorageClass string  `xml:"StorageClass"`
	} `xml:"Transition"`
	NoncurrentVersionTransitions []struct{} `xml:"NoncurrentVersionTransition"`
}

type lcConfig struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []lcRule `xml:"Rule"`
}

func lcInvalid(format string, args ...any) *apiError {
	return errf(http.StatusBadRequest, "InvalidArgument", format, args...)
}

// parseStorageClass maps a lifecycle StorageClass to a Lode layout:
// STANDARD is replicated, EC_K_M (e.g. EC_4_2) is Reed-Solomon K+M.
func parseStorageClass(c string) (k, m int, aerr *apiError) {
	if c == "STANDARD" {
		return 0, 0, nil
	}
	if _, err := fmt.Sscanf(c, "EC_%d_%d", &k, &m); err == nil && ec.Validate(k, m) == nil && c == fmt.Sprintf("EC_%d_%d", k, m) {
		return k, m, nil
	}
	return 0, 0, errf(http.StatusBadRequest, "InvalidStorageClass", "The storage class %q is not supported; use STANDARD or EC_K_M (e.g. EC_4_2)", c)
}

func positive(p *int, what string) (int, *apiError) {
	if p == nil {
		return 0, nil
	}
	if *p <= 0 {
		return 0, lcInvalid("'%s' for a lifecycle rule must be a positive integer", what)
	}
	return *p, nil
}

// parseLifecycle validates a LifecycleConfiguration document.
func parseLifecycle(body []byte) ([]ledger.LifecycleRule, *apiError) {
	var in lcConfig
	if err := xml.Unmarshal(body, &in); err != nil || len(in.Rules) == 0 || len(in.Rules) > 1000 {
		return nil, errMalformedXML
	}
	ids := map[string]bool{}
	out := make([]ledger.LifecycleRule, 0, len(in.Rules))
	for i, r := range in.Rules {
		var rule ledger.LifecycleRule
		rule.ID = r.ID
		if rule.ID == "" {
			rule.ID = fmt.Sprintf("rule-%d", i+1)
		}
		if len(rule.ID) > 255 {
			return nil, lcInvalid("ID length should not exceed allowed limit of 255")
		}
		if ids[rule.ID] {
			return nil, lcInvalid("Rule ID must be unique. Found same ID for more than one rule")
		}
		ids[rule.ID] = true
		switch r.Status {
		case "Enabled":
			rule.Enabled = true
		case "Disabled":
		default:
			return nil, errMalformedXML
		}
		switch {
		case r.Prefix != nil && r.Filter != nil:
			return nil, errMalformedXML
		case r.Prefix != nil:
			rule.Prefix = *r.Prefix
		case r.Filter != nil:
			f := r.Filter
			if f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil {
				return nil, errNotImplemented
			}
			set := 0
			if f.Prefix != nil {
				rule.Prefix = *f.Prefix
				set++
			}
			if f.Tag != nil {
				rule.Tags = map[string]string{f.Tag.Key: f.Tag.Value}
				set++
			}
			if f.And != nil {
				if f.And.Prefix != nil {
					rule.Prefix = *f.And.Prefix
				}
				rule.Tags = map[string]string{}
				for _, t := range f.And.Tags {
					rule.Tags[t.Key] = t.Value
				}
				set++
			}
			if set > 1 {
				return nil, errMalformedXML
			}
		}
		actions := 0
		var aerr *apiError
		if e := r.Expiration; e != nil {
			if (e.Days != nil) == (e.Date != nil) && e.ExpiredObjectDeleteMarker == nil {
				return nil, errMalformedXML
			}
			if rule.ExpirationDays, aerr = positive(e.Days, "Days"); aerr != nil {
				return nil, aerr
			}
			if e.Date != nil {
				t, err := time.Parse(time.RFC3339, *e.Date)
				if err != nil || !t.Equal(t.Truncate(24*time.Hour)) {
					return nil, lcInvalid("'Date' must be at midnight GMT")
				}
				rule.ExpirationDate = &t
			}
			if e.ExpiredObjectDeleteMarker != nil {
				if e.Days != nil || e.Date != nil {
					return nil, errMalformedXML
				}
				rule.ExpiredObjectDeleteMarker = *e.ExpiredObjectDeleteMarker
				if len(rule.Tags) > 0 {
					return nil, lcInvalid("ExpiredObjectDeleteMarker cannot be specified with tags")
				}
			}
			actions++
		}
		if n := r.NoncurrentVersionExpiration; n != nil {
			if n.NoncurrentDays == nil {
				return nil, errMalformedXML
			}
			if rule.NoncurrentDays, aerr = positive(n.NoncurrentDays, "NoncurrentDays"); aerr != nil {
				return nil, aerr
			}
			if rule.NewerNoncurrentVersions, aerr = positive(n.NewerNoncurrentVersions, "NewerNoncurrentVersions"); aerr != nil {
				return nil, aerr
			}
			actions++
		}
		if a := r.AbortIncompleteMultipartUpload; a != nil {
			if len(rule.Tags) > 0 {
				return nil, lcInvalid("AbortIncompleteMultipartUpload cannot be specified with tags")
			}
			if rule.AbortIncompleteDays, aerr = positive(a.DaysAfterInitiation, "DaysAfterInitiation"); aerr != nil {
				return nil, aerr
			}
			if a.DaysAfterInitiation == nil {
				return nil, errMalformedXML
			}
			actions++
		}
		for _, t := range r.Transitions {
			if t.Date != nil || t.Days == nil {
				return nil, errNotImplemented // only Days-based transitions
			}
			days, aerr := positive(t.Days, "Days")
			if aerr != nil {
				return nil, aerr
			}
			k, m, aerr := parseStorageClass(t.StorageClass)
			if aerr != nil {
				return nil, aerr
			}
			rule.Transitions = append(rule.Transitions, ledger.Transition{Days: days, Class: t.StorageClass, ECData: k, ECParity: m})
			actions++
		}
		if len(r.NoncurrentVersionTransitions) > 0 {
			return nil, errNotImplemented
		}
		if actions == 0 {
			return nil, errMalformedXML
		}
		out = append(out, rule)
	}
	return out, nil
}

// lifecycleXML renders stored rules as a LifecycleConfiguration.
func lifecycleXML(rules []ledger.LifecycleRule) []byte {
	var b strings.Builder
	b.WriteString(xml.Header + `<LifecycleConfiguration xmlns="` + s3Namespace + `">`)
	esc := func(s string) string {
		var w strings.Builder
		_ = xml.EscapeText(&w, []byte(s))
		return w.String()
	}
	for _, r := range rules {
		b.WriteString("<Rule><ID>" + esc(r.ID) + "</ID><Filter>")
		switch {
		case len(r.Tags) > 1 || (len(r.Tags) == 1 && r.Prefix != ""):
			b.WriteString("<And><Prefix>" + esc(r.Prefix) + "</Prefix>")
			for k, v := range r.Tags {
				b.WriteString("<Tag><Key>" + esc(k) + "</Key><Value>" + esc(v) + "</Value></Tag>")
			}
			b.WriteString("</And>")
		case len(r.Tags) == 1:
			for k, v := range r.Tags {
				b.WriteString("<Tag><Key>" + esc(k) + "</Key><Value>" + esc(v) + "</Value></Tag>")
			}
		default:
			b.WriteString("<Prefix>" + esc(r.Prefix) + "</Prefix>")
		}
		b.WriteString("</Filter><Status>" + map[bool]string{true: "Enabled", false: "Disabled"}[r.Enabled] + "</Status>")
		switch {
		case r.ExpirationDays > 0:
			fmt.Fprintf(&b, "<Expiration><Days>%d</Days></Expiration>", r.ExpirationDays)
		case r.ExpirationDate != nil:
			b.WriteString("<Expiration><Date>" + r.ExpirationDate.UTC().Format("2006-01-02T15:04:05.000Z") + "</Date></Expiration>")
		case r.ExpiredObjectDeleteMarker:
			b.WriteString("<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>")
		}
		for _, t := range r.Transitions {
			fmt.Fprintf(&b, "<Transition><Days>%d</Days><StorageClass>%s</StorageClass></Transition>", t.Days, esc(t.Class))
		}
		if r.NoncurrentDays > 0 {
			fmt.Fprintf(&b, "<NoncurrentVersionExpiration><NoncurrentDays>%d</NoncurrentDays>", r.NoncurrentDays)
			if r.NewerNoncurrentVersions > 0 {
				fmt.Fprintf(&b, "<NewerNoncurrentVersions>%d</NewerNoncurrentVersions>", r.NewerNoncurrentVersions)
			}
			b.WriteString("</NoncurrentVersionExpiration>")
		}
		if r.AbortIncompleteDays > 0 {
			fmt.Fprintf(&b, "<AbortIncompleteMultipartUpload><DaysAfterInitiation>%d</DaysAfterInitiation></AbortIncompleteMultipartUpload>", r.AbortIncompleteDays)
		}
		b.WriteString("</Rule>")
	}
	b.WriteString("</LifecycleConfiguration>")
	return []byte(b.String())
}

func (s *Server) lifecycle(req *request) error {
	ctx := req.r.Context()
	switch req.r.Method {
	case http.MethodGet:
		req.op = "GetBucketLifecycleConfiguration"
		rules, err := s.ledger.Lifecycle(ctx, req.bucket)
		if err != nil {
			return err
		}
		req.w.Header().Set("Content-Type", "application/xml")
		req.w.WriteHeader(http.StatusOK)
		_, _ = req.w.Write(lifecycleXML(rules))
		return nil
	case http.MethodPut:
		req.op = "PutBucketLifecycleConfiguration"
		body, err := s.readSmallBody(req)
		if err != nil {
			return err
		}
		rules, aerr := parseLifecycle(body)
		if aerr != nil {
			return aerr
		}
		if err := s.ledger.SetLifecycle(ctx, req.bucket, rules); err != nil {
			return err
		}
		s.emit("lifecycle.set", map[string]any{"bucket": req.bucket, "rules": len(rules)})
		req.w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodDelete:
		req.op = "DeleteBucketLifecycle"
		if err := s.ledger.SetLifecycle(ctx, req.bucket, nil); err != nil {
			return err
		}
		req.w.WriteHeader(http.StatusNoContent)
		return nil
	}
	return errMethodNotAllowed
}
