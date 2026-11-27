package adit

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	sigAlgorithm    = "AWS4-HMAC-SHA256"
	amzDateFormat   = "20060102T150405Z"
	maxSkew         = 15 * time.Minute
	maxPresignTTL   = 7 * 24 * time.Hour
	emptySHA256     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload = "UNSIGNED-PAYLOAD"

	streamingSigned          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingSignedTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	streamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

// authContext is the verified identity of a request plus what is needed to
// verify a streamed, chunk-signed body.
type authContext struct {
	AccessKey   string
	AmzDate     string
	Scope       string
	SigningKey  []byte
	Signature   string
	PayloadHash string
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func signingKey(secret, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	return hmacSHA256(k, "aws4_request")
}

// uriEncode applies SigV4 URI encoding: every byte except unreserved
// characters is percent-encoded; '/' is kept when encodeSlash is false.
func uriEncode(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&15])
		}
	}
	return b.String()
}

type queryPair struct{ k, v string }

func parseRawQuery(raw string) []queryPair {
	var out []queryPair
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		if uk, err := url.PathUnescape(k); err == nil {
			k = uk
		}
		if uv, err := url.PathUnescape(v); err == nil {
			v = uv
		}
		out = append(out, queryPair{k, v})
	}
	return out
}

func canonicalQuery(pairs []queryPair, skip string) string {
	enc := make([]queryPair, 0, len(pairs))
	for _, p := range pairs {
		if p.k == skip {
			continue
		}
		enc = append(enc, queryPair{uriEncode(p.k, true), uriEncode(p.v, true)})
	}
	sort.Slice(enc, func(i, j int) bool {
		if enc[i].k != enc[j].k {
			return enc[i].k < enc[j].k
		}
		return enc[i].v < enc[j].v
	})
	parts := make([]string, len(enc))
	for i, p := range enc {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func canonicalHeaderValue(vs []string) string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = strings.Join(strings.Fields(v), " ")
	}
	return strings.Join(out, ",")
}

func canonicalRequest(r *http.Request, query string, signed []string, payloadHash string) string {
	var hdrs strings.Builder
	for _, h := range signed {
		var v string
		switch h {
		case "host":
			v = r.Host
		case "content-length":
			v = r.Header.Get("Content-Length")
			if v == "" && r.ContentLength >= 0 {
				v = strconv.FormatInt(r.ContentLength, 10)
			}
		case "transfer-encoding":
			v = strings.Join(r.TransferEncoding, ",")
		default:
			v = canonicalHeaderValue(r.Header.Values(h))
		}
		hdrs.WriteString(h + ":" + v + "\n")
	}
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	return strings.Join([]string{
		r.Method, uriEncode(path, false), query, hdrs.String(), strings.Join(signed, ";"), payloadHash,
	}, "\n")
}

func stringToSign(amzDate, scope, canonical string) string {
	return sigAlgorithm + "\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonical))
}

type credential struct {
	accessKey, date, region, service string
}

func parseCredential(s string) (credential, bool) {
	p := strings.Split(s, "/")
	if len(p) != 5 || p[4] != "aws4_request" || p[3] != "s3" {
		return credential{}, false
	}
	return credential{p[0], p[1], p[2], p[3]}, true
}

func (c credential) scope() string {
	return c.date + "/" + c.region + "/" + c.service + "/aws4_request"
}

// authenticate verifies a SigV4 header-signed or presigned request.
func (s *Server) authenticate(r *http.Request) (*authContext, *apiError) {
	if r.Header.Get("Authorization") != "" {
		return s.authHeader(r)
	}
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return s.authPresigned(r)
	}
	return nil, errAccessDenied
}

func (s *Server) authHeader(r *http.Request) (*authContext, *apiError) {
	h := r.Header.Get("Authorization")
	rest, ok := strings.CutPrefix(h, sigAlgorithm+" ")
	if !ok {
		return nil, errf(http.StatusBadRequest, "AuthorizationHeaderMalformed", "Only AWS4-HMAC-SHA256 signatures are supported")
	}
	fields := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		fields[k] = v
	}
	cred, ok := parseCredential(fields["Credential"])
	if !ok || fields["SignedHeaders"] == "" || fields["Signature"] == "" {
		return nil, errf(http.StatusBadRequest, "AuthorizationHeaderMalformed", "The authorization header is malformed")
	}
	amzDate := r.Header.Get("X-Amz-Date")
	t, err := time.Parse(amzDateFormat, amzDate)
	if err != nil {
		if t, err = http.ParseTime(r.Header.Get("Date")); err != nil {
			return nil, errf(http.StatusForbidden, "AccessDenied", "AWS authentication requires a valid Date or x-amz-date header")
		}
		amzDate = t.UTC().Format(amzDateFormat)
	}
	if d := s.now().Sub(t); d > maxSkew || d < -maxSkew {
		return nil, errTimeSkewed
	}
	if cred.date != amzDate[:8] {
		return nil, errf(http.StatusBadRequest, "AuthorizationHeaderMalformed", "The credential date does not match the request date")
	}
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		return nil, errf(http.StatusBadRequest, "InvalidRequest", "Missing required header for this request: x-amz-content-sha256")
	}
	signed := strings.Split(fields["SignedHeaders"], ";")
	query := canonicalQuery(parseRawQuery(r.URL.RawQuery), "")
	return s.verify(r, cred, amzDate, query, signed, payload, fields["Signature"])
}

func (s *Server) authPresigned(r *http.Request) (*authContext, *apiError) {
	q := r.URL.Query()
	bad := func(msg string) *apiError {
		return errf(http.StatusBadRequest, "AuthorizationQueryParametersError", "%s", msg)
	}
	if q.Get("X-Amz-Algorithm") != sigAlgorithm {
		return nil, bad("X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\"")
	}
	cred, ok := parseCredential(q.Get("X-Amz-Credential"))
	if !ok {
		return nil, bad("Error parsing the X-Amz-Credential parameter")
	}
	amzDate := q.Get("X-Amz-Date")
	t, err := time.Parse(amzDateFormat, amzDate)
	if err != nil || cred.date != amzDate[:8] {
		return nil, bad("X-Amz-Date must be in the ISO8601 Long Format and match the credential date")
	}
	secs, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	ttl := time.Duration(secs) * time.Second
	if err != nil || secs < 1 || ttl > maxPresignTTL {
		return nil, bad("X-Amz-Expires must be between 1 and 604800 seconds")
	}
	now := s.now()
	if t.Sub(now) > maxSkew {
		return nil, errTimeSkewed
	}
	if now.After(t.Add(ttl)) {
		return nil, errExpired
	}
	payload := q.Get("X-Amz-Content-Sha256")
	if payload == "" {
		payload = unsignedPayload
	}
	signed := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	query := canonicalQuery(parseRawQuery(r.URL.RawQuery), "X-Amz-Signature")
	return s.verify(r, cred, amzDate, query, signed, payload, q.Get("X-Amz-Signature"))
}

func (s *Server) verify(r *http.Request, cred credential, amzDate, query string, signed []string, payload, sig string) (*authContext, *apiError) {
	secret, ok := s.credentials[cred.accessKey]
	if !ok {
		return nil, errInvalidAccessKey
	}
	key := signingKey(secret, cred.date, cred.region, cred.service)
	sts := stringToSign(amzDate, cred.scope(), canonicalRequest(r, query, signed, payload))
	want := hex.EncodeToString(hmacSHA256(key, sts))
	if subtle.ConstantTimeCompare([]byte(want), []byte(sig)) != 1 {
		return nil, errSignatureMismatch
	}
	return &authContext{
		AccessKey: cred.accessKey, AmzDate: amzDate, Scope: cred.scope(),
		SigningKey: key, Signature: sig, PayloadHash: payload,
	}, nil
}
