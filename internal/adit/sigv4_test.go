package adit

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	exampleAccessKey = "AKIAIOSFODNN7EXAMPLE"
	exampleSecret    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

func exampleServer() *Server {
	return &Server{
		credentials: map[string]string{exampleAccessKey: exampleSecret},
		now:         func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) },
	}
}

// AWS's published "GET Object" SigV4 example.
func TestSigV4AWSExampleGetObject(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"+
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"+
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	if _, err := exampleServer().authenticate(r); err != nil {
		t.Fatalf("AWS example signature rejected: %v", err)
	}
	r.Header.Set("Range", "bytes=0-10")
	if _, err := exampleServer().authenticate(r); err != errSignatureMismatch {
		t.Fatalf("tampered request: want SignatureDoesNotMatch, got %v", err)
	}
}

// AWS's published presigned URL example.
func TestSigV4AWSExamplePresigned(t *testing.T) {
	u := "http://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
		"&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
		"&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	r := httptest.NewRequest(http.MethodGet, u, nil)
	s := exampleServer()
	if _, err := s.authenticate(r); err != nil {
		t.Fatalf("AWS presigned example rejected: %v", err)
	}
	s.now = func() time.Time { return time.Date(2013, 5, 26, 0, 0, 0, 0, time.UTC) }
	if _, err := s.authenticate(r); err != errExpired {
		t.Fatalf("want expired, got %v", err)
	}
}

func TestSigV4Rejections(t *testing.T) {
	s := exampleServer()
	r := httptest.NewRequest(http.MethodGet, "http://h/b/k", nil)
	if _, err := s.authenticate(r); err != errAccessDenied {
		t.Fatalf("anonymous: %v", err)
	}
	r.Header.Set("Authorization", "AWS AKIA:sig")
	if _, err := s.authenticate(r); err == nil || err.Code != "AuthorizationHeaderMalformed" {
		t.Fatalf("v2 auth: %v", err)
	}
	r.Header.Set("X-Amz-Date", "20100101T000000Z")
	r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20100101/us-east-1/s3/aws4_request,SignedHeaders=host,Signature=00")
	if _, err := s.authenticate(r); err != errTimeSkewed {
		t.Fatalf("skew: %v", err)
	}
}

// AWS's published aws-chunked "PUT Object" streaming example: 66560 bytes of
// 'a' in a 65536-byte chunk, a 1024-byte chunk and a final empty chunk.
func awsChunkedExample(chunkSigs [3]string) []byte {
	var b bytes.Buffer
	b.WriteString("10000;chunk-signature=" + chunkSigs[0] + "\r\n")
	b.Write(bytes.Repeat([]byte("a"), 65536))
	b.WriteString("\r\n400;chunk-signature=" + chunkSigs[1] + "\r\n")
	b.Write(bytes.Repeat([]byte("a"), 1024))
	b.WriteString("\r\n0;chunk-signature=" + chunkSigs[2] + "\r\n\r\n")
	return b.Bytes()
}

func TestChunkedAWSExample(t *testing.T) {
	auth := &authContext{
		AmzDate:    "20130524T000000Z",
		Scope:      "20130524/us-east-1/s3/aws4_request",
		SigningKey: signingKey(exampleSecret, "20130524", "us-east-1", "s3"),
		Signature:  "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
	}
	good := [3]string{
		"ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
		"0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
		"b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
	}
	got, err := io.ReadAll(newChunkedReader(bytes.NewReader(awsChunkedExample(good)), auth, true, false))
	if err != nil {
		t.Fatalf("AWS chunked example rejected: %v", err)
	}
	if len(got) != 66560 || strings.Trim(string(got), "a") != "" {
		t.Fatalf("decoded %d bytes", len(got))
	}

	bad := good
	bad[1] = strings.Repeat("0", 64)
	_, err = io.ReadAll(newChunkedReader(bytes.NewReader(awsChunkedExample(bad)), auth, true, false))
	if !errors.Is(err, errSignatureMismatch) {
		t.Fatalf("tampered chunk: want SignatureDoesNotMatch, got %v", err)
	}
}

func TestChunkedUnsignedTrailer(t *testing.T) {
	body := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n"
	c := newChunkedReader(strings.NewReader(body), nil, false, true)
	got, err := io.ReadAll(c)
	if err != nil || string(got) != "hello" {
		t.Fatalf("got %q %v", got, err)
	}
	if c.trailers["x-amz-checksum-crc32"] != "NhCmhg==" {
		t.Fatalf("trailers = %v", c.trailers)
	}
	if _, err := io.ReadAll(newChunkedReader(strings.NewReader("5\r\nhel"), nil, false, true)); err == nil {
		t.Fatal("truncated body must fail")
	}
}

func TestURIEncode(t *testing.T) {
	cases := map[string]string{
		"/a b/c+d~e": "/a%20b/c%2Bd~e",
		"/é":         "/%C3%A9",
	}
	for in, want := range cases {
		if got := uriEncode(in, false); got != want {
			t.Errorf("uriEncode(%q) = %q, want %q", in, got, want)
		}
	}
	if got := uriEncode("a/b", true); got != "a%2Fb" {
		t.Errorf("encodeSlash: %q", got)
	}
}
