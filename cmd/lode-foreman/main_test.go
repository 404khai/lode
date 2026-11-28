package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/404khai/lode/internal/config"
)

func TestAuthorize(t *testing.T) {
	keys := []config.AccessKey{{AccessKey: "ak", SecretKey: "sk"}}
	h := authorize(keys, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	for header, want := range map[string]int{
		"":               401,
		"Bearer ak":      401,
		"Bearer ak:nope": 401,
		"Basic ak:sk":    401,
		"Bearer ak:sk":   204,
	} {
		r := httptest.NewRequest("POST", "/", nil)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Errorf("%q -> %d, want %d", header, w.Code, want)
		}
	}
}
