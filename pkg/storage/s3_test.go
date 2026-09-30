package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fixedTime returns a now() function pinned to the AWS documentation example
// timestamp so signatures are reproducible.
func fixedTime(value time.Time) func() time.Time {
	return func() time.Time { return value }
}

// TestPresignGetMatchesAWSVector checks our SigV4 presigner against the worked
// example published in the AWS docs (GET examplebucket/test.txt), which fixes
// every input and the resulting signature. If this passes, the canonical
// request, string-to-sign and signing-key derivation are all correct.
func TestPresignGetMatchesAWSVector(t *testing.T) {
	client := NewClient(Config{
		Endpoint:        "https://s3.amazonaws.com",
		Region:          "us-east-1",
		Bucket:          "examplebucket",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		ForcePathStyle:  false, // virtual-host style, as in the AWS example
	})
	client.now = fixedTime(time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC))

	got, err := client.PresignGet("test.txt", 86400*time.Second)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}

	const wantSignature = "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	if sig := parsed.Query().Get("X-Amz-Signature"); sig != wantSignature {
		t.Fatalf("signature mismatch:\n got  %s\n want %s\n url: %s", sig, wantSignature, got)
	}
	if parsed.Host != "examplebucket.s3.amazonaws.com" {
		t.Fatalf("host = %q, want examplebucket.s3.amazonaws.com", parsed.Host)
	}
	if parsed.Path != "/test.txt" {
		t.Fatalf("path = %q, want /test.txt", parsed.Path)
	}
	if cred := parsed.Query().Get("X-Amz-Credential"); cred != "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request" {
		t.Fatalf("credential = %q", cred)
	}
}

func TestPathStyleURL(t *testing.T) {
	client := NewClient(Config{
		Endpoint:       "http://localhost:9000",
		Region:         "us-east-1",
		Bucket:         "gonvex-dev",
		ForcePathStyle: true,
	})
	urlStr, host, canonicalURI := client.buildURL("proj/tenant/abc123")
	if host != "localhost:9000" {
		t.Fatalf("host = %q, want localhost:9000", host)
	}
	if canonicalURI != "/gonvex-dev/proj/tenant/abc123" {
		t.Fatalf("canonicalURI = %q", canonicalURI)
	}
	if urlStr != "http://localhost:9000/gonvex-dev/proj/tenant/abc123" {
		t.Fatalf("urlStr = %q", urlStr)
	}
}

// TestProxyPutURLRoundTrip checks the upload-proxy signature: a freshly issued
// URL verifies, an expired or tampered one does not, and a download (GET)
// signature cannot be replayed to authorize an upload (domain separation).
func TestProxyPutURLRoundTrip(t *testing.T) {
	client := NewClient(Config{
		Endpoint:        "http://minio:9000",
		Region:          "us-east-1",
		Bucket:          "gonvex-dev",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		ForcePathStyle:  true,
		PublicBaseURL:   "https://runtime.example.com",
	})
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	client.now = fixedTime(now)

	const key = "proj/tenant/file123"
	raw := client.ProxyPutURL(key, 15*time.Minute)
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	if parsed.Host != "runtime.example.com" {
		t.Fatalf("host = %q, want runtime.example.com", parsed.Host)
	}
	if parsed.Path != "/storage/proj/tenant/file123" {
		t.Fatalf("path = %q", parsed.Path)
	}
	if parsed.Query().Get("upload") != "1" {
		t.Fatalf("missing upload=1 marker: %s", raw)
	}
	exp, _ := strconv.ParseInt(parsed.Query().Get("exp"), 10, 64)
	sig := parsed.Query().Get("sig")
	if !client.VerifyProxyPut(key, exp, sig) {
		t.Fatalf("freshly issued upload signature did not verify")
	}
	// Expired.
	if client.VerifyProxyPut(key, now.Add(-time.Second).Unix(), sig) {
		t.Fatalf("expired upload signature verified")
	}
	// Tampered key.
	if client.VerifyProxyPut("proj/tenant/other", exp, sig) {
		t.Fatalf("upload signature verified for a different key")
	}
	// A GET (download) signature must NOT authorize an upload.
	getSig := client.proxySignature(key, exp)
	if client.VerifyProxyPut(key, exp, getSig) {
		t.Fatalf("download signature replayed as an upload signature")
	}
}

func TestUriEncode(t *testing.T) {
	cases := []struct {
		in          string
		encodeSlash bool
		want        string
	}{
		{"test.txt", true, "test.txt"},
		{"a b", true, "a%20b"},
		{"a/b", false, "a/b"},
		{"a/b", true, "a%2Fb"},
		{"key~with-safe_chars.ok", true, "key~with-safe_chars.ok"},
		{"plus+and&amp", true, "plus%2Band%26amp"},
	}
	for _, tc := range cases {
		if got := uriEncode(tc.in, tc.encodeSlash); got != tc.want {
			t.Errorf("uriEncode(%q, %v) = %q, want %q", tc.in, tc.encodeSlash, got, tc.want)
		}
	}
}

// TestPutHeadDeleteRoundTrip exercises the header-signing path against a local
// fake S3 endpoint, verifying that signed requests are well-formed and that
// PutObject/HeadObject/DeleteObject thread method, path and payload correctly.
func TestPutHeadDeleteRoundTrip(t *testing.T) {
	objects := map[string][]byte{}
	contentTypes := map[string]string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
			t.Errorf("missing/invalid Authorization header: %q", auth)
		}
		if r.Header.Get("X-Amz-Content-Sha256") == "" {
			t.Errorf("missing X-Amz-Content-Sha256 header")
		}
		key := strings.TrimPrefix(r.URL.Path, "/gonvex-dev/")
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			objects[key] = body
			contentTypes[key] = r.Header.Get("Content-Type")
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			body, ok := objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", contentTypes[key])
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("ETag", `"deadbeef"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client := NewClient(Config{
		Endpoint:        server.URL,
		Region:          "us-east-1",
		Bucket:          "gonvex-dev",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		ForcePathStyle:  true,
	})
	ctx := context.Background()
	key := "proj/tenant/file1"
	payload := []byte("hello gonvex storage")

	if err := client.PutObject(ctx, key, payload, "text/plain"); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	head, found, err := client.HeadObject(ctx, key)
	if err != nil || !found {
		t.Fatalf("HeadObject: found=%v err=%v", found, err)
	}
	if head.Size != int64(len(payload)) {
		t.Fatalf("head size = %d, want %d", head.Size, len(payload))
	}
	if head.ContentType != "text/plain" {
		t.Fatalf("head content-type = %q", head.ContentType)
	}
	if err := client.DeleteObject(ctx, key); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, found, _ := client.HeadObject(ctx, key); found {
		t.Fatalf("expected object to be gone after delete")
	}
}

func TestGetObjectStreamsPastTheRequestTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 5; i++ {
			_, _ = w.Write([]byte("chunk"))
			w.(http.Flusher).Flush()
			time.Sleep(40 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	client := NewClient(Config{
		Endpoint:        upstream.URL,
		Bucket:          "gonvex-test",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
		ForcePathStyle:  true,
	})
	// Metadata calls keep a total deadline; streaming must not inherit it.
	client.httpClient.Timeout = 50 * time.Millisecond

	resp, err := client.GetObject(context.Background(), "whagons/tenant/video.mp4")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read streamed body: %v", err)
	}
	if got := string(body); got != strings.Repeat("chunk", 5) {
		t.Fatalf("body = %q", got)
	}
}
