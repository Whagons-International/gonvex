package server

import (
	"io"
	"net/http"
	"strconv"
	"strings"
)

// handleStorageProxy streams a stored object to the browser. GetURL hands out
// signed URLs like /storage/<project>/<tenant>/<fileId>?exp=&sig= so the file is
// reachable over the runtime's public hostname while MinIO stays private. The
// signature (HMAC over object key + expiry) is the authorization, mirroring how
// the presigned S3 URLs worked — just terminated at the runtime instead.
func (s *Server) handleStorageProxy(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		http.Error(w, "storage not configured", http.StatusNotFound)
		return
	}
	objectKey := r.PathValue("key")
	if objectKey == "" {
		http.NotFound(w, r)
		return
	}
	exp, _ := strconv.ParseInt(r.URL.Query().Get("exp"), 10, 64)
	if !s.storage.VerifyProxyGet(objectKey, exp, r.URL.Query().Get("sig")) {
		http.Error(w, "invalid or expired storage signature", http.StatusForbidden)
		return
	}

	resp, err := s.storage.FetchObject(r.Context(), objectKey, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	for _, header := range []string{
		"Accept-Ranges",
		"Content-Length",
		"Content-Range",
		"ETag",
		"Last-Modified",
	} {
		if value := resp.Header.Get(header); value != "" {
			w.Header().Set(header, value)
		}
	}
	// Uploads store the client's Content-Type verbatim, and this proxy serves
	// them from the runtime's own origin. Without these headers an uploaded
	// text/html file executes as same-origin script. The runtime sets no
	// cookies, so the realistic impact is same-origin script execution and
	// high-credibility phishing rather than session theft -- still worth
	// closing, and cheap to close.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
	// A missing or unknown type is not safe to render inline either: without a
	// stored type the browser would sniff the body.
	contentType := resp.Header.Get("Content-Type")
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if !inlineSafeContentType(contentType) {
		w.Header().Set("Content-Disposition", "attachment")
	}
	w.Header().Set("Cache-Control", "private, max-age=300")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// inlineSafeContentType lists the types that may render inline. Everything else
// is served as a download, which is what stops an uploaded document from being
// interpreted as script on this origin. Keep this list conservative: image/svg+xml
// is deliberately absent because SVG executes script.
func inlineSafeContentType(contentType string) bool {
	base, _, _ := strings.Cut(contentType, ";")
	switch strings.ToLower(strings.TrimSpace(base)) {
	case "image/png", "image/jpeg", "image/gif", "image/webp", "image/avif",
		"image/bmp", "image/x-icon", "image/vnd.microsoft.icon",
		"video/mp4", "video/webm", "audio/mpeg", "audio/ogg", "audio/wav",
		"application/pdf", "text/plain":
		return true
	default:
		return false
	}
}

func drainClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<16))
	_ = body.Close()
}
