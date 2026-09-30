// Package storage implements Gonvex's S3-compatible object storage layer using
// only the Go standard library. It hand-rolls AWS Signature Version 4 so the
// runtime can talk to S3, R2, MinIO, B2, Tigris and friends without pulling in
// a cloud SDK, matching the repo's minimal-dependency house style.
package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// emptyPayloadHash is the SHA-256 of an empty body, used for GET/HEAD/DELETE
// requests that carry no payload.
const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

const (
	signingService   = "s3"
	signingAlgorithm = "AWS4-HMAC-SHA256"
	amzDateFormat    = "20060102T150405Z"
	dateStampFormat  = "20060102"
)

// Config describes an S3-compatible bucket the runtime can read and write.
type Config struct {
	Endpoint        string // e.g. https://s3.amazonaws.com or http://localhost:9000
	Region          string // e.g. us-east-1
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	// ForcePathStyle uses http://host/bucket/key instead of
	// http://bucket.host/key. Required for MinIO and most self-hosted backends.
	ForcePathStyle bool
	// PublicBaseURL, when set, makes the runtime hand out browser-reachable
	// proxy URLs (served by the runtime's GET /storage handler) instead of
	// presigned URLs that point at the (often private) internal S3 endpoint.
	PublicBaseURL string
	// URLSigningKey signs those proxy URLs (HMAC). Defaults to SecretAccessKey.
	URLSigningKey string
}

// Configured reports whether the minimum required fields are present to make
// signed requests.
func (c Config) Configured() bool {
	return c.Endpoint != "" && c.Bucket != "" && c.AccessKeyID != "" && c.SecretAccessKey != ""
}

// Client is a minimal signed S3 client bound to a single bucket.
type Client struct {
	cfg        Config
	httpClient *http.Client
	// streamClient serves GetObject. Its body is streamed to a caller that
	// controls the lifetime through ctx, so it has no total deadline: a
	// whole-request timeout would cut video playback off mid-stream.
	streamClient *http.Client
	// now is injectable so signing is deterministic in tests.
	now func() time.Time
}

// NewClient builds a Client. The Config must be Configured(); callers should
// check that first and fall back to the not-configured storage path otherwise.
func NewClient(cfg Config) *Client {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg.Region = region
	return &Client{
		cfg:          cfg,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		streamClient: newStreamClient(),
		now:          time.Now,
	}
}

// newStreamClient bounds connecting and waiting for response headers, but not
// reading the body.
func newStreamClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{Transport: transport}
}

// Bucket returns the configured bucket name.
func (c *Client) Bucket() string { return c.cfg.Bucket }

// PresignGet returns a presigned URL the holder can GET to download the object.
func (c *Client) PresignGet(key string, expires time.Duration) (string, error) {
	return c.presign(http.MethodGet, key, expires)
}

// PresignPut returns a presigned URL the holder can PUT the object bytes to.
func (c *Client) PresignPut(key string, expires time.Duration) (string, error) {
	return c.presign(http.MethodPut, key, expires)
}

// GetObject performs a SigV4-signed GET against the (internal) endpoint and
// returns the response for streaming. The caller must close resp.Body.
func (c *Client) GetObject(ctx context.Context, key string, byteRange ...string) (*http.Response, error) {
	req, err := c.signedRequest(ctx, http.MethodGet, key, nil, emptyPayloadHash, "")
	if err != nil {
		return nil, err
	}
	if len(byteRange) > 0 && strings.TrimSpace(byteRange[0]) != "" {
		req.Header.Set("Range", byteRange[0])
	}
	return c.streamClient.Do(req)
}

// PublicURL returns the unsigned object URL, suitable for public-read objects.
func (c *Client) PublicURL(key string) string {
	urlStr, _, _ := c.buildURL(key)
	return urlStr
}

// ProxyGetURL builds a browser-reachable, time-limited download URL served by
// the runtime's GET /storage handler (which streams the object from the private
// S3 endpoint). Used when PublicBaseURL is configured.
func (c *Client) ProxyGetURL(key string, expires time.Duration) string {
	if expires <= 0 {
		expires = 15 * time.Minute
	}
	exp := c.now().Add(expires).Unix()
	base := strings.TrimRight(c.cfg.PublicBaseURL, "/")
	// encodePath returns a leading-slash path; reuse it for per-segment encoding.
	return fmt.Sprintf("%s/storage%s?exp=%d&sig=%s", base, encodePath(key), exp, c.proxySignature(key, exp))
}

// VerifyProxyGet validates a proxy download signature and expiry for key.
func (c *Client) VerifyProxyGet(key string, exp int64, sig string) bool {
	if exp <= 0 || exp < c.now().Unix() {
		return false
	}
	return hmac.Equal([]byte(c.proxySignature(key, exp)), []byte(sig))
}

func (c *Client) proxySignature(key string, exp int64) string {
	secret := c.cfg.URLSigningKey
	if secret == "" {
		secret = c.cfg.SecretAccessKey
	}
	return hex.EncodeToString(hmacSHA256([]byte("gonvex-storage:"+secret), key+"\n"+strconv.FormatInt(exp, 10)))
}

// ProxyPutURL builds a browser-reachable, time-limited upload URL served by the
// runtime's POST /storage handler (which writes the bytes to the private S3
// endpoint). Used when PublicBaseURL is configured so the browser never PUTs
// straight to the internal S3 host. Mirrors ProxyGetURL but signs an
// upload-scoped signature so a download URL cannot be replayed as an upload.
func (c *Client) ProxyPutURL(key string, expires time.Duration) string {
	if expires <= 0 {
		expires = 15 * time.Minute
	}
	exp := c.now().Add(expires).Unix()
	base := strings.TrimRight(c.cfg.PublicBaseURL, "/")
	return fmt.Sprintf("%s/storage%s?exp=%d&sig=%s&upload=1", base, encodePath(key), exp, c.proxyUploadSignature(key, exp))
}

// VerifyProxyPut validates a proxy upload signature and expiry for key.
func (c *Client) VerifyProxyPut(key string, exp int64, sig string) bool {
	if exp <= 0 || exp < c.now().Unix() {
		return false
	}
	return hmac.Equal([]byte(c.proxyUploadSignature(key, exp)), []byte(sig))
}

// proxyUploadSignature is domain-separated from proxySignature ("...-upload:")
// so a GET (download) signature can never be replayed to authorize an upload.
func (c *Client) proxyUploadSignature(key string, exp int64) string {
	secret := c.cfg.URLSigningKey
	if secret == "" {
		secret = c.cfg.SecretAccessKey
	}
	return hex.EncodeToString(hmacSHA256([]byte("gonvex-storage-upload:"+secret), key+"\n"+strconv.FormatInt(exp, 10)))
}

// PutObject uploads bytes to key using a SigV4-signed PUT request.
func (c *Client) PutObject(ctx context.Context, key string, body []byte, contentType string) error {
	payloadHash := sha256Hex(body)
	req, err := c.signedRequest(ctx, http.MethodPut, key, bytes.NewReader(body), payloadHash, contentType)
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(body))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError("put object", resp)
	}
	return nil
}

// HeadResult holds the subset of object metadata HEAD returns.
type HeadResult struct {
	Size        int64
	ContentType string
	ETag        string
}

// HeadObject fetches object metadata. found is false (with a nil error) when
// the object does not exist yet.
func (c *Client) HeadObject(ctx context.Context, key string) (result HeadResult, found bool, err error) {
	req, err := c.signedRequest(ctx, http.MethodHead, key, nil, emptyPayloadHash, "")
	if err != nil {
		return HeadResult{}, false, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return HeadResult{}, false, err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return HeadResult{}, false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return HeadResult{}, false, responseError("head object", resp)
	}
	result = HeadResult{
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        strings.Trim(resp.Header.Get("ETag"), `"`),
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if size, convErr := strconv.ParseInt(cl, 10, 64); convErr == nil {
			result.Size = size
		}
	}
	return result, true, nil
}

// DeleteObject removes key. S3 treats deleting a missing object as success.
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	req, err := c.signedRequest(ctx, http.MethodDelete, key, nil, emptyPayloadHash, "")
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)
	ok := (resp.StatusCode >= 200 && resp.StatusCode < 300) || resp.StatusCode == http.StatusNotFound
	if !ok {
		return responseError("delete object", resp)
	}
	return nil
}

// ListedObject is one entry returned by ListObjects.
type ListedObject struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string    `xml:"Key"`
		Size         int64     `xml:"Size"`
		LastModified time.Time `xml:"LastModified"`
		ETag         string    `xml:"ETag"`
	} `xml:"Contents"`
}

// ListObjects returns up to maxKeys objects whose keys start with prefix, using
// the S3 ListObjectsV2 API and following continuation tokens as needed.
func (c *Client) ListObjects(ctx context.Context, prefix string, maxKeys int) ([]ListedObject, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	var objects []ListedObject
	continuation := ""
	for {
		pageSize := maxKeys - len(objects)
		if pageSize > 1000 {
			pageSize = 1000
		}
		page, next, err := c.listObjectsPage(ctx, prefix, pageSize, continuation)
		if err != nil {
			return nil, err
		}
		objects = append(objects, page...)
		if next == "" || len(objects) >= maxKeys {
			break
		}
		continuation = next
	}
	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}
	return objects, nil
}

func (c *Client) listObjectsPage(ctx context.Context, prefix string, maxKeys int, continuationToken string) ([]ListedObject, string, error) {
	urlStr, host, canonicalURI := c.buildBucketURL()
	query := map[string]string{
		"list-type": "2",
		"max-keys":  strconv.Itoa(maxKeys),
	}
	if prefix != "" {
		query["prefix"] = prefix
	}
	if continuationToken != "" {
		query["continuation-token"] = continuationToken
	}
	canonicalQuery := canonicalQueryString(query)

	now := c.now().UTC()
	amzDate := now.Format(amzDateFormat)
	dateStamp := now.Format(dateStampFormat)
	scope := credentialScope(dateStamp, c.cfg.Region)

	headers := map[string]string{
		"host":                 host,
		"x-amz-content-sha256": emptyPayloadHash,
		"x-amz-date":           amzDate,
	}
	signedHeaders, canonicalHeaders := canonicalHeaderBlock(headers)
	canonicalRequest := strings.Join([]string{
		http.MethodGet,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		emptyPayloadHash,
	}, "\n")
	signature := c.sign(canonicalRequest, amzDate, dateStamp, scope)
	authorization := fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		signingAlgorithm, c.cfg.AccessKeyID, scope, signedHeaders, signature,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr+"?"+canonicalQuery, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", authorization)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", emptyPayloadHash)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", responseError("list objects", resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, "", err
	}
	var parsed listBucketResult
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return nil, "", fmt.Errorf("storage: parse list objects: %w", err)
	}
	objects := make([]ListedObject, 0, len(parsed.Contents))
	for _, item := range parsed.Contents {
		objects = append(objects, ListedObject{
			Key:          item.Key,
			Size:         item.Size,
			LastModified: item.LastModified,
			ETag:         strings.Trim(item.ETag, `"`),
		})
	}
	next := ""
	if parsed.IsTruncated {
		next = parsed.NextContinuationToken
	}
	return objects, next, nil
}

// buildBucketURL returns the request URL, signing host, and canonical path for
// a bucket-level (not object-level) operation such as ListObjectsV2.
func (c *Client) buildBucketURL() (urlStr, host, canonicalURI string) {
	base, err := url.Parse(c.cfg.Endpoint)
	if err != nil || base.Host == "" {
		base = &url.URL{Scheme: "https", Host: strings.TrimPrefix(strings.TrimPrefix(c.cfg.Endpoint, "https://"), "http://")}
	}
	scheme := base.Scheme
	if scheme == "" {
		scheme = "https"
	}
	basePath := strings.TrimRight(base.Path, "/")
	if c.cfg.ForcePathStyle {
		host = base.Host
		canonicalURI = encodePath(basePath + "/" + c.cfg.Bucket)
	} else {
		host = c.cfg.Bucket + "." + base.Host
		canonicalURI = encodePath(basePath + "/")
	}
	urlStr = scheme + "://" + host + canonicalURI
	return urlStr, host, canonicalURI
}

// presign builds a query-string-authenticated SigV4 URL for method/key.
func (c *Client) presign(method, key string, expires time.Duration) (string, error) {
	if expires <= 0 {
		expires = 15 * time.Minute
	}
	urlStr, host, canonicalURI := c.buildURL(key)
	now := c.now().UTC()
	amzDate := now.Format(amzDateFormat)
	dateStamp := now.Format(dateStampFormat)
	scope := credentialScope(dateStamp, c.cfg.Region)

	query := map[string]string{
		"X-Amz-Algorithm":     signingAlgorithm,
		"X-Amz-Credential":    c.cfg.AccessKeyID + "/" + scope,
		"X-Amz-Date":          amzDate,
		"X-Amz-Expires":       strconv.Itoa(int(expires.Seconds())),
		"X-Amz-SignedHeaders": "host",
	}
	canonicalQuery := canonicalQueryString(query)

	canonicalHeaders := "host:" + host + "\n"
	signedHeaders := "host"
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		"UNSIGNED-PAYLOAD",
	}, "\n")

	signature := c.sign(canonicalRequest, amzDate, dateStamp, scope)
	return urlStr + "?" + canonicalQuery + "&X-Amz-Signature=" + signature, nil
}

// signedRequest builds an *http.Request with a SigV4 Authorization header.
func (c *Client) signedRequest(ctx context.Context, method, key string, body io.Reader, payloadHash, contentType string) (*http.Request, error) {
	urlStr, host, canonicalURI := c.buildURL(key)
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return nil, err
	}
	now := c.now().UTC()
	amzDate := now.Format(amzDateFormat)
	dateStamp := now.Format(dateStampFormat)
	scope := credentialScope(dateStamp, c.cfg.Region)

	headers := map[string]string{
		"host":                 host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	if contentType != "" {
		headers["content-type"] = contentType
	}

	signedHeaders, canonicalHeaders := canonicalHeaderBlock(headers)
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		"", // no query string
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	signature := c.sign(canonicalRequest, amzDate, dateStamp, scope)
	authorization := fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		signingAlgorithm, c.cfg.AccessKeyID, scope, signedHeaders, signature,
	)

	req.Header.Set("Authorization", authorization)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nil
}

// sign turns a canonical request into the final hex signature.
func (c *Client) sign(canonicalRequest, amzDate, dateStamp, scope string) string {
	stringToSign := strings.Join([]string{
		signingAlgorithm,
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signingKey := deriveSigningKey(c.cfg.SecretAccessKey, dateStamp, c.cfg.Region, signingService)
	return hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
}

// buildURL returns the request URL, the host used for signing, and the
// canonical (URI-encoded) path.
func (c *Client) buildURL(key string) (urlStr, host, canonicalURI string) {
	base, err := url.Parse(c.cfg.Endpoint)
	if err != nil || base.Host == "" {
		// Endpoint without a scheme parses oddly; fall back to treating the
		// whole string as the host.
		base = &url.URL{Scheme: "https", Host: strings.TrimPrefix(strings.TrimPrefix(c.cfg.Endpoint, "https://"), "http://")}
	}
	scheme := base.Scheme
	if scheme == "" {
		scheme = "https"
	}
	basePath := strings.TrimRight(base.Path, "/")

	if c.cfg.ForcePathStyle {
		host = base.Host
		canonicalURI = encodePath(basePath + "/" + c.cfg.Bucket + "/" + key)
	} else {
		host = c.cfg.Bucket + "." + base.Host
		canonicalURI = encodePath(basePath + "/" + key)
	}
	urlStr = scheme + "://" + host + canonicalURI
	return urlStr, host, canonicalURI
}

func credentialScope(dateStamp, region string) string {
	return strings.Join([]string{dateStamp, region, signingService, "aws4_request"}, "/")
}

// canonicalQueryString renders params sorted by key, with keys and values
// URI-encoded per AWS rules.
func canonicalQueryString(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, uriEncode(key, true)+"="+uriEncode(params[key], true))
	}
	return strings.Join(parts, "&")
}

// canonicalHeaderBlock returns the signed-headers list and the canonical
// header block (each "name:value\n", sorted by name).
func canonicalHeaderBlock(headers map[string]string) (signedHeaders, canonicalHeaders string) {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, strings.ToLower(name))
	}
	sort.Strings(names)
	var builder strings.Builder
	for _, name := range names {
		builder.WriteString(name)
		builder.WriteString(":")
		builder.WriteString(strings.TrimSpace(headers[name]))
		builder.WriteString("\n")
	}
	return strings.Join(names, ";"), builder.String()
}

// encodePath URI-encodes each path segment while preserving the slashes that
// give the path its structure. S3 uses single (not double) encoding.
func encodePath(path string) string {
	if path == "" {
		return "/"
	}
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		segments[i] = uriEncode(segment, true)
	}
	encoded := strings.Join(segments, "/")
	if !strings.HasPrefix(encoded, "/") {
		encoded = "/" + encoded
	}
	return encoded
}

// uriEncode percent-encodes s per RFC 3986 / AWS SigV4 rules. Unreserved
// characters pass through; everything else is upper-hex percent-encoded. When
// encodeSlash is false, '/' is left intact (used for path segments).
func uriEncode(s string, encodeSlash bool) string {
	var builder strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '_' || ch == '.' || ch == '~':
			builder.WriteByte(ch)
		case ch == '/' && !encodeSlash:
			builder.WriteByte(ch)
		default:
			builder.WriteString("%")
			builder.WriteString(strings.ToUpper(hex.EncodeToString([]byte{ch})))
		}
	}
	return builder.String()
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<16))
	_ = body.Close()
}

func responseError(action string, resp *http.Response) error {
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return fmt.Errorf("storage: %s failed: %s: %s", action, resp.Status, strings.TrimSpace(string(snippet)))
}
