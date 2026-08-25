package server

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const firebaseIdentityToolkitScope = "https://www.googleapis.com/auth/identitytoolkit"

type firebaseAdminCredentials struct {
	ProjectID   string `json:"project_id"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURI    string `json:"token_uri"`
}

// verifyFirebaseAccountPolicy uses optional runtime-owned Firebase Admin
// credentials to reject disabled users and ID tokens issued before Firebase's
// valid-since boundary. Normal projects without Admin credentials still rely
// on signed ID-token expiry plus the local Account disabled flag.
func (s *Server) verifyFirebaseAccountPolicy(ctx context.Context, configuration externalAuthConfiguration, subject string, authTime time.Time) error {
	credentials, err := parseFirebaseAdminCredentials(configuration.AdminSecret, configuration.FirebaseProject)
	if err != nil {
		return err
	}
	accessToken, err := firebaseAdminAccessToken(ctx, credentials)
	if err != nil {
		return fmt.Errorf("Firebase account policy is unavailable: %w", err)
	}
	lookupURL := "https://identitytoolkit.googleapis.com/v1/projects/" + url.PathEscape(configuration.FirebaseProject)
	if configuration.FirebaseTenant != "" {
		lookupURL += "/tenants/" + url.PathEscape(configuration.FirebaseTenant)
	}
	lookupURL += "/accounts:lookup"
	body, _ := json.Marshal(map[string]any{"localId": []string{subject}})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, lookupURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("authorization", "Bearer "+accessToken)
	request.Header.Set("content-type", "application/json")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("Firebase account lookup returned HTTP %d", response.StatusCode)
	}
	var payload struct {
		Users []struct {
			LocalID    string `json:"localId"`
			Disabled   bool   `json:"disabled"`
			ValidSince string `json:"validSince"`
		} `json:"users"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return err
	}
	for _, user := range payload.Users {
		if user.LocalID != subject {
			continue
		}
		if user.Disabled {
			return fmt.Errorf("Firebase account is disabled")
		}
		if validSince, err := strconv.ParseInt(user.ValidSince, 10, 64); err == nil && validSince > 0 && authTime.Unix() < validSince {
			return fmt.Errorf("Firebase session was revoked")
		}
		return nil
	}
	return fmt.Errorf("Firebase account is unavailable")
}

func parseFirebaseAdminCredentials(raw, firebaseProject string) (firebaseAdminCredentials, error) {
	var credentials firebaseAdminCredentials
	if err := json.Unmarshal([]byte(raw), &credentials); err != nil {
		return credentials, fmt.Errorf("Firebase Admin credentials are invalid")
	}
	credentials.ProjectID = strings.TrimSpace(credentials.ProjectID)
	credentials.ClientEmail = strings.TrimSpace(credentials.ClientEmail)
	credentials.TokenURI = strings.TrimSpace(credentials.TokenURI)
	if credentials.TokenURI == "" {
		credentials.TokenURI = "https://oauth2.googleapis.com/token"
	}
	if credentials.ProjectID != strings.TrimSpace(firebaseProject) || credentials.ClientEmail == "" || credentials.PrivateKey == "" {
		return credentials, fmt.Errorf("Firebase Admin credentials do not match this project")
	}
	if credentials.TokenURI != "https://oauth2.googleapis.com/token" {
		return credentials, fmt.Errorf("Firebase Admin token_uri is unsupported")
	}
	if _, err := parseFirebasePrivateKey(credentials.PrivateKey); err != nil {
		return credentials, fmt.Errorf("Firebase Admin credentials are invalid: %w", err)
	}
	return credentials, nil
}

func firebaseAdminAccessToken(ctx context.Context, credentials firebaseAdminCredentials) (string, error) {
	privateKey, err := parseFirebasePrivateKey(credentials.PrivateKey)
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{
		"iss": credentials.ClientEmail, "scope": firebaseIdentityToolkitScope,
		"aud": credentials.TokenURI, "iat": now, "exp": now + 3600,
	})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	assertion := unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {assertion},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, credentials.TokenURI, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("content-type", "application/x-www-form-urlencoded")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("OAuth token endpoint returned HTTP %d", response.StatusCode)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", err
	}
	if payload.AccessToken == "" || !strings.EqualFold(payload.TokenType, "Bearer") {
		return "", fmt.Errorf("OAuth token response is invalid")
	}
	return payload.AccessToken, nil
}

func parseFirebasePrivateKey(raw string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(raw))
	if block == nil {
		return nil, fmt.Errorf("private key is invalid")
	}
	var privateKey *rsa.PrivateKey
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		privateKey, _ = parsed.(*rsa.PrivateKey)
	} else if parsedPKCS1, parseErr := x509.ParsePKCS1PrivateKey(block.Bytes); parseErr == nil {
		privateKey = parsedPKCS1
	}
	if privateKey == nil {
		return nil, fmt.Errorf("private key is not RSA")
	}
	return privateKey, nil
}
