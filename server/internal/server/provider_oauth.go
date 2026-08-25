package server

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type oidcProviderConfig struct{ tenantID, clientID, clientSecret string }

func (s *Server) microsoftConfig(ctx context.Context, project string) (oidcProviderConfig, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return oidcProviderConfig{}, fmt.Errorf("auth configuration is unavailable")
	}
	var config oidcProviderConfig
	var encrypted []byte
	var enabled bool
	err = db.QueryRowContext(ctx, `SELECT azure_tenant_id,client_id,client_secret_encrypted,enabled FROM gonvex_auth_providers WHERE project_id=$1 AND provider='microsoft'`, project).Scan(&config.tenantID, &config.clientID, &encrypted, &enabled)
	if err != nil {
		return config, err
	}
	if !enabled || config.tenantID == "" || config.clientID == "" || len(encrypted) == 0 {
		return config, fmt.Errorf("Microsoft auth is not configured")
	}
	config.clientSecret, err = s.decryptControlSecret(encrypted)
	return config, err
}

func (s *Server) appleConfig(ctx context.Context, project string) (oidcProviderConfig, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return oidcProviderConfig{}, fmt.Errorf("auth configuration is unavailable")
	}
	var config oidcProviderConfig
	var encrypted []byte
	var enabled bool
	err = db.QueryRowContext(ctx, `SELECT client_id,client_secret_encrypted,enabled FROM gonvex_auth_providers WHERE project_id=$1 AND provider='apple'`, project).Scan(&config.clientID, &encrypted, &enabled)
	if err != nil {
		return config, err
	}
	if !enabled || config.clientID == "" || len(encrypted) == 0 {
		return config, fmt.Errorf("Apple auth is not configured")
	}
	config.clientSecret, err = s.decryptControlSecret(encrypted)
	return config, err
}

func (s *Server) handleMicrosoftAuthorize(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "microsoft-authorize", 30, time.Minute) {
		return
	}
	q := r.URL.Query()
	project := strings.TrimSpace(q.Get("project"))
	redirect, err := normalizeAppRedirectURI(q.Get("redirect_uri"))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	state, challenge := strings.TrimSpace(q.Get("state")), strings.TrimSpace(q.Get("code_challenge"))
	if project == "" || len(state) < 16 || q.Get("code_challenge_method") != "S256" || !pkceValuePattern.MatchString(challenge) {
		writeJSON(w, 400, map[string]string{"error": "project, state, and PKCE S256 are required"})
		return
	}
	allowed, err := s.providerRedirectAllowed(r.Context(), project, "microsoft", redirect)
	if err != nil || !allowed {
		writeJSON(w, 400, map[string]string{"error": "redirect URI is not registered for this project"})
		return
	}
	config, err := s.microsoftConfig(r.Context(), project)
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	base, _ := normalizeAuthPublicURL(s.config.AuthPublicURL)
	callback := base + "/auth/microsoft/callback"
	transaction, err := randomID("oauth")
	if err != nil {
		return
	}
	nonce, _ := randomID("nonce")
	if err := s.saveAuthTransaction(r.Context(), transaction, authTransaction{ProjectID: project, RedirectURI: redirect, AppState: state, CodeChallenge: challenge, Nonce: nonce, GoogleRedirectURI: callback, Provider: "microsoft"}); err != nil {
		writeJSON(w, 500, map[string]string{"error": "could not start auth flow"})
		return
	}
	authorize, _ := url.Parse("https://login.microsoftonline.com/" + url.PathEscape(config.tenantID) + "/oauth2/v2.0/authorize")
	params := authorize.Query()
	params.Set("client_id", config.clientID)
	params.Set("redirect_uri", callback)
	params.Set("response_type", "code")
	params.Set("response_mode", "query")
	params.Set("scope", "openid email profile")
	params.Set("state", transaction)
	params.Set("nonce", nonce)
	authorize.RawQuery = params.Encode()
	http.Redirect(w, r, authorize.String(), 302)
}

func (s *Server) handleMicrosoftCallback(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "microsoft-callback", 60, time.Minute) {
		return
	}
	transaction, err := s.consumeAuthTransaction(r.Context(), strings.TrimSpace(r.URL.Query().Get("state")))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if transaction.Provider != "microsoft" {
		writeJSON(w, 400, map[string]string{"error": "OAuth provider mismatch"})
		return
	}
	if providerError := strings.TrimSpace(r.URL.Query().Get("error")); providerError != "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": providerError, "state": transaction.AppState})
		return
	}
	config, err := s.microsoftConfig(r.Context(), transaction.ProjectID)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "microsoft_not_configured", "state": transaction.AppState})
		return
	}
	issuer := "https://login.microsoftonline.com/" + config.tenantID + "/v2.0"
	idToken, err := exchangeOIDCCode(r.Context(), "https://login.microsoftonline.com/"+url.PathEscape(config.tenantID)+"/oauth2/v2.0/token", config, strings.TrimSpace(r.URL.Query().Get("code")), transaction.GoogleRedirectURI)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "microsoft_exchange_failed", "state": transaction.AppState})
		return
	}
	identity, err := verifyOIDCToken(r.Context(), idToken, transaction.Nonce, issuer, config.clientID, "https://login.microsoftonline.com/"+url.PathEscape(config.tenantID)+"/discovery/v2.0/keys")
	if err == nil && identity.Email != "" {
		// Entra issued this claim from the configured tenant in a signed ID token.
		identity.EmailVerified = true
	}
	if err != nil || !identity.EmailVerified || identity.Email == "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "invalid_microsoft_identity", "state": transaction.AppState})
		return
	}
	account, err := s.upsertOIDCAccount(r.Context(), transaction.ProjectID, "microsoft", issuer, identity)
	if err == nil {
		err = s.ensureAppAuthMemberships(r.Context(), transaction.ProjectID, account)
	}
	if err != nil {
		code := "membership_setup_failed"
		if errors.Is(err, errAppAuthInvitationRequired) {
			code = "invitation_required"
		}
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": code, "state": transaction.AppState})
		return
	}
	code, err := s.createAppAuthCode(r.Context(), transaction, account.ID)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "code_creation_failed", "state": transaction.AppState})
		return
	}
	redirectToApp(w, r, transaction.RedirectURI, map[string]string{"code": code, "state": transaction.AppState})
}

func (s *Server) handleAppleAuthorize(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "apple-authorize", 30, time.Minute) {
		return
	}
	q := r.URL.Query()
	project := strings.TrimSpace(q.Get("project"))
	redirect, err := normalizeAppRedirectURI(q.Get("redirect_uri"))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	state, challenge := strings.TrimSpace(q.Get("state")), strings.TrimSpace(q.Get("code_challenge"))
	if project == "" || len(state) < 16 || q.Get("code_challenge_method") != "S256" || !pkceValuePattern.MatchString(challenge) {
		writeJSON(w, 400, map[string]string{"error": "project, state, and PKCE S256 are required"})
		return
	}
	allowed, err := s.providerRedirectAllowed(r.Context(), project, "apple", redirect)
	if err != nil || !allowed {
		writeJSON(w, 400, map[string]string{"error": "redirect URI is not registered for this project"})
		return
	}
	config, err := s.appleConfig(r.Context(), project)
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	base, _ := normalizeAuthPublicURL(s.config.AuthPublicURL)
	callback := base + "/auth/apple/callback"
	transaction, err := randomID("oauth")
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "could not start auth flow"})
		return
	}
	nonce, err := randomID("nonce")
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "could not start auth flow"})
		return
	}
	if err := s.saveAuthTransaction(r.Context(), transaction, authTransaction{ProjectID: project, RedirectURI: redirect, AppState: state, CodeChallenge: challenge, Nonce: nonce, GoogleRedirectURI: callback, Provider: "apple"}); err != nil {
		writeJSON(w, 500, map[string]string{"error": "could not start auth flow"})
		return
	}
	authorize, _ := url.Parse("https://appleid.apple.com/auth/authorize")
	params := authorize.Query()
	params.Set("client_id", config.clientID)
	params.Set("redirect_uri", callback)
	params.Set("response_type", "code")
	params.Set("response_mode", "form_post")
	params.Set("scope", "name email")
	params.Set("state", transaction)
	params.Set("nonce", nonce)
	authorize.RawQuery = params.Encode()
	http.Redirect(w, r, authorize.String(), http.StatusFound)
}

func (s *Server) handleAppleCallback(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "apple-callback", 60, time.Minute) {
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid Apple callback"})
		return
	}
	transaction, err := s.consumeAuthTransaction(r.Context(), strings.TrimSpace(r.Form.Get("state")))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if transaction.Provider != "apple" {
		writeJSON(w, 400, map[string]string{"error": "OAuth provider mismatch"})
		return
	}
	if providerError := strings.TrimSpace(r.Form.Get("error")); providerError != "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": providerError, "state": transaction.AppState})
		return
	}
	config, err := s.appleConfig(r.Context(), transaction.ProjectID)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "apple_not_configured", "state": transaction.AppState})
		return
	}
	idToken, err := exchangeOIDCCode(r.Context(), "https://appleid.apple.com/auth/token", config, strings.TrimSpace(r.Form.Get("code")), transaction.GoogleRedirectURI)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "apple_exchange_failed", "state": transaction.AppState})
		return
	}
	identity, err := verifyOIDCToken(r.Context(), idToken, transaction.Nonce, "https://appleid.apple.com", config.clientID, "https://appleid.apple.com/auth/keys")
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "invalid_apple_identity", "state": transaction.AppState})
		return
	}
	account, err := s.upsertOIDCAccount(r.Context(), transaction.ProjectID, "apple", "https://appleid.apple.com", identity)
	if err == nil {
		err = s.ensureAppAuthMemberships(r.Context(), transaction.ProjectID, account)
	}
	if err != nil {
		code := "membership_setup_failed"
		if errors.Is(err, errAppAuthInvitationRequired) {
			code = "invitation_required"
		}
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": code, "state": transaction.AppState})
		return
	}
	code, err := s.createAppAuthCode(r.Context(), transaction, account.ID)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "code_creation_failed", "state": transaction.AppState})
		return
	}
	redirectToApp(w, r, transaction.RedirectURI, map[string]string{"code": code, "state": transaction.AppState})
}

func (s *Server) providerRedirectAllowed(ctx context.Context, project, provider, redirect string) (bool, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return false, err
	}
	var allowed bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM gonvex_auth_providers p
		JOIN gonvex_auth_redirect_uris r ON r.project_id=p.project_id AND r.provider=p.provider
		JOIN gonvex_runtime_projects project_row ON project_row.id=p.project_id
		WHERE p.project_id=$1 AND p.provider=$2 AND p.enabled AND r.redirect_uri=$3
		AND COALESCE(NULLIF(project_row.auth_mode,''),'gonvex-native') IN ('gonvex-native','hybrid'))`, project, provider, redirect).Scan(&allowed)
	return allowed, err
}

func exchangeOIDCCode(ctx context.Context, endpoint string, config oidcProviderConfig, code, redirect string) (string, error) {
	form := url.Values{"code": {code}, "client_id": {config.clientID}, "client_secret": {config.clientSecret}, "redirect_uri": {redirect}, "grant_type": {"authorization_code"}}
	request, _ := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(form.Encode()))
	request.Header.Set("content-type", "application/x-www-form-urlencoded")
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	var payload struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || payload.IDToken == "" {
		return "", fmt.Errorf("OIDC token exchange failed")
	}
	return payload.IDToken, nil
}

func verifyOIDCToken(ctx context.Context, token, nonce, issuer, audience, jwksURL string) (googleIdentity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return googleIdentity{}, fmt.Errorf("OIDC token is malformed")
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if decodeJWTPart(parts[0], &header) != nil || header.Algorithm != "RS256" {
		return googleIdentity{}, fmt.Errorf("OIDC token header is invalid")
	}
	key, err := fetchOIDCKey(ctx, jwksURL, header.KeyID)
	if err != nil {
		return googleIdentity{}, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return googleIdentity{}, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return googleIdentity{}, fmt.Errorf("OIDC signature is invalid")
	}
	var claims struct {
		Issuer            string          `json:"iss"`
		Audience          string          `json:"aud"`
		Subject           string          `json:"sub"`
		Email             string          `json:"email"`
		PreferredUsername string          `json:"preferred_username"`
		Name              string          `json:"name"`
		Picture           string          `json:"picture"`
		Nonce             string          `json:"nonce"`
		EmailVerified     json.RawMessage `json:"email_verified"`
		ExpiresAt         int64           `json:"exp"`
		IssuedAt          int64           `json:"iat"`
	}
	if err := decodeJWTPart(parts[1], &claims); err != nil {
		return googleIdentity{}, err
	}
	now := time.Now().Unix()
	if claims.Issuer != issuer || claims.Audience != audience || claims.Subject == "" || claims.ExpiresAt <= now || claims.IssuedAt > now+300 || !constantTimeString(claims.Nonce, nonce) {
		return googleIdentity{}, fmt.Errorf("OIDC claims are invalid")
	}
	email := strings.ToLower(strings.TrimSpace(claims.Email))
	if email == "" {
		email = strings.ToLower(strings.TrimSpace(claims.PreferredUsername))
	}
	verified := false
	if len(claims.EmailVerified) > 0 {
		_ = json.Unmarshal(claims.EmailVerified, &verified)
		if !verified {
			var text string
			_ = json.Unmarshal(claims.EmailVerified, &text)
			verified = strings.EqualFold(text, "true")
		}
	}
	return googleIdentity{Subject: claims.Subject, Email: email, EmailVerified: verified, Name: claims.Name, Picture: claims.Picture}, nil
}

func fetchOIDCKey(ctx context.Context, endpoint, keyID string) (*rsa.PublicKey, error) {
	request, _ := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("OIDC key endpoint returned HTTP %d", response.StatusCode)
	}
	var document struct {
		Keys []struct {
			KeyType  string `json:"kty"`
			KeyID    string `json:"kid"`
			Modulus  string `json:"n"`
			Exponent string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return nil, err
	}
	for _, item := range document.Keys {
		if item.KeyID != keyID || item.KeyType != "RSA" {
			continue
		}
		modulus, _ := base64.RawURLEncoding.DecodeString(item.Modulus)
		exponentBytes, _ := base64.RawURLEncoding.DecodeString(item.Exponent)
		exponent := 0
		for _, value := range exponentBytes {
			exponent = exponent<<8 | int(value)
		}
		if exponent >= 3 {
			return &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}, nil
		}
	}
	return nil, sql.ErrNoRows
}
