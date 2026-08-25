package server

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	authModeNative       = "gonvex-native"
	authModeFirebase     = "firebase"
	authModeExternalOIDC = "external-oidc"
	authModeHybrid       = "hybrid"
)

type externalAuthConfiguration struct {
	Mode            string
	Provider        string
	Enabled         bool
	SignupMode      string
	Issuer          string
	Audience        string
	JWKSURL         string
	FirebaseProject string
	FirebaseTenant  string
	HasAdminSecret  bool
	AdminSecret     string
}

type verifiedExternalIdentity struct {
	Provider      string
	Issuer        string
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
	Picture       string
	IssuedAt      time.Time
	AuthTime      time.Time
}

// externalIdentityProvider is the trusted host extension point for project
// authentication. Implementations verify credentials and return identity only;
// the shared resolver owns Accounts, sessions, tenant routing, and admission.
type externalIdentityProvider interface {
	Verify(context.Context, externalAuthConfiguration, string) (verifiedExternalIdentity, error)
}

type signedJWTIdentityProvider struct {
	firebase      bool
	accountPolicy func(context.Context, externalAuthConfiguration, string, time.Time) error
}

func normalizeProjectAuthMode(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", authModeNative:
		return authModeNative, nil
	case authModeFirebase:
		return authModeFirebase, nil
	case authModeExternalOIDC:
		return authModeExternalOIDC, nil
	case authModeHybrid:
		return authModeHybrid, nil
	default:
		return "", fmt.Errorf("authMode must be gonvex-native, firebase, external-oidc, or hybrid")
	}
}

func authModeAllowsProvider(mode, provider string) bool {
	switch mode {
	case authModeHybrid:
		return provider == "firebase" || provider == authModeExternalOIDC
	case authModeFirebase:
		return provider == "firebase"
	case authModeExternalOIDC:
		return provider == authModeExternalOIDC
	default:
		return false
	}
}

func (s *Server) externalAuthConfiguration(ctx context.Context, store controlStore, project, provider string) (externalAuthConfiguration, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider != "firebase" && provider != authModeExternalOIDC {
		return externalAuthConfiguration{}, fmt.Errorf("external authentication provider is unsupported")
	}
	var configuration externalAuthConfiguration
	configuration.Provider = provider
	var encryptedAdmin []byte
	err := store.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(project.auth_mode,''),'gonvex-native'),
		provider.enabled,provider.signup_mode,provider.issuer,provider.audience,provider.jwks_url,
		provider.firebase_project_id,provider.firebase_tenant_id,provider.admin_credentials_encrypted
		FROM gonvex_runtime_projects project
		JOIN gonvex_auth_providers provider ON provider.project_id=project.id AND provider.provider=$2
		WHERE project.id=$1 AND project.status NOT IN ('deleted','disabled')`, project, provider).Scan(
		&configuration.Mode, &configuration.Enabled, &configuration.SignupMode,
		&configuration.Issuer, &configuration.Audience, &configuration.JWKSURL,
		&configuration.FirebaseProject, &configuration.FirebaseTenant, &encryptedAdmin,
	)
	if err == sql.ErrNoRows {
		return configuration, fmt.Errorf("external authentication is not configured")
	}
	if err != nil {
		return configuration, err
	}
	configuration.HasAdminSecret = len(encryptedAdmin) > 0
	if configuration.HasAdminSecret {
		configuration.AdminSecret, err = s.decryptControlSecret(encryptedAdmin)
		if err != nil {
			return configuration, err
		}
	}
	configuration.Mode, err = normalizeProjectAuthMode(configuration.Mode)
	if err != nil || !configuration.Enabled || !authModeAllowsProvider(configuration.Mode, provider) {
		return configuration, fmt.Errorf("external authentication is disabled for this project")
	}
	if provider == "firebase" {
		if configuration.FirebaseProject == "" {
			return configuration, fmt.Errorf("Firebase projectId is not configured")
		}
		if configuration.Issuer == "" {
			configuration.Issuer = "https://securetoken.google.com/" + configuration.FirebaseProject
		}
		if configuration.Audience == "" {
			configuration.Audience = configuration.FirebaseProject
		}
		if configuration.JWKSURL == "" {
			configuration.JWKSURL = "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com"
		}
	}
	if configuration.Issuer == "" || configuration.Audience == "" || configuration.JWKSURL == "" {
		return configuration, fmt.Errorf("issuer, audience, and jwksUrl are required")
	}
	return configuration, nil
}

func validateExternalAuthURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("jwksUrl must be an absolute HTTPS URL without credentials or a fragment")
	}
	return nil
}

func (provider signedJWTIdentityProvider) Verify(ctx context.Context, configuration externalAuthConfiguration, token string) (verifiedExternalIdentity, error) {
	if err := validateExternalAuthURL(configuration.JWKSURL); err != nil {
		return verifiedExternalIdentity{}, err
	}
	if len(token) == 0 || len(token) > 32<<10 {
		return verifiedExternalIdentity{}, fmt.Errorf("external identity token is invalid")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return verifiedExternalIdentity{}, fmt.Errorf("external identity token is malformed")
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if decodeJWTPart(parts[0], &header) != nil || header.Algorithm != "RS256" || header.KeyID == "" {
		return verifiedExternalIdentity{}, fmt.Errorf("external identity token header is invalid")
	}
	key, err := fetchOIDCKey(ctx, configuration.JWKSURL, header.KeyID)
	if err != nil {
		return verifiedExternalIdentity{}, fmt.Errorf("external signing key is unavailable: %w", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return verifiedExternalIdentity{}, fmt.Errorf("external identity token signature is invalid")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return verifiedExternalIdentity{}, fmt.Errorf("external identity token signature is invalid")
	}
	var claims struct {
		Issuer        string          `json:"iss"`
		Audience      json.RawMessage `json:"aud"`
		Subject       string          `json:"sub"`
		Email         string          `json:"email"`
		EmailVerified bool            `json:"email_verified"`
		Name          string          `json:"name"`
		Picture       string          `json:"picture"`
		ExpiresAt     int64           `json:"exp"`
		IssuedAt      int64           `json:"iat"`
		AuthTime      int64           `json:"auth_time"`
		Firebase      *struct {
			Tenant         string `json:"tenant"`
			SignInProvider string `json:"sign_in_provider"`
		} `json:"firebase"`
	}
	if err := decodeJWTPart(parts[1], &claims); err != nil {
		return verifiedExternalIdentity{}, fmt.Errorf("external identity token claims are malformed")
	}
	now := time.Now().Unix()
	if claims.Issuer != configuration.Issuer || !jwtAudienceContains(claims.Audience, configuration.Audience) ||
		strings.TrimSpace(claims.Subject) == "" || claims.ExpiresAt <= now || claims.IssuedAt <= 0 || claims.IssuedAt > now+300 {
		return verifiedExternalIdentity{}, fmt.Errorf("external identity token claims are invalid")
	}
	if provider.firebase {
		if claims.Firebase == nil || claims.Firebase.SignInProvider == "" || claims.AuthTime <= 0 || claims.AuthTime > now+300 {
			return verifiedExternalIdentity{}, fmt.Errorf("Firebase ID token claims are invalid")
		}
		if configuration.FirebaseTenant != "" && claims.Firebase.Tenant != configuration.FirebaseTenant {
			return verifiedExternalIdentity{}, fmt.Errorf("Firebase tenant does not match this project")
		}
		if provider.accountPolicy != nil && configuration.AdminSecret != "" {
			if err := provider.accountPolicy(ctx, configuration, strings.TrimSpace(claims.Subject), time.Unix(claims.AuthTime, 0).UTC()); err != nil {
				return verifiedExternalIdentity{}, err
			}
		}
	}
	return verifiedExternalIdentity{
		Provider: configuration.Provider, Issuer: claims.Issuer, Subject: strings.TrimSpace(claims.Subject),
		Email: strings.ToLower(strings.TrimSpace(claims.Email)), EmailVerified: claims.EmailVerified,
		Name: strings.TrimSpace(claims.Name), Picture: strings.TrimSpace(claims.Picture),
		IssuedAt: time.Unix(claims.IssuedAt, 0).UTC(), AuthTime: time.Unix(claims.AuthTime, 0).UTC(),
	}, nil
}

func jwtAudienceContains(raw json.RawMessage, expected string) bool {
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == expected
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, item := range many {
			if item == expected {
				return true
			}
		}
	}
	return false
}

func (s *Server) resolveExternalAccountTx(ctx context.Context, tx *sql.Tx, project string, identity verifiedExternalIdentity) (appAuthAccount, error) {
	lockKey := sha256Hex(project + "\x00" + identity.Provider + "\x00" + identity.Issuer + "\x00" + identity.Subject)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return appAuthAccount{}, err
	}
	accountID := ""
	err := tx.QueryRowContext(ctx, `SELECT identity.account_id FROM account_identities identity
		JOIN accounts account ON account.id=identity.account_id
		WHERE identity.project_id=$1 AND identity.provider=$2 AND identity.issuer=$3 AND identity.subject=$4
		AND account.auth_realm_id=$1`, project, identity.Provider, identity.Issuer, identity.Subject).Scan(&accountID)
	if err != nil && err != sql.ErrNoRows {
		return appAuthAccount{}, err
	}
	resolution := "existing-identity"
	if accountID == "" && identity.EmailVerified && identity.Email != "" {
		rows, err := tx.QueryContext(ctx, `SELECT DISTINCT identity.account_id FROM account_identities identity
			JOIN accounts account ON account.id=identity.account_id
			WHERE identity.project_id=$1 AND identity.verified_email AND lower(identity.email)=lower($2)
			AND account.auth_realm_id=$1 ORDER BY identity.account_id LIMIT 2`, project, identity.Email)
		if err != nil {
			return appAuthAccount{}, err
		}
		matches := []string{}
		for rows.Next() {
			var candidate string
			if err := rows.Scan(&candidate); err != nil {
				rows.Close()
				return appAuthAccount{}, err
			}
			matches = append(matches, candidate)
		}
		rows.Close()
		if len(matches) == 1 {
			accountID = matches[0]
			resolution = "verified-email-link"
		}
	}
	if accountID == "" {
		var err error
		accountID, err = randomID("acct")
		if err != nil {
			return appAuthAccount{}, err
		}
		resolution = "created"
		if _, err := tx.ExecContext(ctx, `INSERT INTO accounts(id,auth_realm_id,email,name,avatar_url,updated_at)
			VALUES($1,$2,$3,$4,$5,now())`, accountID, project, identity.Email, identity.Name, identity.Picture); err != nil {
			return appAuthAccount{}, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET
			email=CASE WHEN $3<>'' THEN $3 ELSE email END,
			name=CASE WHEN $4<>'' THEN $4 ELSE name END,
			avatar_url=CASE WHEN $5<>'' THEN $5 ELSE avatar_url END,updated_at=now()
			WHERE id=$1 AND auth_realm_id=$2`, accountID, project, identity.Email, identity.Name, identity.Picture); err != nil {
			return appAuthAccount{}, err
		}
	}
	if err := tx.QueryRowContext(ctx, `INSERT INTO account_identities(project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,now())
		ON CONFLICT(project_id,provider,issuer,subject) DO UPDATE SET email=EXCLUDED.email,
		verified_email=EXCLUDED.verified_email,updated_at=now() RETURNING account_id`,
		project, accountID, identity.Provider, identity.Issuer, identity.Subject, identity.Email, identity.EmailVerified).Scan(&accountID); err != nil {
		return appAuthAccount{}, err
	}
	_, _ = tx.ExecContext(ctx, `INSERT INTO gonvex_auth_identity_events(project_id,account_id,provider,issuer,subject,resolution)
		VALUES($1,$2,$3,$4,$5,$6)`, project, accountID, identity.Provider, identity.Issuer, identity.Subject, resolution)
	var account appAuthAccount
	err = tx.QueryRowContext(ctx, `SELECT id,id,email,$3::boolean,name,avatar_url,$4::text,disabled_at IS NOT NULL,created_at,updated_at
		FROM accounts WHERE id=$1 AND auth_realm_id=$2 AND disabled_at IS NULL`, accountID, project,
		identity.EmailVerified, identity.Provider).Scan(&account.ID, &account.AccountID, &account.Email,
		&account.EmailVerified, &account.Name, &account.Picture, &account.Provider, &account.Disabled,
		&account.CreatedAt, &account.LastSignedInAt)
	return account, err
}

var externalIdentityProviderFactories = map[string]func(*Server) externalIdentityProvider{
	"firebase": func(server *Server) externalIdentityProvider {
		return signedJWTIdentityProvider{firebase: true, accountPolicy: server.verifyFirebaseAccountPolicy}
	},
	authModeExternalOIDC: func(*Server) externalIdentityProvider {
		return signedJWTIdentityProvider{}
	},
}

func (s *Server) externalIdentityProviderFor(name string) (externalIdentityProvider, error) {
	factory := externalIdentityProviderFactories[strings.ToLower(strings.TrimSpace(name))]
	if factory == nil {
		return nil, fmt.Errorf("external authentication provider is unsupported")
	}
	return factory(s), nil
}
