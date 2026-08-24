package server

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
)

// Control Plane provider credentials are encrypted before persistence with a
// runtime-owned key. Tenant modules and browser responses never receive this
// key or the decrypted value.
func (s *Server) controlSecretAEAD() (cipher.AEAD, error) {
	secret := strings.TrimSpace(s.dashboardSecret())
	if secret == "" {
		return nil, fmt.Errorf("GONVEX_DASHBOARD_SESSION_SECRET is required to store provider credentials")
	}
	key := sha256.Sum256([]byte("gonvex-control-secret-v1\x00" + secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (s *Server) encryptControlSecret(plaintext string) ([]byte, error) {
	aead, err := s.controlSecretAEAD()
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, []byte(plaintext), []byte("gonvex-auth-provider")), nil
}

func (s *Server) decryptControlSecret(ciphertext []byte) (string, error) {
	aead, err := s.controlSecretAEAD()
	if err != nil {
		return "", err
	}
	if len(ciphertext) < aead.NonceSize() {
		return "", fmt.Errorf("provider credential is invalid")
	}
	nonce := ciphertext[:aead.NonceSize()]
	plaintext, err := aead.Open(nil, nonce, ciphertext[aead.NonceSize():], []byte("gonvex-auth-provider"))
	if err != nil {
		return "", fmt.Errorf("provider credential cannot be decrypted")
	}
	return string(plaintext), nil
}
