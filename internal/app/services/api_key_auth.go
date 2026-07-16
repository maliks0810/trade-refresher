package services

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	"refresher/trade-refresher/internal/utils/azure"
)

type APIKeyAuthenticator struct {
	vault            azure.Vault
	secretName       string
	cacheTTL         time.Duration
	localFallbackKey string

	mu        sync.RWMutex
	key       string
	expiresAt time.Time
}

func NewAPIKeyAuthenticator(vault azure.Vault, secretName string, cacheTTL time.Duration, localFallbackKey string) *APIKeyAuthenticator {
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Minute
	}
	return &APIKeyAuthenticator{
		vault:            vault,
		secretName:       secretName,
		cacheTTL:         cacheTTL,
		localFallbackKey: localFallbackKey,
	}
}

func (a *APIKeyAuthenticator) Validate(provided string) (bool, string, error) {
	provided = strings.TrimSpace(provided)
	if provided == "" {
		return false, "", nil
	}
	key, err := a.keyForValidation()
	if err != nil {
		return false, "", err
	}
	if constantTimeEqual(provided, key) {
		return true, fingerprint(key), nil
	}
	return false, "", nil
}

func (a *APIKeyAuthenticator) Warm() error {
	if a == nil {
		return errors.New("trade refresher api key authenticator is not configured")
	}
	_, err := a.keyForValidation()
	return err
}

func (a *APIKeyAuthenticator) keyForValidation() (string, error) {
	now := time.Now()
	a.mu.RLock()
	if now.Before(a.expiresAt) && a.key != "" {
		key := a.key
		a.mu.RUnlock()
		return key, nil
	}
	a.mu.RUnlock()

	a.mu.Lock()
	defer a.mu.Unlock()
	if now.Before(a.expiresAt) && a.key != "" {
		return a.key, nil
	}

	var key string
	if a.vault != nil && a.secretName != "" {
		current, err := a.vault.Get(a.secretName)
		if err != nil {
			return "", err
		}
		current = strings.TrimSpace(current)
		if current == "" {
			return "", errors.New("trade refresher api key is empty")
		}
		key = current
	} else if a.localFallbackKey != "" {
		key = a.localFallbackKey
	} else {
		return "", errors.New("trade refresher api key is not configured")
	}

	a.key = key
	a.expiresAt = now.Add(a.cacheTTL)
	return key, nil
}

func constantTimeEqual(a, b string) bool {
	aHash := sha256.Sum256([]byte(a))
	bHash := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(aHash[:], bHash[:]) == 1
}

func fingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:12]
}
