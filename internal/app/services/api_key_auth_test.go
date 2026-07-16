package services

import (
	"errors"
	"testing"
	"time"
)

type apiKeyTestVault struct {
	values map[string]string
	err    error
}

func (v apiKeyTestVault) Get(key string) (string, error) {
	if v.err != nil {
		return "", v.err
	}
	return v.values[key], nil
}

func (v apiKeyTestVault) GetMany(keys []string) (map[string]string, error) {
	return nil, errors.New("not used")
}

func TestAPIKeyAuthenticatorAcceptsConfiguredKey(t *testing.T) {
	auth := NewAPIKeyAuthenticator(apiKeyTestVault{values: map[string]string{
		"current": "current-key",
	}}, "current", time.Minute, "")

	valid, keyFingerprint, err := auth.Validate("current-key")
	if err != nil || !valid || keyFingerprint == "" {
		t.Fatalf("Validate = %v, %q, %v", valid, keyFingerprint, err)
	}
	valid, _, err = auth.Validate("wrong-key")
	if err != nil || valid {
		t.Fatalf("invalid key accepted: valid=%v err=%v", valid, err)
	}
}

func TestAPIKeyAuthenticatorFailsClosedWhenVaultFails(t *testing.T) {
	auth := NewAPIKeyAuthenticator(apiKeyTestVault{err: errors.New("vault unavailable")}, "current", time.Minute, "")
	if valid, _, err := auth.Validate("provided-key"); err == nil || valid {
		t.Fatalf("Validate should fail closed: valid=%v err=%v", valid, err)
	}
}

func TestAPIKeyAuthenticatorWarmFailsForEmptyCurrentKey(t *testing.T) {
	auth := NewAPIKeyAuthenticator(apiKeyTestVault{values: map[string]string{"current": "  "}}, "current", time.Minute, "")
	if err := auth.Warm(); err == nil {
		t.Fatal("Warm should reject an empty current key")
	}
}
