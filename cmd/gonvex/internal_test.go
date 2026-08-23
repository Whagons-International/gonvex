package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInternalProvisionTenantRequiresAdminCredentialAndUsesDevHarness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/dev/tenants" {
			t.Fatalf("request=%s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("authorization") != "Bearer secret" {
			t.Fatal("admin credential was not forwarded")
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["projectId"] != "shop" || payload["name"] != "E2E" {
			t.Fatalf("payload=%#v", payload)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant": map[string]string{"id": "tenant-a"}})
	}))
	defer server.Close()
	if err := runInternal([]string{"provision-tenant", "--runtime", server.URL, "--project", "shop", "--admin-key", "secret", "--tenant-name", "E2E"}); err != nil {
		t.Fatal(err)
	}
}

func TestInternalHarnessIsNotAvailableWithoutAdminCredential(t *testing.T) {
	if err := runInternal([]string{"e2e-setup", "--runtime", "http://runtime.test", "--project", "shop", "--tenant-name", "E2E"}); err == nil {
		t.Fatal("internal harness accepted a request without an admin key")
	}
}
