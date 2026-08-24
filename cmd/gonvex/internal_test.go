package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestDeterministicE2ETenantIDIsStableAndShardSpecific(t *testing.T) {
	first := deterministicE2ETenantID("shop", "E2E", "one")
	if first != deterministicE2ETenantID("shop", "E2E", "one") {
		t.Fatal("same E2E shard produced a different tenant id")
	}
	if first == deterministicE2ETenantID("shop", "E2E", "two") {
		t.Fatal("different E2E shards produced the same tenant id")
	}
	if len(first) != 36 || first[14] != '6' {
		t.Fatalf("E2E tenant id is not UUIDv6-shaped: %q", first)
	}
}

func TestE2EShardProvisioningCreatesTenantThenClonesActor(t *testing.T) {
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("authorization") != "Bearer secret" {
			t.Fatal("admin credential was not forwarded")
		}
		requests = append(requests, r.URL.Path)
		if r.URL.Path == "/dev/tenants" {
			_ = json.NewEncoder(w).Encode(map[string]any{"tenant": map[string]string{"id": "stable"}})
			return
		}
		if r.URL.Path == "/dev/internal/e2e/members" {
			var payload map[string]string
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload["tenantId"] == "" || payload["email"] != "actor@example.test" {
				t.Fatalf("member payload=%#v", payload)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accountId": "acct", "memberId": "member"})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	if err := runInternal([]string{"e2e-shard", "--runtime", server.URL, "--project", "shop", "--admin-key", "secret", "--tenant-name", "E2E", "--shard", "worker-1", "--email", "actor@example.test"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(requests, []string{"/dev/tenants", "/dev/internal/e2e/members"}) {
		t.Fatalf("requests=%v", requests)
	}
}

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
