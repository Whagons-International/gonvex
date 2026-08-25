package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func runInternal(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: gonvex internal (provision-tenant | resolve-identity | e2e-setup) [options]")
	}
	command := args[0]
	flags := flag.NewFlagSet("internal "+command, flag.ContinueOnError)
	runtimeURL := flags.String("runtime", "", "Gonvex runtime URL")
	project := flags.String("project", "", "project id")
	adminKey := flags.String("admin-key", "", "runtime admin key")
	tenantName := flags.String("tenant-name", "", "tenant name")
	tenantID := flags.String("tenant-id", "", "optional UUIDv6 tenant id")
	email := flags.String("email", "", "account email to resolve")
	shard := flags.String("shard", "", "stable E2E shard name")
	artifactFile := flags.String("file", "", "module artifact JSON file")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if command == "verify-module-artifact" {
		if strings.TrimSpace(*artifactFile) == "" {
			return fmt.Errorf("--file is required")
		}
		raw, err := os.ReadFile(*artifactFile)
		if err != nil {
			return err
		}
		var artifact manifest.ModuleArtifact
		if err := json.Unmarshal(raw, &artifact); err != nil {
			return fmt.Errorf("decode module artifact: %w", err)
		}
		if err := artifact.Validate(); err != nil {
			return err
		}
		fmt.Println(artifact.Hash)
		return nil
	}
	if strings.TrimSpace(*runtimeURL) == "" || strings.TrimSpace(*project) == "" || strings.TrimSpace(*adminKey) == "" {
		return fmt.Errorf("--runtime, --project, and --admin-key are required")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	switch command {
	case "provision-tenant", "e2e-setup", "e2e-base", "e2e-shard":
		if strings.TrimSpace(*tenantName) == "" {
			return fmt.Errorf("--tenant-name is required")
		}
		payload := map[string]string{"projectId": *project, "name": *tenantName}
		if command != "provision-tenant" && *tenantID == "" {
			*tenantID = deterministicE2ETenantID(*project, *tenantName, *shard)
		}
		if *tenantID != "" {
			payload["id"] = *tenantID
		}
		raw, err := internalRequestBytes(client, *runtimeURL, *adminKey, http.MethodPost, "/dev/tenants", payload)
		if err != nil {
			return err
		}
		if strings.TrimSpace(*email) != "" {
			memberRaw, memberErr := internalRequestBytes(client, *runtimeURL, *adminKey, http.MethodPost, "/dev/internal/e2e/members", map[string]string{"projectId": *project, "tenantId": *tenantID, "email": *email})
			if memberErr != nil {
				return memberErr
			}
			fmt.Println(string(memberRaw))
			return nil
		}
		fmt.Println(string(raw))
		return nil
	case "clone-test-actor":
		if strings.TrimSpace(*tenantID) == "" || strings.TrimSpace(*email) == "" {
			return fmt.Errorf("--tenant-id and --email are required")
		}
		return internalRequest(client, *runtimeURL, *adminKey, http.MethodPost, "/dev/internal/e2e/members", map[string]string{"projectId": *project, "tenantId": *tenantID, "email": *email})
	case "resolve-identity":
		if strings.TrimSpace(*email) == "" {
			return fmt.Errorf("--email is required")
		}
		path := "/dev/projects/" + url.PathEscape(*project) + "/auth/accounts"
		raw, err := internalRequestBytes(client, *runtimeURL, *adminKey, http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		var response struct {
			Accounts []map[string]any `json:"accounts"`
		}
		if err := json.Unmarshal(raw, &response); err != nil {
			return fmt.Errorf("decode identity response: %w", err)
		}
		for _, account := range response.Accounts {
			candidate, _ := account["email"].(string)
			if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(*email)) {
				encoded, _ := json.Marshal(account)
				fmt.Println(string(encoded))
				return nil
			}
		}
		return fmt.Errorf("account %q was not found", *email)
	default:
		return fmt.Errorf("unknown internal command %q", command)
	}
}

func deterministicE2ETenantID(project, name, shard string) string {
	sum := sha256.Sum256([]byte("gonvex-e2e-v1\x00" + project + "\x00" + name + "\x00" + shard))
	raw := sum[:16]
	raw[6] = (raw[6] & 0x0f) | 0x60
	raw[8] = (raw[8] & 0x3f) | 0x80
	hex := fmt.Sprintf("%x", raw)
	return hex[:8] + "-" + hex[8:12] + "-" + hex[12:16] + "-" + hex[16:20] + "-" + hex[20:]
}

func internalRequest(client *http.Client, runtimeURL, adminKey, method, path string, payload any) error {
	raw, err := internalRequestBytes(client, runtimeURL, adminKey, method, path, payload)
	if err != nil {
		return err
	}
	fmt.Println(string(raw))
	return nil
}

func internalRequestBytes(client *http.Client, runtimeURL, adminKey, method, path string, payload any) ([]byte, error) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}
	request, err := http.NewRequest(method, strings.TrimRight(runtimeURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("authorization", "Bearer "+adminKey)
	request.Header.Set("content-type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("internal Gonvex request returned %d: %s", response.StatusCode, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}
