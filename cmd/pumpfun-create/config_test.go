package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
)

func writeConfig(t *testing.T, json string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(json), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CORE_CONFIG", path)
}

func TestSettingsProvidersAndDelivery(t *testing.T) {
	writeConfig(t, `{"source_count":2,"delivery":"durable","queue_capacity":128}`)
	t.Setenv("CORE_PROVIDER_A_ENDPOINT", "primary.example:443")
	t.Setenv("CORE_PROVIDER_A_TOKEN", "secret-a")
	t.Setenv("CORE_PROVIDER_B_ENDPOINT", "secondary.example:443")
	t.Setenv("CORE_PROVIDER_B_TOKEN", "secret-b")
	c, err := loadSettings()
	if err != nil {
		t.Fatal(err)
	}
	got := c.coreConfig()
	if len(got.Sources) != 2 || got.Delivery != core.Durable || got.QueueCapacity != 128 {
		t.Fatalf("unexpected engine configuration: %+v", got)
	}
	if got.Sources[0].Token != "secret-a" || got.Sources[1].Token != "secret-b" {
		t.Fatal("provider tokens were not resolved from environment")
	}
	if got.Sources[0].Endpoint != "primary.example:443" || got.Sources[1].Endpoint != "secondary.example:443" {
		t.Fatal("provider endpoints were not resolved from environment")
	}
}

func TestSettingsRequireSecondProviderAndRejectSecretsInJSON(t *testing.T) {
	writeConfig(t, `{"source_count":2}`)
	t.Setenv("CORE_PROVIDER_A_ENDPOINT", "localhost:443")
	t.Setenv("CORE_PROVIDER_B_ENDPOINT", "")
	if _, err := loadSettings(); err == nil || !strings.Contains(err.Error(), "CORE_PROVIDER_B_ENDPOINT") {
		t.Fatalf("missing second provider must be rejected, got %v", err)
	}
	writeConfig(t, `{"token":"secret-in-config"}`)
	if _, err := loadSettings(); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("secret field in non-secret JSON config must be rejected, got %v", err)
	}
}

func TestSettingsRejectUnknownDeliveryMode(t *testing.T) {
	writeConfig(t, `{"delivery":"instant"}`)
	if _, err := loadSettings(); err == nil {
		t.Fatal("unknown delivery mode accepted")
	}
}

func TestSettingsRequirePublicNodeTokenWithoutBlockingTokenlessProviders(t *testing.T) {
	writeConfig(t, `{"source_count":1}`)
	t.Setenv("CORE_REPLAY_ONLY", "0")
	t.Setenv("CORE_PROVIDER_A_ENDPOINT", "solana-yellowstone-grpc.publicnode.com:443")
	t.Setenv("CORE_PROVIDER_A_TOKEN", "")
	t.Setenv("CORE_TOKEN", "")
	if _, err := loadSettings(); err == nil || !strings.Contains(err.Error(), "CORE_PROVIDER_A_TOKEN") {
		t.Fatalf("missing PublicNode token should fail fast, got %v", err)
	}

	t.Setenv("CORE_PROVIDER_A_ENDPOINT", "localhost:443")
	if _, err := loadSettings(); err != nil {
		t.Fatalf("providers without token requirements should be allowed: %v", err)
	}

	t.Setenv("CORE_PROVIDER_A_ENDPOINT", "solana-yellowstone-grpc.publicnode.com:443")
	t.Setenv("CORE_PROVIDER_A_TOKEN", "present")
	if _, err := loadSettings(); err != nil {
		t.Fatalf("PublicNode with a token should be allowed: %v", err)
	}
	t.Setenv("CORE_PROVIDER_A_TOKEN", "")
	t.Setenv("CORE_REPLAY_ONLY", "1")
	if _, err := loadSettings(); err != nil {
		t.Fatalf("offline WAL replay should not require provider credentials: %v", err)
	}
}
