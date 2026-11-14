package sourcecfg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func source(id, domain string) Source {
	return Source{ID: id, Provider: "publicnode", FailureDomain: domain,
		Endpoint: "solana-yellowstone-grpc.publicnode.com:443", TokenEnv: "YLAB_TOKEN",
		TokenHeader: "x-token", Commitment: "finalized", FilterVersion: "pump-v1", Programs: []string{"PumpProgram"}, Enabled: true}
}

func TestSharedFailureDomainIsNotIndependent(t *testing.T) {
	inv := Inventory{Version: 1, Sources: []Source{source("pna", "publicnode"), source("pnb", "publicnode"), source("pnc", "publicnode")}}
	if err := inv.Validate(); err != nil {
		t.Fatal(err)
	}
	top := inv.Analyze()
	if top.Connections != 3 || top.IndependentDomains != 1 || len(top.Warnings) != 1 {
		t.Fatalf("unexpected shared-domain topology: %+v", top)
	}
	inv.Sources = append(inv.Sources, source("other", "other-provider"))
	inv.Sources[3].Provider = "other-provider"
	if err := inv.Validate(); err != nil {
		t.Fatal(err)
	}
	if inv.Analyze().IndependentDomains != 2 {
		t.Fatal("adding independent provider must add one failure domain")
	}
}

func TestSameProviderCannotInventIndependentFailureDomains(t *testing.T) {
	inv := Inventory{Version: 1, Sources: []Source{source("pna", "publicnode-one"), source("pnb", "publicnode-two")}}
	if err := inv.Validate(); err == nil {
		t.Fatal("different labels must not make one provider independent")
	}
}

func TestConfigRejectsSecretsAndAmbiguity(t *testing.T) {
	base := source("pna", "publicnode")
	for name, mutate := range map[string]func(*Source){
		"inline-token":           func(s *Source) { s.Endpoint = "token@provider.example:443" },
		"URL-token":              func(s *Source) { s.Endpoint = "https://provider.example:443?key=secret" },
		"invalid-env":            func(s *Source) { s.TokenEnv = "TOKEN=secret" },
		"missing-failure-domain": func(s *Source) { s.FailureDomain = "" },
		"missing-filter":         func(s *Source) { s.FilterVersion = "" },
	} {
		t.Run(name, func(t *testing.T) {
			s := base
			mutate(&s)
			if err := (Inventory{Version: 1, Sources: []Source{s}}).Validate(); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}

func TestStrictLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.json")
	for _, content := range []string{
		`{"version":1,"sources":[],"secret":"x"}`,
		`{"version":1,"sources":[]} {"version":1}`,
	} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil || strings.TrimSpace(err.Error()) == "" {
			t.Fatal("expected strict JSON failure")
		}
	}
}
