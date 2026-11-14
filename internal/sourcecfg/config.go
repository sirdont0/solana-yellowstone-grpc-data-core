// Package sourcecfg defines an explicit multi-provider source inventory for
// experiments. Multiple sessions at one failure domain are never counted as
// independent confirmations of chain history.
package sourcecfg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
)

var safeName = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,47}$`)
var safeEnv = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

type Inventory struct {
	Version int      `json:"version"`
	Sources []Source `json:"sources"`
}

// Source represents one independently managed gRPC connection. Provider and
// FailureDomain are deliberately different fields: two accounts, backend hosts
// or tokens at the same provider may still share one failure domain.
type Source struct {
	ID            string   `json:"id"`
	Provider      string   `json:"provider"`
	FailureDomain string   `json:"failure_domain"`
	Endpoint      string   `json:"endpoint"`
	TokenEnv      string   `json:"token_env"`
	TokenHeader   string   `json:"token_header"`
	Commitment    string   `json:"commitment"`
	FilterVersion string   `json:"filter_version"`
	Programs      []string `json:"programs"`
	Enabled       bool     `json:"enabled"`
}

type Topology struct {
	Connections        int            `json:"connections"`
	IndependentDomains int            `json:"independent_domains"`
	ByFailureDomain    map[string]int `json:"by_failure_domain"`
	Warnings           []string       `json:"warnings,omitempty"`
}

func Load(path string) (Inventory, error) {
	f, err := os.Open(path)
	if err != nil {
		return Inventory{}, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 1<<20))
	dec.DisallowUnknownFields()
	var inventory Inventory
	if err := dec.Decode(&inventory); err != nil {
		return Inventory{}, err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Inventory{}, errors.New("config must contain exactly one JSON object")
	}
	return inventory, inventory.Validate()
}

func (inv Inventory) Validate() error {
	if inv.Version != 1 {
		return fmt.Errorf("unsupported source inventory version %d", inv.Version)
	}
	if len(inv.Sources) == 0 || len(inv.Sources) > 16 {
		return errors.New("sources count must be 1..16")
	}
	ids := make(map[string]struct{}, len(inv.Sources))
	providerDomains := make(map[string]string)
	for _, s := range inv.Sources {
		if !safeName.MatchString(s.ID) || !safeName.MatchString(s.Provider) || !safeName.MatchString(s.FailureDomain) {
			return fmt.Errorf("source %q: invalid id, provider or failure_domain", s.ID)
		}
		if _, exists := ids[s.ID]; exists {
			return fmt.Errorf("duplicate source id %q", s.ID)
		}
		ids[s.ID] = struct{}{}
		if prior, ok := providerDomains[s.Provider]; ok && prior != s.FailureDomain {
			return fmt.Errorf("provider %q uses inconsistent failure domains %q and %q", s.Provider, prior, s.FailureDomain)
		}
		providerDomains[s.Provider] = s.FailureDomain
		if strings.Contains(s.Endpoint, "://") || strings.ContainsAny(s.Endpoint, "@/?#") {
			return fmt.Errorf("source %q: endpoint must be host:port without embedded credentials", s.ID)
		}
		if host, port, err := net.SplitHostPort(s.Endpoint); err != nil || host == "" || port == "" {
			return fmt.Errorf("source %q: endpoint must be host:port", s.ID)
		}
		if s.TokenEnv != "" && !safeEnv.MatchString(s.TokenEnv) {
			return fmt.Errorf("source %q: token_env must be a secret environment variable name", s.ID)
		}
		if strings.TrimSpace(s.TokenHeader) == "" || strings.ContainsAny(s.TokenHeader, " \r\n:") {
			return fmt.Errorf("source %q: invalid token_header", s.ID)
		}
		if s.Commitment != "finalized" && s.Commitment != "confirmed" && s.Commitment != "processed" {
			return fmt.Errorf("source %q: invalid commitment %q", s.ID, s.Commitment)
		}
		if s.FilterVersion == "" || len(s.Programs) == 0 {
			return fmt.Errorf("source %q: filter_version and programs required", s.ID)
		}
	}
	return nil
}

// Analyze only counts enabled sources. Its independent_domains number is a
// topology property, not a statement that any source's data is complete.
func (inv Inventory) Analyze() Topology {
	t := Topology{ByFailureDomain: make(map[string]int)}
	for _, s := range inv.Sources {
		if s.Enabled {
			t.Connections++
			t.ByFailureDomain[s.FailureDomain]++
		}
	}
	t.IndependentDomains = len(t.ByFailureDomain)
	var domains []string
	for domain := range t.ByFailureDomain {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	for _, domain := range domains {
		if n := t.ByFailureDomain[domain]; n > 1 {
			t.Warnings = append(t.Warnings, fmt.Sprintf("%d connections share failure domain %q; shared outage and quota remain", n, domain))
		}
	}
	if t.Connections == 0 {
		t.Warnings = append(t.Warnings, "no enabled source")
	}
	return t
}
