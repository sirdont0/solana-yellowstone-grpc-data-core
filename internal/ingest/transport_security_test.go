package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestProviderTokenCannotUsePlaintextGRPC(t *testing.T) {
	for _, endpoint := range []string{"grpc://127.0.0.1:1", "http://127.0.0.1:1"} {
		t.Run(endpoint, func(t *testing.T) {
			cfg := integrationConfig(t, endpoint)
			cfg.Token = "test-token"
			if err := cfg.Validate(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "tls") {
				t.Fatalf("expected early TLS validation error, got %v", err)
			}
			conn, err := GRPCDial(context.Background(), cfg)
			if conn != nil {
				_ = conn.Close()
				t.Fatal("plaintext credential connection was created")
			}
			if err == nil || !strings.Contains(err.Error(), "plaintext") {
				t.Fatalf("expected plaintext credential rejection, got %v", err)
			}
		})
	}
	cfg := integrationConfig(t, "grpc://127.0.0.1:1")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("tokenless local test transport should remain allowed: %v", err)
	}
}

func TestSanitizeErrRedactsProviderToken(t *testing.T) {
	got := sanitizeErr(errors.New("provider error token=private-demo-token\nretry"), "private-demo-token")
	if strings.Contains(got, "private-demo-token") || !strings.Contains(got, "[REDACTED]") || strings.Contains(got, "\n") {
		t.Fatalf("error message did not redact secret safely: %q", got)
	}
}
