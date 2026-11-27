package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Adit.Listen != ":9000" {
		t.Fatalf("listen = %q", cfg.Adit.Listen)
	}
}

func TestLoadFileThenEnv(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte("adit:\n  listen: \":1234\"\n  region: eu-west-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LODE_ADIT_LISTEN", ":5678")
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Adit.Listen != ":5678" {
		t.Errorf("env override not applied: %q", cfg.Adit.Listen)
	}
	if cfg.Adit.Region != "eu-west-1" {
		t.Errorf("file value lost: %q", cfg.Adit.Region)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("default lost: %q", cfg.Log.Level)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/lode.yaml"); err == nil {
		t.Fatal("expected error")
	}
}
