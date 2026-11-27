// Package config loads Lode configuration from YAML with LODE_* env overrides.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Log     Log     `yaml:"log"`
	Adit    Adit    `yaml:"adit"`
	Metrics Metrics `yaml:"metrics"`
	Ledger  Ledger  `yaml:"ledger"`
}

type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type Adit struct {
	Listen string `yaml:"listen"`
	Region string `yaml:"region"`
}

type Metrics struct {
	Listen string `yaml:"listen"`
}

type Ledger struct {
	DSN string `yaml:"dsn"`
}

// Default returns the configuration used when no file or env is given.
func Default() Config {
	return Config{
		Log:     Log{Level: "info", Format: "json"},
		Adit:    Adit{Listen: ":9000", Region: "us-east-1"},
		Metrics: Metrics{Listen: ":9090"},
	}
}

// Load reads path (optional, "" skips the file) on top of defaults, then applies env.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path) // #nosec G304 -- operator-supplied config path
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	applyEnv(&cfg)
	return cfg, nil
}

func applyEnv(cfg *Config) {
	for env, dst := range map[string]*string{
		"LODE_LOG_LEVEL":      &cfg.Log.Level,
		"LODE_LOG_FORMAT":     &cfg.Log.Format,
		"LODE_ADIT_LISTEN":    &cfg.Adit.Listen,
		"LODE_ADIT_REGION":    &cfg.Adit.Region,
		"LODE_METRICS_LISTEN": &cfg.Metrics.Listen,
		"LODE_LEDGER_DSN":     &cfg.Ledger.DSN,
	} {
		if v, ok := os.LookupEnv(env); ok {
			*dst = v
		}
	}
}
