// Package config loads Lode configuration from YAML with LODE_* env overrides.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Log      Log      `yaml:"log"`
	Adit     Adit     `yaml:"adit"`
	Metrics  Metrics  `yaml:"metrics"`
	Ledger   Ledger   `yaml:"ledger"`
	Vein     Vein     `yaml:"vein"`
	Tailings Tailings `yaml:"tailings"`
}

// Tailings configures garbage collection.
type Tailings struct {
	Grace        time.Duration `yaml:"grace"`         // keep unreferenced nuggets this long
	Interval     time.Duration `yaml:"interval"`      // time between passes
	UploadExpiry time.Duration `yaml:"upload_expiry"` // abort multipart uploads older than this; 0 disables
	CompactRatio float64       `yaml:"compact_ratio"` // compact segments at least this fraction dead
}

type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type Adit struct {
	Listen     string      `yaml:"listen"`
	Region     string      `yaml:"region"`
	AccessKeys []AccessKey `yaml:"access_keys"`
}

// AccessKey is a static SigV4 credential.
type AccessKey struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type Vein struct {
	DataDir string `yaml:"data_dir"`
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
		Vein:    Vein{DataDir: "./data"},
		Tailings: Tailings{
			Grace: 24 * time.Hour, Interval: 5 * time.Minute, UploadExpiry: 7 * 24 * time.Hour, CompactRatio: 0.5,
		},
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
		"LODE_VEIN_DATA_DIR":  &cfg.Vein.DataDir,
	} {
		if v, ok := os.LookupEnv(env); ok {
			*dst = v
		}
	}
	for env, dst := range map[string]*time.Duration{
		"LODE_TAILINGS_GRACE":         &cfg.Tailings.Grace,
		"LODE_TAILINGS_INTERVAL":      &cfg.Tailings.Interval,
		"LODE_TAILINGS_UPLOAD_EXPIRY": &cfg.Tailings.UploadExpiry,
	} {
		if v, ok := os.LookupEnv(env); ok {
			if d, err := time.ParseDuration(v); err == nil {
				*dst = d
			}
		}
	}
	ak, sk := os.Getenv("LODE_ACCESS_KEY"), os.Getenv("LODE_SECRET_KEY")
	if ak != "" && sk != "" {
		cfg.Adit.AccessKeys = append(cfg.Adit.AccessKeys, AccessKey{AccessKey: ak, SecretKey: sk})
	}
}

// Validate reports configuration that cannot work.
func (c Config) Validate() error {
	if c.Ledger.DSN == "" {
		return fmt.Errorf("config: ledger.dsn (LODE_LEDGER_DSN) is required")
	}
	if len(c.Adit.AccessKeys) == 0 {
		return fmt.Errorf("config: at least one adit.access_keys entry (or LODE_ACCESS_KEY/LODE_SECRET_KEY) is required")
	}
	if c.Tailings.Interval <= 0 {
		return fmt.Errorf("config: tailings.interval must be positive")
	}
	for _, k := range c.Adit.AccessKeys {
		if k.AccessKey == "" || k.SecretKey == "" {
			return fmt.Errorf("config: access keys need both access_key and secret_key")
		}
	}
	return nil
}
