// Package config loads Lode configuration from YAML with LODE_* env overrides.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Log      Log      `yaml:"log"`
	Adit     Adit     `yaml:"adit"`
	Metrics  Metrics  `yaml:"metrics"`
	Ledger   Ledger   `yaml:"ledger"`
	Vein     Vein     `yaml:"vein"`
	Cluster  Cluster  `yaml:"cluster"`
	Tailings Tailings `yaml:"tailings"`
	Canary   Canary   `yaml:"canary"`
	Smelter  Smelter  `yaml:"smelter"`
}

// Canary configures failure detection.
type Canary struct {
	SuspectPhi float64       `yaml:"suspect_phi"` // phi above which a vein takes no writes
	DownAfter  time.Duration `yaml:"down_after"`  // silence after which a vein is down and repaired around
	Interval   time.Duration `yaml:"interval"`
}

// Smelter configures repair.
type Smelter struct {
	Workers        int   `yaml:"workers"`
	BandwidthBytes int64 `yaml:"bandwidth_bytes"` // repair writes per second; 0 = unlimited
}

type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type Adit struct {
	Listen      string      `yaml:"listen"`
	Region      string      `yaml:"region"`
	AccessKeys  []AccessKey `yaml:"access_keys"`
	Replication int         `yaml:"replication"`  // default replicas per nugget
	WriteQuorum int         `yaml:"write_quorum"` // 0 = majority of replication
	Placement   string      `yaml:"placement"`    // hrw | zone-spread | capacity-weighted
	// Rebalance is how often to check whether the vein set changed; 0 disables.
	Rebalance time.Duration `yaml:"rebalance"`
}

// AccessKey is a static SigV4 credential.
type AccessKey struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

// Vein configures a storage node daemon.
type Vein struct {
	ID              string        `yaml:"id"`        // stable identity; defaults to one persisted in data_dir
	Listen          string        `yaml:"listen"`    // gRPC listen address
	Advertise       string        `yaml:"advertise"` // address Adit dials; defaults to listen
	Zone            string        `yaml:"zone"`
	Host            string        `yaml:"host"`
	DataDir         string        `yaml:"data_dir"`
	Heartbeat       time.Duration `yaml:"heartbeat"`
	CompactInterval time.Duration `yaml:"compact_interval"`
	CompactRatio    float64       `yaml:"compact_ratio"` // compact segments at least this fraction dead
	ScrubRate       int64         `yaml:"scrub_rate"`    // Assay bytes per second; 0 = unlimited
	ScrubPause      time.Duration `yaml:"scrub_pause"`   // wait between scrub passes; negative disables Assay
}

// Cluster configures Adit's view of the veins.
type Cluster struct {
	StaleAfter time.Duration `yaml:"stale_after"` // veins with older heartbeats take no writes
}

type Metrics struct {
	Listen string `yaml:"listen"`
}

type Ledger struct {
	DSN string `yaml:"dsn"`
}

// Tailings configures garbage collection.
type Tailings struct {
	Grace        time.Duration `yaml:"grace"`         // keep unreferenced nuggets this long
	Interval     time.Duration `yaml:"interval"`      // time between passes
	UploadExpiry time.Duration `yaml:"upload_expiry"` // abort multipart uploads older than this; 0 disables
}

// Default returns the configuration used when no file or env is given.
func Default() Config {
	return Config{
		Log:     Log{Level: "info", Format: "json"},
		Adit:    Adit{Listen: ":9000", Region: "us-east-1", Replication: 3, Placement: "zone-spread", Rebalance: time.Minute},
		Metrics: Metrics{Listen: ":9090"},
		Vein: Vein{
			Listen: ":7000", DataDir: "./data", Heartbeat: 2 * time.Second,
			CompactInterval: 10 * time.Minute, CompactRatio: 0.5,
			ScrubRate: 32 << 20, ScrubPause: time.Hour,
		},
		Canary:  Canary{SuspectPhi: 8, DownAfter: 30 * time.Second, Interval: time.Second},
		Smelter: Smelter{Workers: 4, BandwidthBytes: 64 << 20},
		Cluster: Cluster{StaleAfter: 10 * time.Second},
		Tailings: Tailings{
			Grace: 24 * time.Hour, Interval: 5 * time.Minute, UploadExpiry: 7 * 24 * time.Hour,
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
		"LODE_VEIN_ID":        &cfg.Vein.ID,
		"LODE_VEIN_LISTEN":    &cfg.Vein.Listen,
		"LODE_VEIN_ADVERTISE": &cfg.Vein.Advertise,
		"LODE_VEIN_ZONE":      &cfg.Vein.Zone,
		"LODE_VEIN_HOST":      &cfg.Vein.Host,
		"LODE_VEIN_DATA_DIR":  &cfg.Vein.DataDir,
		"LODE_ADIT_PLACEMENT": &cfg.Adit.Placement,
	} {
		if v, ok := os.LookupEnv(env); ok {
			*dst = v
		}
	}
	for env, dst := range map[string]*int{
		"LODE_ADIT_REPLICATION":  &cfg.Adit.Replication,
		"LODE_ADIT_WRITE_QUORUM": &cfg.Adit.WriteQuorum,
		"LODE_SMELTER_WORKERS":   &cfg.Smelter.Workers,
	} {
		if v, ok := os.LookupEnv(env); ok {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	for env, dst := range map[string]*int64{
		"LODE_SMELTER_BANDWIDTH_BYTES": &cfg.Smelter.BandwidthBytes,
		"LODE_VEIN_SCRUB_RATE":         &cfg.Vein.ScrubRate,
	} {
		if v, ok := os.LookupEnv(env); ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				*dst = n
			}
		}
	}
	for env, dst := range map[string]*time.Duration{
		"LODE_TAILINGS_GRACE":         &cfg.Tailings.Grace,
		"LODE_TAILINGS_INTERVAL":      &cfg.Tailings.Interval,
		"LODE_TAILINGS_UPLOAD_EXPIRY": &cfg.Tailings.UploadExpiry,
		"LODE_VEIN_HEARTBEAT":         &cfg.Vein.Heartbeat,
		"LODE_VEIN_COMPACT_INTERVAL":  &cfg.Vein.CompactInterval,
		"LODE_VEIN_SCRUB_PAUSE":       &cfg.Vein.ScrubPause,
		"LODE_CANARY_DOWN_AFTER":      &cfg.Canary.DownAfter,
		"LODE_ADIT_REBALANCE":         &cfg.Adit.Rebalance,
		"LODE_CLUSTER_STALE_AFTER":    &cfg.Cluster.StaleAfter,
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

// ValidateAdit reports configuration that cannot work for lode-adit.
func (c Config) ValidateAdit() error {
	if c.Ledger.DSN == "" {
		return fmt.Errorf("config: ledger.dsn (LODE_LEDGER_DSN) is required")
	}
	if len(c.Adit.AccessKeys) == 0 {
		return fmt.Errorf("config: at least one adit.access_keys entry (or LODE_ACCESS_KEY/LODE_SECRET_KEY) is required")
	}
	for _, k := range c.Adit.AccessKeys {
		if k.AccessKey == "" || k.SecretKey == "" {
			return fmt.Errorf("config: access keys need both access_key and secret_key")
		}
	}
	if c.Adit.Replication < 1 || c.Adit.Replication > 16 {
		return fmt.Errorf("config: adit.replication must be between 1 and 16")
	}
	if c.Adit.WriteQuorum < 0 || c.Adit.WriteQuorum > c.Adit.Replication {
		return fmt.Errorf("config: adit.write_quorum must be between 0 and adit.replication")
	}
	if c.Tailings.Interval <= 0 {
		return fmt.Errorf("config: tailings.interval must be positive")
	}
	return nil
}

// ValidateVein reports configuration that cannot work for lode-vein.
func (c Config) ValidateVein() error {
	if c.Ledger.DSN == "" {
		return fmt.Errorf("config: ledger.dsn (LODE_LEDGER_DSN) is required")
	}
	if c.Vein.Listen == "" || c.Vein.DataDir == "" {
		return fmt.Errorf("config: vein.listen and vein.data_dir are required")
	}
	if c.Vein.Heartbeat <= 0 {
		return fmt.Errorf("config: vein.heartbeat must be positive")
	}
	return nil
}
