// Package config loads and validates pgsc-mirror configuration.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const defaultUpstream = "https://ftp.ebi.ac.uk/pub/databases/spot/pgs"

// Duration is a TOML text duration such as "30s" or "24h".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// ByteSize is a TOML byte count such as "10GiB" or "500MB".
type ByteSize struct{ Bytes int64 }

func (s *ByteSize) UnmarshalText(text []byte) error {
	raw := strings.TrimSpace(string(text))
	upper := strings.ToUpper(raw)
	units := []struct {
		suffix string
		value  uint64
	}{
		{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"TB", 1_000_000_000_000}, {"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000}, {"B", 1},
	}
	number := raw
	multiplier := uint64(1)
	for _, unit := range units {
		if strings.HasSuffix(upper, unit.suffix) {
			number = strings.TrimSpace(raw[:len(raw)-len(unit.suffix)])
			multiplier = unit.value
			break
		}
	}
	n, err := strconv.ParseUint(number, 10, 64)
	if err != nil || n == 0 || n > uint64(math.MaxInt64)/multiplier {
		return fmt.Errorf("invalid positive byte size %q", raw)
	}
	s.Bytes = int64(n * multiplier)
	return nil
}

type Config struct {
	GenomeBuild string    `toml:"genome_build"`
	Upstream    Upstream  `toml:"upstream"`
	Transfer    Transfer  `toml:"transfer"`
	Targets     Targets   `toml:"targets"`
	Local       Local     `toml:"local"`
	GCS         GCS       `toml:"gcs"`
	Retention   Retention `toml:"retention"`
	// Verify is retained only so early configuration files continue to load.
	// Explicit verification no longer has configuration or service scheduling.
	Verify   Verify   `toml:"verify"`
	Service  Service  `toml:"service"`
	Identity Identity `toml:"identity"`
	State    State    `toml:"state"`
}

type Upstream struct {
	BaseURLs    []string `toml:"base_urls"`
	ScoreList   string   `toml:"score_list"`
	MetadataCSV string   `toml:"metadata_csv"`
}

type Transfer struct {
	Concurrency     int      `toml:"concurrency"`
	FileConcurrency int      `toml:"file_concurrency"`
	MaxFileSize     ByteSize `toml:"max_file_size"`
	RequestTimeout  Duration `toml:"request_timeout"`
	MaxAttempts     int      `toml:"max_attempts"`
	InitialBackoff  Duration `toml:"initial_backoff"`
	MaxBackoff      Duration `toml:"max_backoff"`
	LeaseDuration   Duration `toml:"lease_duration"`
	SidecarLimit    int      `toml:"sidecar_limit"`
}

type Targets struct {
	Local bool `toml:"local"`
	GCS   bool `toml:"gcs"`
}

type Local struct {
	Root string `toml:"root"`
}

type GCS struct {
	Bucket         string `toml:"bucket"`
	Prefix         string `toml:"prefix"`
	BillingProject string `toml:"billing_project"`
}

type Retention struct {
	MissingGrace Duration `toml:"missing_grace"`
	KeepReleases int      `toml:"keep_releases"`
}

type Verify struct {
	// Deprecated: accepted but ignored.
	DefaultSample int `toml:"default_sample"`
	// Deprecated: accepted but ignored.
	Schedule string `toml:"schedule"`
}

type Service struct {
	UpdateInterval    Duration `toml:"update_interval"`
	ReconcileInterval Duration `toml:"reconcile_interval"`
	// Deprecated: accepted but ignored.
	VerifyInterval Duration `toml:"verify_interval"`
	ErrorBackoff   Duration `toml:"error_backoff"`
}

type Identity struct {
	UserAgent string `toml:"user_agent"`
	Contact   string `toml:"contact"`
}

type State struct {
	Path             string   `toml:"path"`
	WorkDir          string   `toml:"work_dir"`
	CheckpointMaxAge Duration `toml:"checkpoint_max_age"`
}

func Defaults() Config {
	return Config{
		GenomeBuild: "GRCh38",
		Upstream: Upstream{
			BaseURLs:    []string{defaultUpstream, strings.Replace(defaultUpstream, "https://", "http://", 1)},
			ScoreList:   "pgs_scores_list.txt",
			MetadataCSV: "metadata/pgs_all_metadata_scores.csv",
		},
		Transfer: Transfer{
			Concurrency:     4,
			FileConcurrency: 2,
			MaxFileSize:     ByteSize{10 << 30},
			RequestTimeout:  Duration{2 * time.Minute},
			MaxAttempts:     5,
			InitialBackoff:  Duration{time.Second},
			MaxBackoff:      Duration{30 * time.Second},
			LeaseDuration:   Duration{15 * time.Minute},
		},
		Targets:   Targets{Local: true},
		Local:     Local{Root: "./mirror"},
		Retention: Retention{MissingGrace: Duration{30 * 24 * time.Hour}, KeepReleases: 10},
		Service: Service{
			UpdateInterval:    Duration{6 * time.Hour},
			ReconcileInterval: Duration{7 * 24 * time.Hour},
			ErrorBackoff:      Duration{5 * time.Minute},
		},
		Identity: Identity{UserAgent: "pgsc-mirror/dev"},
		State:    State{CheckpointMaxAge: Duration{24 * time.Hour}},
	}
}

func Load(path string) (Config, error) {
	c := Defaults()
	if path == "" {
		return c, errors.New("a configuration file is required; use --config")
	}
	metadata, err := toml.DecodeFile(path, &c)
	if err != nil {
		return c, fmt.Errorf("load config %q: %w", path, err)
	}
	if unknown := metadata.Undecoded(); len(unknown) > 0 {
		return c, fmt.Errorf("load config %q: unknown key %q", path, unknown[0].String())
	}
	base := filepath.Dir(path)
	if c.Local.Root != "" && !filepath.IsAbs(c.Local.Root) {
		c.Local.Root = filepath.Clean(filepath.Join(base, c.Local.Root))
	}
	if c.State.Path == "" && c.Local.Root != "" {
		c.State.Path = filepath.Join(c.Local.Root, ".pgsc-mirror", "state.db")
	} else if c.State.Path != "" && !filepath.IsAbs(c.State.Path) {
		c.State.Path = filepath.Clean(filepath.Join(base, c.State.Path))
	}
	if c.State.WorkDir != "" && !filepath.IsAbs(c.State.WorkDir) {
		c.State.WorkDir = filepath.Clean(filepath.Join(base, c.State.WorkDir))
	}
	c = c.WithRuntimeDefaults()
	if c.Identity.Contact != "" && !strings.Contains(c.Identity.UserAgent, c.Identity.Contact) {
		c.Identity.UserAgent += " (" + c.Identity.Contact + ")"
	}
	return c, c.Validate()
}

// WithRuntimeDefaults derives operational paths without embedding any
// deployment-specific location in the application.
func (c Config) WithRuntimeDefaults() Config {
	if c.State.WorkDir == "" && c.State.Path != "" {
		c.State.WorkDir = filepath.Join(filepath.Dir(c.State.Path), "work")
	}
	if c.State.CheckpointMaxAge.Duration == 0 {
		c.State.CheckpointMaxAge = Duration{24 * time.Hour}
	}
	return c
}

func (c Config) Validate() error {
	if c.GenomeBuild != "GRCh38" {
		return fmt.Errorf("unsupported genome_build %q (only GRCh38 is currently supported)", c.GenomeBuild)
	}
	if len(c.Upstream.BaseURLs) == 0 {
		return errors.New("upstream.base_urls must not be empty")
	}
	for _, raw := range c.Upstream.BaseURLs {
		if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
			return fmt.Errorf("upstream URL %q must use http or https", raw)
		}
	}
	if c.Transfer.Concurrency < 1 || c.Transfer.Concurrency > 64 {
		return errors.New("transfer.concurrency must be between 1 and 64")
	}
	if c.Transfer.FileConcurrency < 1 || c.Transfer.FileConcurrency > 64 {
		return errors.New("transfer.file_concurrency must be between 1 and 64")
	}
	if c.Transfer.MaxFileSize.Bytes <= 0 {
		return errors.New("transfer.max_file_size must be positive")
	}
	if c.Transfer.MaxAttempts < 1 {
		return errors.New("transfer.max_attempts must be positive")
	}
	if c.Transfer.RequestTimeout.Duration <= 0 || c.Transfer.LeaseDuration.Duration <= 0 {
		return errors.New("transfer request_timeout and lease_duration must be positive")
	}
	if c.Transfer.InitialBackoff.Duration < 0 || c.Transfer.MaxBackoff.Duration < c.Transfer.InitialBackoff.Duration {
		return errors.New("transfer backoff durations are invalid")
	}
	if c.Transfer.SidecarLimit < 0 {
		return errors.New("transfer.sidecar_limit must not be negative")
	}
	if !c.Targets.Local && !c.Targets.GCS {
		return errors.New("at least one target must be enabled")
	}
	if c.Targets.Local && c.Local.Root == "" {
		return errors.New("local.root is required when the local target is enabled")
	}
	if c.Targets.GCS && c.GCS.Bucket == "" {
		return errors.New("gcs.bucket is required when the GCS target is enabled")
	}
	if c.State.Path == "" {
		return errors.New("state.path is required")
	}
	if c.State.WorkDir == "" {
		return errors.New("state.work_dir is required")
	}
	if c.State.CheckpointMaxAge.Duration <= 0 {
		return errors.New("state.checkpoint_max_age must be positive")
	}
	if strings.TrimSpace(c.Identity.UserAgent) == "" {
		return errors.New("identity.user_agent is required")
	}
	if c.Retention.KeepReleases < 1 || c.Retention.MissingGrace.Duration < 0 {
		return errors.New("retention.keep_releases must be positive and missing_grace must not be negative")
	}
	if c.Service.UpdateInterval.Duration <= 0 || c.Service.ReconcileInterval.Duration <= 0 || c.Service.ErrorBackoff.Duration <= 0 {
		return errors.New("service intervals and error_backoff must be positive")
	}
	return nil
}

func (c Config) Prepare() error {
	if c.Targets.Local {
		if err := os.MkdirAll(c.Local.Root, 0o755); err != nil {
			return fmt.Errorf("create local root: %w", err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(c.State.Path), 0o755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	if err := os.MkdirAll(c.State.WorkDir, 0o755); err != nil {
		return fmt.Errorf("create work directory: %w", err)
	}
	return nil
}
