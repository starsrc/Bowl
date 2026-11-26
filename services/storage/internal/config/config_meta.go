package config

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	DefaultAtimeMode = "noatime"

	DefaultEntryTimeoutSeconds = 1.0

	DefaultAttrTimeoutSeconds = 1.0

	DefaultDirEntryTimeoutSeconds = 1.0

	DefaultOpenKeepCache = false

	DefaultOpenDirCache = false

	DefaultDiskIdleTTL = 5 * time.Minute

	DefaultGCInterval = 30 * time.Second

	DefaultDataDir = "/tmp/bowl-storage-data"
)

type MetaConfig struct {
	AtimeModeVal              string        `env:"STORAGE_ATIME_MODE" envDefault:"noatime"`
	EntryTimeoutSecondsVal    float64       `env:"STORAGE_ENTRY_TIMEOUT_SECONDS" envDefault:"1.0"`
	AttrTimeoutSecondsVal     float64       `env:"STORAGE_ATTR_TIMEOUT_SECONDS" envDefault:"1.0"`
	DirEntryTimeoutSecondsVal float64       `env:"STORAGE_DIR_ENTRY_TIMEOUT_SECONDS" envDefault:"1.0"`
	OpenKeepCacheVal          bool          `env:"STORAGE_OPEN_KEEP_CACHE" envDefault:"false"`
	OpenDirCacheVal           bool          `env:"STORAGE_OPEN_DIR_CACHE" envDefault:"false"`
	DiskIdleTTLVal            time.Duration `env:"STORAGE_DISK_IDLE_TTL" envDefault:"5m"`
	GCIntervalVal             time.Duration `env:"STORAGE_GC_INTERVAL" envDefault:"30s"`
	DataDirVal                string        `env:"DATA_DIR"`
	StorageDataDirVal         string        `env:"STORAGE_DATA_DIR"`
}

func (c *MetaConfig) AtimeMode() string {
	if c.AtimeModeVal == "" {
		return DefaultAtimeMode
	}
	return strings.ToLower(strings.TrimSpace(c.AtimeModeVal))
}

func (c *MetaConfig) EntryTimeoutSeconds() float64 {
	return c.EntryTimeoutSecondsVal
}

func (c *MetaConfig) AttrTimeoutSeconds() float64 {
	return c.AttrTimeoutSecondsVal
}

func (c *MetaConfig) DirEntryTimeoutSeconds() float64 {
	return c.DirEntryTimeoutSecondsVal
}

func (c *MetaConfig) OpenKeepCache() bool {
	return c.OpenKeepCacheVal
}

func (c *MetaConfig) OpenDirCache() bool {
	return c.OpenDirCacheVal
}

func (c *MetaConfig) DiskIdleTTL() time.Duration {
	return c.DiskIdleTTLVal
}

func (c *MetaConfig) GCInterval() time.Duration {
	return c.GCIntervalVal
}

func (c *MetaConfig) DataDir() string {
	if strings.TrimSpace(c.DataDirVal) != "" {
		return c.DataDirVal
	}
	if strings.TrimSpace(c.StorageDataDirVal) != "" {
		return c.StorageDataDirVal
	}
	if c.DataDirVal != "" || c.StorageDataDirVal != "" {
		if c.DataDirVal != "" {
			return c.DataDirVal
		}
		return c.StorageDataDirVal
	}
	return DefaultDataDir
}

func (c *MetaConfig) Validate() error {
	if strings.TrimSpace(c.DataDir()) == "" {
		return fmt.Errorf("invalid data dir %q: must not be empty", c.DataDir())
	}

	mode := strings.ToLower(strings.TrimSpace(c.AtimeModeVal))
	switch mode {
	case "noatime", "relatime", "strictatime":
	default:
		return fmt.Errorf("invalid atime mode %q: must be one of 'noatime', 'relatime', 'strictatime'", c.AtimeModeVal)
	}

	if err := validateNonNegativeTimeout("entry timeout", c.EntryTimeoutSecondsVal); err != nil {
		return err
	}
	if err := validateNonNegativeTimeout("attr timeout", c.AttrTimeoutSecondsVal); err != nil {
		return err
	}
	if err := validateNonNegativeTimeout("dir entry timeout", c.DirEntryTimeoutSecondsVal); err != nil {
		return err
	}
	if c.DiskIdleTTLVal < 0 {
		return fmt.Errorf("invalid disk idle ttl %s: must be non-negative", c.DiskIdleTTLVal)
	}
	if c.GCIntervalVal < 0 {
		return fmt.Errorf("invalid gc interval %s: must be non-negative", c.GCIntervalVal)
	}

	return nil
}

func validateNonNegativeTimeout(field string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return fmt.Errorf("invalid %s %v: must be finite and non-negative", field, v)
	}
	return nil
}
