package config

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	// DefaultAtimeMode 默认访问时间更新策略（noatime）。
	DefaultAtimeMode = "noatime"
	// DefaultEntryTimeoutSeconds 默认非目录条目内核缓存时长（1.0秒）。
	DefaultEntryTimeoutSeconds = 1.0
	// DefaultAttrTimeoutSeconds 默认属性内核缓存时长（1.0秒）。
	DefaultAttrTimeoutSeconds = 1.0
	// DefaultDirEntryTimeoutSeconds 默认目录项内核缓存时长（1.0秒）。
	DefaultDirEntryTimeoutSeconds = 1.0
	// DefaultOpenKeepCache 默认打开文件是否保留内核页缓存。
	DefaultOpenKeepCache = false
	// DefaultOpenDirCache 默认打开目录是否保留内核目录缓存。
	DefaultOpenDirCache = false
	// DefaultDataDir 默认本地数据根目录。
	DefaultDataDir = "/tmp/bowl-storage-data"
)

// MetaConfig 承载元数据与 FUSE 缓存相关配置。
type MetaConfig struct {
	AtimeModeVal              string  `env:"STORAGE_ATIME_MODE" envDefault:"noatime"`
	EntryTimeoutSecondsVal    float64 `env:"STORAGE_ENTRY_TIMEOUT_SECONDS" envDefault:"1.0"`
	AttrTimeoutSecondsVal     float64 `env:"STORAGE_ATTR_TIMEOUT_SECONDS" envDefault:"1.0"`
	DirEntryTimeoutSecondsVal float64 `env:"STORAGE_DIR_ENTRY_TIMEOUT_SECONDS" envDefault:"1.0"`
	OpenKeepCacheVal          bool    `env:"STORAGE_OPEN_KEEP_CACHE" envDefault:"false"`
	OpenDirCacheVal           bool    `env:"STORAGE_OPEN_DIR_CACHE" envDefault:"false"`
	DataDirVal                string  `env:"DATA_DIR"`
	StorageDataDirVal         string  `env:"STORAGE_DATA_DIR"`
	// 交接（handoff）原型：两个存储节点指向同一个目录即视为共用一个远端；留空则关闭交接。
	HandoffRemoteDirVal string `env:"HANDOFF_REMOTE_DIR"`
	// 本节点在交接 HEAD 里的身份；留空取主机名。
	HandoffNodeIDVal string `env:"HANDOFF_NODE_ID"`
	// 后台同步周期；0 关闭后台同步。
	HandoffSyncIntervalVal time.Duration `env:"HANDOFF_SYNC_INTERVAL" envDefault:"30s"`
}

// AtimeMode 返回最后访问时间更新策略的小写标准化字符串（"noatime" / "relatime" / "strictatime"）。
func (c *MetaConfig) AtimeMode() string {
	if c.AtimeModeVal == "" {
		return DefaultAtimeMode
	}
	return strings.ToLower(strings.TrimSpace(c.AtimeModeVal))
}

// EntryTimeoutSeconds 返回非目录节点内核条目缓存时长（秒）。
func (c *MetaConfig) EntryTimeoutSeconds() float64 {
	return c.EntryTimeoutSecondsVal
}

// AttrTimeoutSeconds 返回节点内核属性缓存时长（秒）。
func (c *MetaConfig) AttrTimeoutSeconds() float64 {
	return c.AttrTimeoutSecondsVal
}

// DirEntryTimeoutSeconds 返回目录节点内核条目缓存时长（秒）。
func (c *MetaConfig) DirEntryTimeoutSeconds() float64 {
	return c.DirEntryTimeoutSecondsVal
}

// OpenKeepCache 返回打开文件时是否保留内核页缓存。
func (c *MetaConfig) OpenKeepCache() bool {
	return c.OpenKeepCacheVal
}

// OpenDirCache 返回打开目录时是否保留内核目录缓存。
func (c *MetaConfig) OpenDirCache() bool {
	return c.OpenDirCacheVal
}

// DataDir 返回本地数据根目录路径（优先读取 DATA_DIR，其次 STORAGE_DATA_DIR，默认 DefaultDataDir）。
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

// Validate 校验元数据配置合法性。
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

	return nil
}

func validateNonNegativeTimeout(field string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return fmt.Errorf("invalid %s %v: must be finite and non-negative", field, v)
	}
	return nil
}
