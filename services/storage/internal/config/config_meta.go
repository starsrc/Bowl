package config

import (
	"fmt"
	"strings"
	"time"
)

const (
	// DefaultSessionLeaseTTL 默认客户端会话租约时长（60秒）。
	DefaultSessionLeaseTTL = 60 * time.Second
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
)

// MetaConfig 承载元数据与 FUSE 缓存相关配置。
type MetaConfig struct {
	SessionLeaseTTLVal        time.Duration `env:"STORAGE_SESSION_LEASE_TTL" envDefault:"60s"`
	AtimeModeVal              string        `env:"STORAGE_ATIME_MODE" envDefault:"noatime"`
	EntryTimeoutSecondsVal    float64       `env:"STORAGE_ENTRY_TIMEOUT_SECONDS" envDefault:"1.0"`
	AttrTimeoutSecondsVal     float64       `env:"STORAGE_ATTR_TIMEOUT_SECONDS" envDefault:"1.0"`
	DirEntryTimeoutSecondsVal float64       `env:"STORAGE_DIR_ENTRY_TIMEOUT_SECONDS" envDefault:"1.0"`
	OpenKeepCacheVal          bool          `env:"STORAGE_OPEN_KEEP_CACHE" envDefault:"false"`
	OpenDirCacheVal           bool          `env:"STORAGE_OPEN_DIR_CACHE" envDefault:"false"`
}

// SessionLeaseTTL 返回客户端会话租约时长。
func (c MetaConfig) SessionLeaseTTL() time.Duration {
	return c.SessionLeaseTTLVal
}

// AtimeMode 返回最后访问时间更新策略的小写标准化字符串（"noatime" / "relatime" / "strictatime"）。
func (c MetaConfig) AtimeMode() string {
	if c.AtimeModeVal == "" {
		return DefaultAtimeMode
	}
	return strings.ToLower(strings.TrimSpace(c.AtimeModeVal))
}

// EntryTimeoutSeconds 返回非目录节点内核条目缓存时长（秒）。
func (c MetaConfig) EntryTimeoutSeconds() float64 {
	return c.EntryTimeoutSecondsVal
}

// AttrTimeoutSeconds 返回节点内核属性缓存时长（秒）。
func (c MetaConfig) AttrTimeoutSeconds() float64 {
	return c.AttrTimeoutSecondsVal
}

// DirEntryTimeoutSeconds 返回目录节点内核条目缓存时长（秒）。
func (c MetaConfig) DirEntryTimeoutSeconds() float64 {
	return c.DirEntryTimeoutSecondsVal
}

// OpenKeepCache 返回打开文件时是否保留内核页缓存。
func (c MetaConfig) OpenKeepCache() bool {
	return c.OpenKeepCacheVal
}

// OpenDirCache 返回打开目录时是否保留内核目录缓存。
func (c MetaConfig) OpenDirCache() bool {
	return c.OpenDirCacheVal
}

// Validate 校验元数据配置合法性。
func (c MetaConfig) Validate() error {
	if c.SessionLeaseTTLVal <= 0 {
		return fmt.Errorf("invalid session lease TTL %v: must be greater than 0", c.SessionLeaseTTLVal)
	}

	mode := strings.ToLower(strings.TrimSpace(c.AtimeModeVal))
	switch mode {
	case "noatime", "relatime", "strictatime":
	default:
		return fmt.Errorf("invalid atime mode %q: must be one of 'noatime', 'relatime', 'strictatime'", c.AtimeModeVal)
	}

	if c.EntryTimeoutSecondsVal < 0 {
		return fmt.Errorf("invalid entry timeout %v: must be non-negative", c.EntryTimeoutSecondsVal)
	}
	if c.AttrTimeoutSecondsVal < 0 {
		return fmt.Errorf("invalid attr timeout %v: must be non-negative", c.AttrTimeoutSecondsVal)
	}
	if c.DirEntryTimeoutSecondsVal < 0 {
		return fmt.Errorf("invalid dir entry timeout %v: must be non-negative", c.DirEntryTimeoutSecondsVal)
	}

	return nil
}
