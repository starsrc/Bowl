package control

import (
	"cmp"
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability/obsfx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/handoff"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

// ManagerParams 定义控制面管理器的 Fx 依赖注入参数。
type ManagerParams struct {
	fx.In
	Lifecycle    fx.Lifecycle `optional:"true"`
	Store        db.Store
	MetaCfg      *config.MetaConfig      `optional:"true"`
	MetaVal      config.MetaConfig       `optional:"true"`
	Logger       *slog.Logger            `optional:"true"`
	StoreMetrics *store.Metrics          `optional:"true"`
	Collector    *storageEngineCollector `optional:"true"`
}

//nolint:gocritic // hugeParam: fx.In 依赖注入参数结构体必须按值传递。
func newManagerFromParams(params ManagerParams) Manager {
	cfg := params.MetaCfg
	if cfg == nil && params.MetaVal != (config.MetaConfig{}) {
		cfg = &params.MetaVal
	}
	opts := make([]Option, 0, 10)
	if cfg != nil {
		mode := AtimeModeNoAtime
		switch cfg.AtimeMode() {
		case "relatime":
			mode = AtimeModeRelAtime
		case "strictatime":
			mode = AtimeModeStrictAtime
		}
		opts = append(opts,
			WithStorageRoot(cfg.DataDir()),
			WithAtimeMode(mode),
			WithEntryTimeout(cfg.EntryTimeoutSeconds()),
			WithAttrTimeout(cfg.AttrTimeoutSeconds()),
			WithDirEntryTimeout(cfg.DirEntryTimeoutSeconds()),
			WithOpenKeepCache(cfg.OpenKeepCache()),
			WithOpenDirCache(cfg.OpenDirCache()),
		)
	}
	if params.Logger != nil {
		opts = append(opts, WithLogger(params.Logger))
	}
	if cfg != nil && (cfg.HandoffRemoteDirVal != "" || cfg.HandoffRemoteURLVal != "") {
		opts = append(opts,
			handoffOption(cfg, params.Logger),
			WithHandoffSync(cfg.HandoffSyncIntervalVal),
			WithHandoffGC(cfg.HandoffGCIntervalVal, cfg.HandoffGCKeepSnapshotsVal, cfg.HandoffGCMinAgeVal),
		)
	}
	if params.StoreMetrics != nil {
		opts = append(opts, WithStoreMetrics(params.StoreMetrics))
	}
	if params.Collector != nil {
		opts = append(opts, withEngineCollector(params.Collector))
	}

	m := newManager(params.Store, opts...)
	if params.Lifecycle != nil {
		params.Lifecycle.Append(fx.Hook{
			OnStop: func(context.Context) error {
				return m.Close()
			},
		})
	}
	return m
}

func provideStoreMetricsCollector(m *store.Metrics) prometheus.Collector {
	return m
}

func provideEngineCollector(c *storageEngineCollector) prometheus.Collector {
	return c
}

// Module 提供单盘控制面管理器依赖，并将共享 *store.Metrics 与 storageEngineCollector
// 通过 obsfx.AsCollector 注册进实例级隔离 Prometheus Registry。
var Module = fx.Module("control",
	fx.Provide(
		store.NewMetrics,
		newStorageEngineCollector,
		obsfx.AsCollector(provideStoreMetricsCollector),
		obsfx.AsCollector(provideEngineCollector),
		newManagerFromParams,
	),
)

// handoffOption 按配置启用交接；远端初始化失败只告警并保持交接关闭，不阻止服务启动。
func handoffOption(cfg *config.MetaConfig, logger *slog.Logger) Option {
	if logger == nil {
		logger = slog.Default()
	}
	nodeID := cfg.HandoffNodeIDVal
	if nodeID == "" {
		nodeID, _ = os.Hostname()
	}
	remote, where, err := newHandoffRemote(cfg)
	if err != nil {
		logger.Warn("handoff disabled: remote init failed", "remote", where, "err", err)
		return nil
	}
	logger.Info("handoff enabled", "remote", where, "node_id", nodeID)
	return WithHandoff(remote, nodeID)
}

// newHandoffRemote 按配置构造交接远端，where 是用于日志的远端描述（不含凭证）。
func newHandoffRemote(cfg *config.MetaConfig) (remote handoff.Remote, where string, err error) {
	if cfg.HandoffRemoteURLVal == "" {
		remote, err = handoff.NewDirRemote(cfg.HandoffRemoteDirVal)
		return remote, cfg.HandoffRemoteDirVal, err
	}
	s3cfg, err := handoff.ParseS3URL(cfg.HandoffRemoteURLVal)
	if err != nil {
		return nil, cfg.HandoffRemoteURLVal, err
	}
	s3cfg.AccessKey = cmp.Or(cfg.HandoffS3AccessKeyVal, os.Getenv("AWS_ACCESS_KEY_ID"))
	s3cfg.SecretKey = cmp.Or(cfg.HandoffS3SecretKeyVal, os.Getenv("AWS_SECRET_ACCESS_KEY"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	remote, err = handoff.NewS3Remote(ctx, &s3cfg)
	return remote, cfg.HandoffRemoteURLVal, err
}
