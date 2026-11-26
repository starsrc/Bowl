package projection

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/starsrc/pkg/tx"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

func OpenPool(cfg config.DBPoolConfig) (*pgxpool.Pool, error) {
	pool, err := tx.OpenPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dbpool config: %w", err)
	}
	return pool, nil
}

func ProvideStore(pool *pgxpool.Pool) Store {
	return NewPGStore(pool)
}

type ControlManagerParams struct {
	Store   Store
	MetaCfg *config.MetaConfig
	MetaVal config.MetaConfig
	Logger  *slog.Logger
}

func ProvideControlManager(params ControlManagerParams) (*ControlManager, error) {
	cfg := params.MetaCfg
	if cfg == nil && params.MetaVal != (config.MetaConfig{}) {
		cfg = &params.MetaVal
	}
	storageRoot := config.DefaultDataDir
	actorIdleTTL := config.DefaultActorIdleTTL
	gcMode := GCModePeriodic
	gcInterval := config.DefaultGCInterval
	if cfg != nil {
		storageRoot = cfg.DataDir()
		actorIdleTTL = cfg.ActorIdleTTL()
		gcInterval = cfg.GCInterval()
		if gcInterval == 0 {
			gcMode = GCModeWakeOnly
		}
	}

	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:  storageRoot,
		Store:        params.Store,
		Logger:       params.Logger,
		ActorIdleTTL: actorIdleTTL,
		GCMode:       gcMode,
		GCInterval:   gcInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize control manager: %w", err)
	}

	return mgr, nil
}
