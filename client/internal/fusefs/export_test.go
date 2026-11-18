package fusefs

import (
	"time"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// NewRawFSWithClockForTest constructs a RawFS with an injected clock for deterministic tests.
//
//nolint:gocritic // Mirrors NewRawFS value-passed Config signature without altering production API.
func NewRawFSWithClockForTest(client storagev1connect.StorageServiceClient, cfg Config, nowFn func() time.Time) *RawFS {
	return newRawFS(client, cfg, nowFn)
}
