package fusefs

import (
	"time"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func NewRawFSWithClockForTest(client storagev1connect.StorageServiceClient, cfg Config, nowFn func() time.Time) *RawFS {
	return newRawFS(client, cfg, nowFn)
}
