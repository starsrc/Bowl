package internal

import (
	_ "connectrpc.com/connect"
	_ "github.com/google/uuid"
	_ "github.com/hanwen/go-fuse/v2/fuse"
	_ "github.com/starsrc/bowl/gen/go/storage/v1"
	_ "github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	_ "github.com/stretchr/testify/require"
	_ "golang.org/x/net/http2"
	_ "golang.org/x/sys/unix"
)
