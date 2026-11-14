//go:build linux

package fusefs_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestBuildMountOptions_LinuxDirectMountFlags(t *testing.T) {
	rwOpts := fusefs.BuildMountOptions(fusefs.Config{DiskID: "disk-rw", ReadOnly: false})
	roOpts := fusefs.BuildMountOptions(fusefs.Config{DiskID: "disk-ro", ReadOnly: true})

	require.True(t, rwOpts.DirectMount)
	require.True(t, roOpts.DirectMount)
	require.Equal(t, uintptr(unix.MS_NOSUID|unix.MS_NODEV), rwOpts.DirectMountFlags)
	require.Equal(t, uintptr(unix.MS_NOSUID|unix.MS_NODEV|unix.MS_RDONLY), roOpts.DirectMountFlags)
}
