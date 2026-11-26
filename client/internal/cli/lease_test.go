package cli

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLease_GivenValidationErrors_ReturnsExit2(t *testing.T) {
	t.Setenv(EnvDiskID, "")

	requireCLIError2(t, "bowl lease [command]", "lease")
	requireCLIError2(t, "disk ID is required", "lease", "list")
	requireCLIError2(t, "cannot be used together", "lease", "list", "-d", testDiskID1, "-q", "--json")
	requireCLIError2(t, "invalid mount-id", "lease", "evict", "-d", testDiskID1, "--mount-id", "bad")
	requireCLIError2(t, "invalid mount-id", "lease", "evict", "-d", testDiskID1, "--mount-id", testNilUUID)
	requireCLIError2(t, "--branch cannot be empty", "lease", "list", "-d", testDiskID1, "--branch", "")
}

func TestLease_GivenListAndEvict_FiltersByBranchAndReportsUpdatedEpochs(t *testing.T) {
	rt := defaultCLIRuntime()
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "lease-disk", 1<<30, 0)
	storage.seedBranch(testDiskID1, "feat", 1, 0)
	exp := time.Date(2026, 11, 20, 12, 0, 30, 0, time.UTC)
	const (
		mountMain = "01950000-0000-7000-9000-000000000001"
		mountFeat = "01950000-0000-7000-9000-000000000002"
	)
	storage.seedMountLease(testDiskID1, "main", mountMain, false, 2, exp)
	storage.seedMountLease(testDiskID1, "feat", mountFeat, true, 0, exp)
	setDiskStorageFake(rt, storage)

	code, qOut, errOut := runCLIWithRuntime(rt, "lease", "ls", "-d", testDiskID1, "-q")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Equal(t, mountMain+"\n"+mountFeat+"\n", qOut)

	code, filteredOut, errOut := runCLIWithRuntime(rt, "lease", "list", "-d", testDiskID1, "--branch", "feat", "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var listRes leaseListJSON
	require.NoError(t, json.Unmarshal([]byte(filteredOut), &listRes))
	require.Equal(t, "feat", listRes.Branch)
	require.Len(t, listRes.Mounts, 1)
	require.Equal(t, mountFeat, listRes.Mounts[0].MountID)
	require.Equal(t, "ro", listRes.Mounts[0].Mode)

	code, evictOut, errOut := runCLIWithRuntime(rt, "lease", "evict", "-d", testDiskID1, "--branch", "main")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	require.Contains(t, evictOut, "Evicted 1 mount lease(s)")
	require.Contains(t, evictOut, "Updated branch writer epochs:")
	require.Contains(t, evictOut, "main")

	code, evictJSONOut, errOut := runCLIWithRuntime(rt, "lease", "evict", "-d", testDiskID1, "--mount-id", mountFeat, "--json")
	require.Equal(t, 0, code, errOut)
	require.Empty(t, errOut)
	var evictRes leaseEvictJSON
	require.NoError(t, json.Unmarshal([]byte(evictJSONOut), &evictRes))
	require.Equal(t, uint32(1), evictRes.EvictedCount)
	require.Equal(t, mountFeat, evictRes.MountID)
}
