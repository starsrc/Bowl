package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMountMeta_GivenZombieOrReusedPIDProcStat_TreatsProcessAsDeadAndPopulatesStartTicks(t *testing.T) {
	t.Run("ParseProcStatHandlesSpacesAndParensInComm", func(t *testing.T) {
		raw := "12345 (bowl mount (v2)) Z 1 12345 12345 0 -1 4194304 10 0 0 0 5 2 0 0 20 0 4 0 987654321 100 50\n"
		state, startTicks, err := parseProcStat(raw)
		require.NoError(t, err)
		require.Equal(t, byte('Z'), state)
		require.Equal(t, uint64(987654321), startTicks)

		for _, bad := range []string{
			"12345 no-parens S 1 2 3",
			"12345 (short) S 1 2 3",
			"12345 (bad-ticks) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 notanumber",
		} {
			_, _, parseErr := parseProcStat(bad)
			require.Error(t, parseErr, "scene=malformed_proc_stat raw=%q", bad)
		}
	})

	t.Run("ZombieAndDeadStatesTreatedAsDead", func(t *testing.T) {
		for _, deadState := range []byte{'Z', 'X', 'x'} {
			t.Run(string(deadState), func(t *testing.T) {
				rt := defaultCLIRuntime()
				mp := "/mnt/bowl-zombie"
				metaDir := t.TempDir()
				setTempMountMetaDirs(rt, metaDir)
				rt.readProcStatFn = func(int) (byte, uint64, error) {
					return deadState, 4242, nil
				}

				meta := mountMeta{
					Mountpoint:     mp,
					PID:            os.Getpid(),
					ProcStartTicks: 4242,
					DiskID:         testDiskID1,
					AdminAddr:      "off",
					StartedAt:      time.Now().UTC(),
				}
				metaPath, err := rt.writeMountMeta(&meta)
				require.NoError(t, err)

				require.False(t, rt.processAlive(os.Getpid()), "scene=dead_state_%c want=processAlive_false", deadState)
				require.False(t, rt.mountMetaProcessAlive(meta), "scene=dead_state_%c want=mountMetaProcessAlive_false", deadState)
				require.Empty(t, rt.listMountMetas(), "scene=dead_state_%c want=excluded_from_listMountMetas", deadState)
				_, _, ok := rt.resolveUmountMountMeta(mp)
				require.False(t, ok, "scene=dead_state_%c want=resolveUmountMountMeta_false", deadState)
				require.True(t, rt.isMountMetaDrained(metaPath, &meta), "scene=dead_state_%c want=isMountMetaDrained_true", deadState)
			})
		}
	})

	t.Run("PIDReuseDetectedByStartTicksMismatch", func(t *testing.T) {
		rt := defaultCLIRuntime()
		mp := "/mnt/bowl-pid-reuse"
		metaDir := t.TempDir()
		setTempMountMetaDirs(rt, metaDir)
		rt.readProcStatFn = func(int) (byte, uint64, error) {
			return 'S', 999999, nil
		}

		reusedMeta := mountMeta{
			Mountpoint:     mp,
			PID:            os.Getpid(),
			ProcStartTicks: 111111,
			DiskID:         testDiskID1,
			AdminAddr:      "off",
			StartedAt:      time.Now().UTC(),
		}
		metaPath, err := rt.writeMountMeta(&reusedMeta)
		require.NoError(t, err)

		require.False(t, rt.mountMetaProcessAlive(reusedMeta), "scene=pid_reuse want=dead")
		require.Empty(t, rt.listMountMetas(), "scene=pid_reuse want=excluded_from_listMountMetas")
		_, _, ok := rt.resolveUmountMountMeta(mp)
		require.False(t, ok, "scene=pid_reuse want=resolveUmountMountMeta_false")
		require.True(t, rt.isMountMetaDrained(metaPath, &reusedMeta), "scene=pid_reuse want=isMountMetaDrained_true")

		matchingMeta := mountMeta{
			Mountpoint:     mp,
			PID:            os.Getpid(),
			ProcStartTicks: 999999,
			DiskID:         testDiskID1,
			AdminAddr:      "off",
			StartedAt:      time.Now().UTC(),
		}
		metaPath, err = rt.writeMountMeta(&matchingMeta)
		require.NoError(t, err)

		require.True(t, rt.mountMetaProcessAlive(matchingMeta), "scene=matching_ticks want=alive")
		require.Len(t, rt.listMountMetas(), 1, "scene=matching_ticks want=included_in_listMountMetas")
		gotPath, gotMeta, ok := rt.resolveUmountMountMeta(mp)
		require.True(t, ok, "scene=matching_ticks want=resolveUmountMountMeta_true")
		require.Equal(t, metaPath, gotPath)
		require.Equal(t, uint64(999999), gotMeta.ProcStartTicks)
		require.False(t, rt.isMountMetaDrained(metaPath, &matchingMeta), "scene=matching_ticks want=not_drained")
	})

	t.Run("LegacyZeroTicksAndStatErrorFallbacks", func(t *testing.T) {
		rt := defaultCLIRuntime()
		rt.readProcStatFn = func(int) (byte, uint64, error) {
			return 'S', 999999, nil
		}
		require.True(t, rt.mountMetaProcessAlive(mountMeta{PID: os.Getpid(), ProcStartTicks: 0}), "scene=legacy_zero_ticks want=alive")

		for _, fallbackErr := range []error{syscall.EPERM, syscall.EACCES, errProcStatUnsupported} {
			rt.readProcStatFn = func(int) (byte, uint64, error) {
				return 0, 0, fallbackErr
			}
			require.True(t, rt.processAlive(os.Getpid()), "scene=fallback_%v want=processAlive_true", fallbackErr)
			require.True(t, rt.mountMetaProcessAlive(mountMeta{PID: os.Getpid(), ProcStartTicks: 111111}), "scene=fallback_%v want=mountMetaProcessAlive_true", fallbackErr)
		}

		rt.readProcStatFn = func(int) (byte, uint64, error) {
			return 0, 0, os.ErrNotExist
		}
		require.False(t, rt.processAlive(os.Getpid()), "scene=enoent want=processAlive_false")
		require.False(t, rt.mountMetaProcessAlive(mountMeta{PID: os.Getpid(), ProcStartTicks: 111111}), "scene=enoent want=mountMetaProcessAlive_false")
	})

	t.Run("WriteMountMetaPopulatesStartTicksAndOmitsWhenUnavailable", func(t *testing.T) {
		rt := defaultCLIRuntime()
		mp := "/mnt/bowl-populate-ticks"
		metaDir := t.TempDir()
		setTempMountMetaDirs(rt, metaDir)
		rt.readProcStatFn = func(int) (byte, uint64, error) {
			return 'S', 555123, nil
		}

		meta := mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "off",
			StartedAt:  time.Now().UTC(),
		}
		metaPath, err := rt.writeMountMeta(&meta)
		require.NoError(t, err)
		require.Equal(t, uint64(555123), meta.ProcStartTicks)

		raw, readErr := os.ReadFile(metaPath)
		require.NoError(t, readErr)
		var stored mountMeta
		require.NoError(t, json.Unmarshal(raw, &stored))
		require.Equal(t, uint64(555123), stored.ProcStartTicks)
		require.Contains(t, string(raw), `"proc_start_ticks":555123`)

		rt.readProcStatFn = func(int) (byte, uint64, error) {
			return 0, 0, errProcStatUnsupported
		}
		metaNoProc := mountMeta{
			Mountpoint: filepath.Join(mp, "no-proc"),
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "off",
			StartedAt:  time.Now().UTC(),
		}
		noProcPath, err := rt.writeMountMeta(&metaNoProc)
		require.NoError(t, err)
		require.Zero(t, metaNoProc.ProcStartTicks)

		rawNoProc, readErr := os.ReadFile(noProcPath)
		require.NoError(t, readErr)
		require.NotContains(t, string(rawNoProc), "proc_start_ticks")
	})
}
