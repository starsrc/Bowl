package sandbox

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotForMatchesSpec(t *testing.T) {
	tpl := t.TempDir()
	m := &MicroVM{}
	if got := m.snapshotFor(tpl, 1, 1024); got != "" {
		t.Fatalf("没有快照时应返回空,得到 %q", got)
	}
	snap := filepath.Join(tpl, snapshotDir)
	if err := os.MkdirAll(snap, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := `{"cpus":{"boot_vcpus":1},"memory":{"size":1073741824}}`
	if err := os.WriteFile(filepath.Join(snap, "config.json"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := m.snapshotFor(tpl, 1, 1024); got != "" {
		t.Fatalf("缺磁盘时应返回空,得到 %q", got)
	}
	if err := os.WriteFile(filepath.Join(tpl, snapshotDisk), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := m.snapshotFor(tpl, 1, 1024); got != snap {
		t.Fatalf("规格一致时应返回 %q,得到 %q", snap, got)
	}
	if got := m.snapshotFor(tpl, 2, 1024); got != "" {
		t.Fatalf("CPU 数不一致时应返回空,得到 %q", got)
	}
	if got := m.snapshotFor(tpl, 1, 2048); got != "" {
		t.Fatalf("内存不一致时应返回空,得到 %q", got)
	}
}
