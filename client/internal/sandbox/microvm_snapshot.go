package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	snapshotDir  = "snap"
	snapshotDisk = "snap.img"
)

type guestReset struct {
	IP       string `json:"ip"`
	Gateway  string `json:"gateway"`
	UnixNano int64  `json:"unix_nano"`
	Seed     []byte `json:"seed"`
}

func (m *MicroVM) SnapshotTemplate(ctx context.Context, name string, prewarm []string) error {
	tplDir, err := m.templateDir(name)
	if err != nil {
		return err
	}
	id := "snapshot-" + strconv.Itoa(os.Getpid())
	if stepErr := m.Create(ctx, &CreateSpec{ID: id, Template: name}); stepErr != nil {
		return stepErr
	}
	defer func() { _ = m.Remove(context.WithoutCancel(ctx), id) }()
	if len(prewarm) > 0 {
		if code, err := m.Exec(ctx, id, "/", prewarm, ExecOptions{}, nil, io.Discard, io.Discard); err != nil || code != 0 {
			return fmt.Errorf("预热命令失败(退出码 %d): %w", code, err)
		}
	}
	if stepErr := m.Pause(ctx, id); stepErr != nil {
		return stepErr
	}
	tmp := filepath.Join(tplDir, snapshotDir+".tmp")
	_ = os.RemoveAll(tmp)
	if stepErr := os.MkdirAll(tmp, 0o700); stepErr != nil {
		return stepErr
	}
	body, _ := json.Marshal(map[string]string{"destination_url": "file://" + tmp})
	if stepErr := m.vmAPI(ctx, id, "vm.snapshot", body); stepErr != nil {
		return stepErr
	}
	disk := filepath.Join(tplDir, snapshotDisk)
	if stepErr := copyImage(ctx, m.systemDisk(id), disk); stepErr != nil {
		return stepErr
	}
	_ = os.RemoveAll(filepath.Join(tplDir, snapshotDir))
	return os.Rename(tmp, filepath.Join(tplDir, snapshotDir))
}

func (m *MicroVM) snapshotFor(tplDir string, cpus, memoryMiB int) string {
	dir := filepath.Join(tplDir, snapshotDir)
	cfg, err := readJSON(filepath.Join(dir, "config.json"))
	if err != nil || !isRegularFile(filepath.Join(tplDir, snapshotDisk)) {
		return ""
	}
	gotCPUs, _ := child(cfg, "cpus")["boot_vcpus"].(float64)
	gotMem, _ := child(cfg, "memory")["size"].(float64)
	if int(gotCPUs) != cpus || int64(gotMem) != int64(memoryMiB)<<20 {
		return ""
	}
	return dir
}

func (m *MicroVM) restore(ctx context.Context, st *vmState, snap string) error {
	dir := m.dir(st.ID)
	var err error
	if st.NetIndex, err = m.freeNetIndex(); err != nil {
		return err
	}
	if stepErr := m.saveState(st); stepErr != nil {
		return stepErr
	}
	if err = ensureHostNAT(); err != nil {
		return fmt.Errorf("配置宿主机转发: %w", err)
	}
	n := strconv.Itoa(st.NetIndex)
	hostIP := netPrefix + n + ".1"

	cfg, err := readJSON(filepath.Join(snap, "config.json"))
	if err != nil {
		return fmt.Errorf("读快照配置: %w", err)
	}
	disks, _ := cfg["disks"].([]any)
	nets, _ := cfg["net"].([]any)
	if len(disks) != 1 || len(nets) != 1 {
		return errors.New("快照配置里不是一块盘加一块网卡")
	}
	disk, _ := disks[0].(map[string]any)
	nic, _ := nets[0].(map[string]any)
	if disk == nil || nic == nil {
		return errors.New("快照配置的格式不认识")
	}
	disk["path"] = m.systemDisk(st.ID)
	nic["tap"], nic["ip"] = tapPrefix+n, hostIP
	child(cfg, "vsock")["socket"] = filepath.Join(dir, "vsock.sock")
	child(cfg, "serial")["file"] = filepath.Join(dir, "console.log")
	mine := filepath.Join(dir, snapshotDir)
	if stepErr := os.MkdirAll(mine, 0o700); stepErr != nil {
		return stepErr
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if stepErr := os.WriteFile(filepath.Join(mine, "config.json"), raw, 0o600); stepErr != nil {
		return stepErr
	}
	for _, f := range []string{"state.json", "memory-ranges"} {
		if stepErr := os.Symlink(filepath.Join(snap, f), filepath.Join(mine, f)); stepErr != nil {
			return stepErr
		}
	}

	vm, err := startDetached(filepath.Join(dir, "vm.log"), m.cloudHypervisor(),
		"--api-socket", filepath.Join(dir, "api.sock"),
		"--restore", "source_url=file://"+mine+",memory_restore_mode=copyonwrite,resume=true")
	if err != nil {
		return fmt.Errorf("启动虚机: %w", err)
	}
	st.VMPid = vm
	if stepErr := m.saveState(st); stepErr != nil {
		return stepErr
	}
	seed := make([]byte, 32)
	_, _ = rand.Read(seed)
	ready := func() bool {
		req := execRequest{Reset: &guestReset{IP: guestIP(st.NetIndex), Gateway: hostIP, UnixNano: time.Now().UnixNano(), Seed: seed}}
		code, runErr := m.run(ctx, st.ID, &req, nil, io.Discard, io.Discard)
		return runErr == nil && code == 0
	}
	if err = waitFor(ctx, readyTimeout, ready); err != nil {
		return fmt.Errorf("恢复出来的虚机没有就绪: %w\n%s", err, tailFile(filepath.Join(dir, "vm.log"), 15))
	}
	return nil
}

func readJSON(p string) (map[string]any, error) {
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var v map[string]any
	err = json.Unmarshal(raw, &v)
	return v, err
}

func child(v map[string]any, key string) map[string]any {
	if c, ok := v[key].(map[string]any); ok {
		return c
	}
	return map[string]any{}
}
