package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 本机模板可以带一份快照:一台从模板启动好的虚机在暂停那一刻的内存和磁盘。建 sandbox 时
// 从快照恢复,不走内核启动。内存文件以写时复制的方式映射进每台虚机,没被改写的页由同一模板
// 的所有虚机共用宿主机的页缓存。
//
// 恢复出来的虚机和模板虚机一模一样,所以恢复后要让代理改三样东西:网卡地址(每台虚机一个网段)、
// 时钟(停在拍快照那一刻)、随机数种子(否则每台虚机产生同一串随机数)。
//
// ponytail: 快照里的 CPU 和内存规格是固定的,要别的规格的 sandbox 走普通启动。
// ponytail: 拍快照前已经在跑的进程会带着旧的随机数状态和旧的时间,预热命令要挑无状态的。
const (
	snapshotDir  = "snap"
	snapshotDisk = "snap.img"
)

// guestReset 是恢复后要代理改的东西。
type guestReset struct {
	IP       string `json:"ip"`
	Gateway  string `json:"gateway"`
	UnixNano int64  `json:"unix_nano"`
	Seed     []byte `json:"seed"`
}

// SnapshotTemplate 给 tplDir 里的本机模板拍快照:启动一台虚机,执行 prewarm(可为空),暂停,
// 把内存存进 tplDir/snap,把那一刻的磁盘存成 tplDir/snap.img。
func (m *MicroVM) SnapshotTemplate(ctx context.Context, tplDir string, prewarm []string) error {
	id := "snapshot-" + strconv.Itoa(os.Getpid())
	spec := &CreateSpec{ID: id, Image: filepath.Base(tplDir), LocalImage: blockDevicePath(tplDir)}
	if stepErr := m.Create(ctx, spec); stepErr != nil {
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
	// 虚机还停着,磁盘就是拍快照那一刻的样子。
	disk := filepath.Join(tplDir, snapshotDisk)
	if stepErr := copyImage(ctx, blockDevicePath(filepath.Join(m.dir(id), "root")), disk); stepErr != nil {
		return stepErr
	}
	_ = os.RemoveAll(filepath.Join(tplDir, snapshotDir))
	return os.Rename(tmp, filepath.Join(tplDir, snapshotDir))
}

// snapshotFor 返回能用来建这个规格的 sandbox 的快照目录;没有快照或规格对不上时返回空。
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

// restore 从快照恢复出 st 这台虚机。磁盘已经由调用方复制到位。
func (m *MicroVM) restore(ctx context.Context, st *vmState, snap string) error {
	dir := m.dir(st.ID)
	st.Block = true
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

	// 快照的配置里是模板虚机的路径和网卡,换成这台虚机的;内存和设备状态原样引用。
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
	disk["path"] = blockDevicePath(filepath.Join(dir, "root"))
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

// copyImage 复制一个镜像文件。文件系统支持 reflink(XFS、btrfs)时不复制数据;否则是一次稀疏的整份复制。
func copyImage(ctx context.Context, src, dst string) error {
	out, err := exec.CommandContext(ctx, "cp", "--reflink=auto", "--sparse=always", src, dst).CombinedOutput() //nolint:gosec // 路径由本包和调用方构造
	if err != nil {
		return fmt.Errorf("复制镜像: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func readJSON(p string) (map[string]any, error) {
	raw, err := os.ReadFile(p) //nolint:gosec // 路径由本包构造
	if err != nil {
		return nil, err
	}
	var v map[string]any
	err = json.Unmarshal(raw, &v)
	return v, err
}

// child 取 JSON 对象里的子对象;没有时返回一个空对象,读写都不会 panic。
func child(v map[string]any, key string) map[string]any {
	if c, ok := v[key].(map[string]any); ok {
		return c
	}
	return map[string]any{}
}
