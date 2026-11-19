package sandbox

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 每台虚机一块 tap 网卡和一个 /24:宿主机这端是 172.30.<n>.1,虚机是 172.30.<n>.2。
// 出网走宿主机 NAT;宿主机直接访问虚机 IP 就能连到虚机里监听的端口。
//
// ponytail: 所有 sandbox 的 tap 都放行转发,sandbox 之间能互相访问;多租户上线前要按 tap 隔离。
// ponytail: 编号分配是扫状态目录,两个 create 同时跑可能拿到同一个编号;要并发建时加文件锁。
const (
	netPrefix  = "172.30."
	tapPrefix  = "bsbx"
	natSubnet  = "172.30.0.0/16"
	maxNetwork = 250
)

func guestIP(n int) string {
	if n <= 0 {
		return ""
	}
	return netPrefix + strconv.Itoa(n) + ".2"
}

// setupNetwork 分配编号、准备宿主机的转发和 NAT、写好虚机的 resolv.conf,
// 返回 cloud-hypervisor 的 --net 参数和内核命令行里的 ip= 参数。
func (m *MicroVM) setupNetwork(st *vmState, root string) (netArg, ipArg string, err error) {
	if st.NetIndex == 0 {
		if st.NetIndex, err = m.freeNetIndex(); err != nil {
			return "", "", err
		}
	}
	if err = ensureHostNAT(); err != nil {
		return "", "", fmt.Errorf("配置宿主机转发: %w", err)
	}
	dns := cmp.Or(m.DNS, "1.1.1.1")
	resolv := filepath.Join(root, "etc", "resolv.conf")
	// Ubuntu 镜像里这常是指向 systemd-resolved 的符号链接,虚机里没有它,换成普通文件。
	_ = os.Remove(resolv)
	if err = os.WriteFile(resolv, []byte("nameserver "+dns+"\n"), 0o644); err != nil { //nolint:gosec // resolv.conf 本来就是全员可读
		return "", "", fmt.Errorf("写 resolv.conf: %w", err)
	}
	n := strconv.Itoa(st.NetIndex)
	hostIP := netPrefix + n + ".1"
	mac := fmt.Sprintf("52:54:00:30:%02x:02", st.NetIndex)
	netArg = "tap=" + tapPrefix + n + ",ip=" + hostIP + ",mask=255.255.255.0,mac=" + mac
	// ip=<client>::<gateway>:<netmask>::<device>:<autoconf>,内核在挂根之前就配好网卡。
	ipArg = "ip=" + guestIP(st.NetIndex) + "::" + hostIP + ":255.255.255.0::eth0:off"
	return netArg, ipArg, nil
}

func (m *MicroVM) freeNetIndex() (int, error) {
	used := map[int]bool{}
	ids, err := m.IDs(context.Background())
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		if st, err := m.loadState(id); err == nil {
			used[st.NetIndex] = true
		}
	}
	for n := 1; n <= maxNetwork; n++ {
		if !used[n] {
			return n, nil
		}
	}
	return 0, fmt.Errorf("这台机器上已经有 %d 个 sandbox,没有空闲的网段", maxNetwork)
}

// ensureHostNAT 打开 IPv4 转发,并加上 sandbox 网段的 NAT 和转发规则。规则已在时不重复加。
// Docker 会把 FORWARD 链默认策略设成 DROP,所以转发要显式放行。
func ensureHostNAT() error {
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o600); err != nil {
		return err
	}
	rules := [][]string{
		{"-t", "nat", "POSTROUTING", "-s", natSubnet, "!", "-o", tapPrefix + "+", "-j", "MASQUERADE"},
		{"-t", "filter", "FORWARD", "-i", tapPrefix + "+", "-j", "ACCEPT"},
		{"-t", "filter", "FORWARD", "-o", tapPrefix + "+", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"},
	}
	for _, r := range rules {
		table, chain, spec := r[:2], r[2], r[3:]
		check := append(append(append([]string{}, table...), "-C", chain), spec...)
		if exec.Command("iptables", check...).Run() == nil { //nolint:gosec // 参数是本包写死的
			continue
		}
		insert := append(append(append([]string{}, table...), "-I", chain), spec...)
		if out, err := exec.Command("iptables", insert...).CombinedOutput(); err != nil { //nolint:gosec // 同上
			return fmt.Errorf("iptables %s: %w: %s", strings.Join(insert, " "), err, strings.TrimSpace(string(out)))
		}
	}
	return nil
}

// Pause 冻结虚机的 vCPU,内存留在宿主机上。
//
// ponytail: 暂停不释放内存。释放内存要做内存快照,而 M0 测出系统盘走 virtio-fs 时快照恢复不了,
// 要等 M8 换块设备。
func (m *MicroVM) Pause(ctx context.Context, id string) error {
	return m.setPaused(ctx, id, true)
}

// Resume 让暂停的虚机接着运行。
func (m *MicroVM) Resume(ctx context.Context, id string) error {
	return m.setPaused(ctx, id, false)
}

func (m *MicroVM) setPaused(ctx context.Context, id string, paused bool) error {
	id = StripID(id)
	st, err := m.loadState(id)
	if err != nil {
		return err
	}
	action := "vm.resume"
	if paused {
		action = "vm.pause"
	}
	if err := m.vmAPI(ctx, id, action); err != nil {
		return err
	}
	st.Paused = paused
	return m.saveState(&st)
}

// vmAPI 调 cloud-hypervisor 的 REST 接口(unix 套接字上的 HTTP),只用不带请求体的动作。
func (m *MicroVM) vmAPI(ctx context.Context, id, action string) error {
	sock := filepath.Join(m.dir(id), "api.sock")
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost/api/v1/"+action, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("调用虚机接口 %s: %w", action, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("虚机接口 %s 返回 %s: %s", action, resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}
