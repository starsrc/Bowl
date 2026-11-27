package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

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

func (m *MicroVM) setupNetwork(st *vmState) (netArg, ipArg string, err error) {
	if st.NetIndex == 0 {
		if st.NetIndex, err = m.freeNetIndex(); err != nil {
			return "", "", err
		}
	}
	if err = ensureHostNAT(); err != nil {
		return "", "", fmt.Errorf("配置宿主机转发: %w", err)
	}
	n := strconv.Itoa(st.NetIndex)
	hostIP := netPrefix + n + ".1"
	mac := fmt.Sprintf("52:54:00:30:%02x:02", st.NetIndex)
	netArg = "tap=" + tapPrefix + n + ",ip=" + hostIP + ",mask=255.255.255.0,mac=" + mac
	ipArg = "ip=" + guestIP(st.NetIndex) + "::" + hostIP + ":255.255.255.0::eth0:off"
	return netArg, ipArg, nil
}

func (m *MicroVM) freeNetIndex() (int, error) {
	used := map[int]bool{}
	ids, err := m.IDs()
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

func ensureHostNAT() error {
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o600); err != nil {
		return err
	}
	tap := tapPrefix + "+"
	established := []string{"-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"}
	rules := []struct {
		table, chain string
		spec         []string
	}{
		{"nat", "POSTROUTING", []string{"-s", natSubnet, "!", "-o", tap, "-j", "MASQUERADE"}},
		{"filter", "FORWARD", append([]string{"-o", tap}, established...)},
		{"filter", "FORWARD", []string{"-i", tap, "-d", "10.0.0.0/8", "-j", "DROP"}},
		{"filter", "FORWARD", []string{"-i", tap, "-d", "172.16.0.0/12", "-j", "DROP"}},
		{"filter", "FORWARD", []string{"-i", tap, "-d", "192.168.0.0/16", "-j", "DROP"}},
		{"filter", "FORWARD", []string{"-i", tap, "-d", "169.254.0.0/16", "-j", "DROP"}},
		{"filter", "FORWARD", []string{"-i", tap, "-j", "ACCEPT"}},
		{"filter", "INPUT", append([]string{"-i", tap}, established...)},
		{"filter", "INPUT", []string{"-i", tap, "-j", "DROP"}},
	}
	for _, r := range slices.Backward(rules) {
		check := append([]string{"-t", r.table, "-C", r.chain}, r.spec...)
		if exec.Command("iptables", check...).Run() == nil {
			continue
		}
		insert := append([]string{"-t", r.table, "-I", r.chain, "1"}, r.spec...)
		if out, err := exec.Command("iptables", insert...).CombinedOutput(); err != nil {
			return fmt.Errorf("iptables %s: %w: %s", strings.Join(insert, " "), err, strings.TrimSpace(string(out)))
		}
	}
	return nil
}

func (m *MicroVM) Pause(ctx context.Context, id string) error {
	return m.setPaused(ctx, id, true)
}

func (m *MicroVM) Resume(ctx context.Context, id string) error {
	return m.setPaused(ctx, id, false)
}

func (m *MicroVM) setPaused(ctx context.Context, id string, paused bool) error {
	st, err := m.loadState(id)
	if err != nil {
		return err
	}
	action := "vm.resume"
	if paused {
		action = "vm.pause"
		m.guestSync(ctx, id)
	}
	if err := m.vmAPI(ctx, id, action, nil); err != nil {
		return err
	}
	st.Paused = paused
	return m.saveState(&st)
}

func (m *MicroVM) vmAPI(ctx context.Context, id, action string, body []byte) error {
	sock := filepath.Join(m.dir(id), "api.sock")
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost/api/v1/"+action, bytes.NewReader(body))
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
