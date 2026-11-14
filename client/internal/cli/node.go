package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/sandbox"
)

// 节点（node）是“一处存储 + 跑在它旁边的算力”的本地登记：
//
//	endpoint          本机访问这个存储节点的地址
//	docker            沙箱跑在哪个 Docker 上（留空 = 本机 Docker）
//	sandbox_endpoint  沙箱内访问存储用的地址（留空 = 由 endpoint 推出；算力和存储同内网时填内网地址）
//
// 登记之后，凡是接受存储地址的地方都可以直接写节点名（--endpoint cloud、--to cloud），
// 沙箱会自动起在该节点对应的 Docker 上。没有登记任何节点时，一切行为与之前相同。
type sboxNode struct {
	Name            string `json:"name"`
	Endpoint        string `json:"endpoint"`
	Docker          string `json:"docker,omitempty"`
	SandboxEndpoint string `json:"sandbox_endpoint,omitempty"`
}

const nodeUsageHelp = `bowl node - 登记存储节点以及沙箱在该节点上使用的算力

用法:
  bowl node set <name> --endpoint <url> [--docker <host>] [--sandbox-endpoint <url>]
  bowl node list
  bowl node rm <name>

标志:
  --endpoint string           本机访问该存储节点的地址（必填）
  --docker string             沙箱跑在哪个 Docker 上：ssh://user@host、tcp://host:2376，或 docker context 名。
                              留空表示本机 Docker
  --sandbox-endpoint string   沙箱内访问存储用的地址。留空时使用 --endpoint（回环地址改写为 host.docker.internal）；
                              算力和存储在同一内网时填内网地址

登记后可以用节点名代替地址，例如:
  bowl sandbox create --endpoint local
  bowl sandbox move <sandbox-id> --to cloud

节点登记保存在 $BOWL_CONFIG_DIR/nodes.json（默认是系统的用户配置目录下的 bowl/）。
`

func nodesPath() (string, error) {
	dir := strings.TrimSpace(os.Getenv("BOWL_CONFIG_DIR"))
	if dir == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("无法定位配置目录: %w", err)
		}
		dir = filepath.Join(base, "bowl")
	}
	return filepath.Join(dir, "nodes.json"), nil
}

// loadNodes 读取节点登记；文件不存在视为没有登记。
func loadNodes() ([]sboxNode, error) {
	p, err := nodesPath()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var nodes []sboxNode
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, fmt.Errorf("节点登记文件 %s 格式错误: %w", p, err)
	}
	return nodes, nil
}

func saveNodes(nodes []sboxNode) error {
	p, err := nodesPath()
	if err != nil {
		return err
	}
	if mkErr := os.MkdirAll(filepath.Dir(p), 0o755); mkErr != nil {
		return mkErr
	}
	raw, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, append(raw, '\n'), 0o600)
}

// resolveEndpoint 把用户给的存储地址解析成规范的 URL：先按节点名查登记，再走通用的地址解析。
func resolveEndpoint(raw string) (string, error) {
	if name := strings.TrimSpace(raw); name != "" && !strings.Contains(name, "://") {
		nodes, err := loadNodes()
		if err != nil {
			return "", err
		}
		for i := range nodes {
			if nodes[i].Name == name {
				return rpc.ResolveEndpoint(nodes[i].Endpoint)
			}
		}
	}
	return rpc.ResolveEndpoint(raw)
}

// nodeForEndpoint 找出登记地址等于 endpoint 的节点。
func nodeForEndpoint(endpoint string) (sboxNode, bool) {
	nodes, _ := loadNodes()
	for i := range nodes {
		if resolved, err := rpc.ResolveEndpoint(nodes[i].Endpoint); err == nil && resolved == endpoint {
			return nodes[i], true
		}
	}
	return sboxNode{}, false
}

// sboxRuntimeFor 返回盘在 endpoint 上的沙箱应当使用的运行时：该节点登记的 Docker，没登记就是本机。
func sboxRuntimeFor(endpoint string) (*sandbox.Runtime, sboxNode) {
	node, ok := nodeForEndpoint(endpoint)
	if !ok || node.Docker == "" {
		return sboxRuntime, node
	}
	return &sandbox.Runtime{Host: node.Docker}, node
}

// sboxAllRuntimes 返回所有可能跑着沙箱的 Docker：本机在前，其后是各节点登记的。
func sboxAllRuntimes() []*sandbox.Runtime {
	runtimes := []*sandbox.Runtime{sboxRuntime}
	nodes, _ := loadNodes()
	seen := []string{""}
	for i := range nodes {
		if host := nodes[i].Docker; !slices.Contains(seen, host) {
			seen = append(seen, host)
			runtimes = append(runtimes, &sandbox.Runtime{Host: host})
		}
	}
	return runtimes
}

var sboxLocated = map[string]*sandbox.Runtime{}

// sboxFor 找出沙箱 id 的容器在哪个 Docker 上。哪儿都没找到时返回本机运行时，
// 让后续动作报出通常的“沙箱不存在”。
func sboxFor(id string) *sandbox.Runtime {
	if rt, ok := sboxLocated[id]; ok {
		return rt
	}
	runtimes := sboxAllRuntimes()
	found := runtimes[0]
	if len(runtimes) > 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, rt := range runtimes {
			if rt.Exists(ctx, id) {
				found = rt
				break
			}
		}
	}
	sboxLocated[id] = found
	return found
}

// sboxStart 在 spec.HostEndpoint 对应的算力上起沙箱：按那台机器的架构准备客户端，
// 并带上该节点登记的沙箱内存储地址。
func sboxStart(spec *sandbox.CreateSpec, binary string, rebuild bool, stderr io.Writer) error {
	rt, node := sboxRuntimeFor(spec.HostEndpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	arch, err := rt.Arch(ctx)
	if err != nil {
		return err
	}
	bin, err := sandbox.EnsureClientBinary(binary, arch, rebuild, stderr)
	if err != nil {
		return err
	}
	spec.Arch, spec.ClientBinary, spec.SandboxEndpoint = arch, bin, node.SandboxEndpoint
	delete(sboxLocated, spec.ID)
	return rt.Create(ctx, spec)
}

// executeNode 处理 bowl node 子命令。
func executeNode(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, nodeUsageHelp)
		return 2
	}
	switch args[0] {
	case "help", "--help", "-h":
		fmt.Fprint(stdout, nodeUsageHelp)
		return 0
	case "list", "ls":
		return nodeList(stdout, stderr)
	case "set", "add":
		return nodeSet(args[1:], stdout, stderr)
	case "rm", "remove", "delete":
		return nodeRemove(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "错误: 未知命令 \"%s\"\n\n%s", args[0], nodeUsageHelp)
		return 2
	}
}

func nodeList(stdout, stderr io.Writer) int {
	nodes, err := loadNodes()
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	if len(nodes) == 0 {
		fmt.Fprintln(stdout, "还没有登记节点（bowl node set <name> --endpoint <url>）")
		return 0
	}
	fmt.Fprintf(stdout, "%-12s %-28s %-28s %s\n", "NAME", "ENDPOINT", "DOCKER", "SANDBOX ENDPOINT")
	for i := range nodes {
		n := &nodes[i]
		fmt.Fprintf(stdout, "%-12s %-28s %-28s %s\n", n.Name, n.Endpoint, cmp.Or(n.Docker, "(本机)"), cmp.Or(n.SandboxEndpoint, "(同 ENDPOINT)"))
	}
	return 0
}

func nodeSet(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bowl node set", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	node := sboxNode{}
	fs.StringVar(&node.Endpoint, "endpoint", "", "")
	fs.StringVar(&node.Docker, "docker", "", "")
	fs.StringVar(&node.SandboxEndpoint, "sandbox-endpoint", "", "")
	if err := fs.Parse(sboxHoistFlags(args, []string{"endpoint", "docker", "sandbox-endpoint"})); err != nil || fs.NArg() != 1 {
		fmt.Fprint(stderr, nodeUsageHelp)
		return 2
	}
	node.Name = fs.Arg(0)
	if strings.Contains(node.Name, "://") || strings.ContainsAny(node.Name, " /") {
		fmt.Fprintln(stderr, "错误: 节点名不能包含 :// 、空格或斜杠")
		return 2
	}
	resolved, err := rpc.ResolveEndpoint(node.Endpoint)
	if node.Endpoint == "" || err != nil {
		fmt.Fprintf(stderr, "错误: 需要一个有效的 --endpoint: %v\n", cmp.Or(err, errors.New("未指定")))
		return 2
	}
	node.Endpoint = resolved
	nodes, err := loadNodes()
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	nodes = slices.DeleteFunc(nodes, func(n sboxNode) bool { return n.Name == node.Name })
	nodes = append(nodes, node)
	if err := saveNodes(nodes); err != nil {
		fmt.Fprintf(stderr, "错误: 保存节点登记失败: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "已登记节点 %s → %s（算力: %s）\n", node.Name, node.Endpoint, cmp.Or(node.Docker, "本机 Docker"))
	return 0
}

func nodeRemove(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, "用法: bowl node rm <name>\n")
		return 2
	}
	nodes, err := loadNodes()
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	kept := slices.DeleteFunc(slices.Clone(nodes), func(n sboxNode) bool { return n.Name == args[0] })
	if len(kept) == len(nodes) {
		fmt.Fprintf(stderr, "错误: 没有名为 %s 的节点\n", args[0])
		return 1
	}
	if err := saveNodes(kept); err != nil {
		fmt.Fprintf(stderr, "错误: 保存节点登记失败: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "已移除节点 %s\n", args[0])
	return 0
}
