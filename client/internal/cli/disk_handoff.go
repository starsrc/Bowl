package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/starsrc/bowl/client/internal/rpc"
)

const diskHandoffUsageHelp = `用法:
  bowl disk push <disk-id> [--release]   把磁盘当前状态同步到远端；--release 同时交出写权限
  bowl disk pull <disk-id> [--force] [--keep-local]
                                         从远端拉取最新状态并认领写权限；
                                         --force 强行接管仍被其他节点持有的磁盘，并允许覆盖本地未同步的写入；
                                         --keep-local 本地有未同步的写入时把它们另存为一块新盘，
                                         之后可用 bowl disk merge 合并回来
  bowl disk head <disk-id>               查看远端记录的当前写者与最新快照

说明:
  交接在两个存储节点之间转移一块盘的写权限。切换算力的完整动作是：
    1) 在原节点卸载磁盘   2) 原节点 push --release   3) 新节点 pull   4) 在新节点挂载
  --endpoint 指向执行该动作的那个存储节点。

标志:
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --json               输出服务端返回的原始 JSON
  -h, --help           查看帮助信息
`

// 上传与下载的耗时取决于数据量，给足时间；连不上的情况由拨号超时兜住。
var diskHandoffHTTPClient = &http.Client{Timeout: 30 * time.Minute}

type diskHandoffHead struct {
	DiskID      string `json:"disk_id"`
	Name        string `json:"name"`
	Generation  uint64 `json:"generation"`
	Owner       string `json:"owner"`
	Snapshot    string `json:"snapshot"`
	WriterEpoch uint64 `json:"writer_epoch"`
	UpdatedBy   string `json:"updated_by"`
	UpdatedAt   string `json:"updated_at"`
}

type diskHandoffResult struct {
	Head  diskHandoffHead `json:"head"`
	Stats struct {
		Packs         int   `json:"packs"`
		PacksUploaded int   `json:"packs_uploaded"`
		PackBytes     int64 `json:"pack_bytes_uploaded"`
		MetaBytes     int64 `json:"meta_bytes"`
	} `json:"stats"`
	Pulled struct {
		Packs           int   `json:"packs"`
		PacksReused     int   `json:"packs_reused"`
		PacksDownloaded int   `json:"packs_downloaded"`
		PackBytes       int64 `json:"pack_bytes_downloaded"`
		PacksDeferred   int   `json:"packs_deferred"`
		DeferredBytes   int64 `json:"pack_bytes_deferred"`
	} `json:"pulled"`
	Installed       bool   `json:"installed"`
	KeptLocalDiskID string `json:"kept_local_disk_id"`
}

// transferSummary 用一句话说明这次交接实际传了多少数据。
func (r *diskHandoffResult) transferSummary(verb string) string {
	if verb == "push" {
		return fmt.Sprintf("上传 %d/%d 个数据包，%s", r.Stats.PacksUploaded, r.Stats.Packs, FormatSize(uint64(r.Stats.PackBytes)))
	}
	return fmt.Sprintf("共 %d 个数据包：本地复用 %d 个；%d 个（%s）留待读到时再取",
		r.Pulled.Packs, r.Pulled.PacksReused, r.Pulled.PacksDeferred, FormatSize(uint64(r.Pulled.DeferredBytes)))
}

// executeDiskHandoff 处理 disk push / pull / head。
// 这三个命令走存储服务的 JSON 管理路由而不是 RPC，见服务端 handler_handoff.go 的说明。
func executeDiskHandoff(verb string, args []string, stdout, stderr io.Writer) int {
	var (
		diskID, endpoint                   string
		release, force, keepLocal, jsonOut bool
	)
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "-h" || arg == "--help" || arg == "help":
			fmt.Fprint(stdout, diskHandoffUsageHelp)
			return 0
		case arg == "--release" && verb == "push":
			release = true
		case arg == "--force" && verb == "pull":
			force = true
		case arg == "--keep-local" && verb == "pull":
			keepLocal = true
		case arg == "--json":
			jsonOut = true
		case arg == "--endpoint" && i+1 < len(args):
			i++
			endpoint = args[i]
		case strings.HasPrefix(arg, "--endpoint="):
			endpoint = strings.TrimPrefix(arg, "--endpoint=")
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(stderr, "错误: disk %s 不支持标志 %s\n\n%s", verb, arg, diskHandoffUsageHelp)
			return 2
		case diskID == "":
			diskID = arg
		default:
			fmt.Fprintf(stderr, "错误: 多余的参数 %s\n", arg)
			return 2
		}
	}
	if diskID == "" {
		fmt.Fprintf(stderr, "错误: 缺少 <disk-id>\n\n%s", diskHandoffUsageHelp)
		return 2
	}
	base, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 endpoint: %v\n", err)
		return 2
	}

	raw, status, err := callDiskHandoff(base, verb, diskID, handoffFlags{release: release, force: force, keepLocal: keepLocal})
	if err != nil {
		fmt.Fprintf(stderr, "错误: 请求存储节点失败: %v\n", err)
		return 1
	}
	if status != http.StatusOK {
		fmt.Fprintf(stderr, "错误: %s\n", describeHandoffFailure(raw, status))
		return 1
	}
	if jsonOut {
		fmt.Fprintln(stdout, strings.TrimSpace(string(raw)))
		return 0
	}
	return printDiskHandoff(verb, raw, stdout, stderr)
}

// handoffFlags 是一次 push / pull 请求的开关。
type handoffFlags struct {
	release, force, keepLocal bool
}

func callDiskHandoff(base, verb, diskID string, flags handoffFlags) (raw []byte, status int, err error) {
	base = strings.TrimRight(base, "/")
	var resp *http.Response
	if verb == "head" {
		resp, err = diskHandoffHTTPClient.Get(base + "/handoff/v1/head?disk_id=" + url.QueryEscape(diskID))
	} else {
		body, _ := json.Marshal(map[string]any{"disk_id": diskID, "release": flags.release, "force": flags.force, "keep_local": flags.keepLocal})
		resp, err = diskHandoffHTTPClient.Post(base+"/handoff/v1/"+verb, "application/json", bytes.NewReader(body))
	}
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return raw, resp.StatusCode, err
}

// describeHandoffFailure 把服务端的失败原因翻成"下一步该做什么"。
func describeHandoffFailure(raw []byte, status int) string {
	var body struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if json.Unmarshal(raw, &body) != nil || body.Error == "" {
		return fmt.Sprintf("存储节点返回 HTTP %d: %s", status, strings.TrimSpace(string(raw)))
	}
	hint := map[string]string{
		"NOT_OWNER":      "这块盘的写权限在别的节点上。先在那个节点 push --release，或在这里 pull --force 强行接管",
		"MOUNTED":        "这块盘还挂着。先卸载再交接",
		"LOCAL_DIVERGED": "本地有没同步过的写入，拉取会覆盖它们。用 pull --keep-local 把它们另存为一块盘（之后可合并回来），或用 pull --force 直接丢弃",
		"CONFLICT":       "交接过程中远端被别的节点改写了，重试一次",
		"DISABLED":       "这个存储节点没有配置交接远端 (HANDOFF_REMOTE_DIR)",
		"NOT_FOUND":      "远端没有这块盘的记录，它还没被 push 过",
	}[body.Reason]
	if hint == "" {
		return body.Error
	}
	return hint + "\n      (" + body.Error + ")"
}

func printDiskHandoff(verb string, raw []byte, stdout, stderr io.Writer) int {
	if verb == "head" {
		var head diskHandoffHead
		if err := json.Unmarshal(raw, &head); err != nil {
			fmt.Fprintf(stderr, "错误: 无法解析响应: %v\n", err)
			return 1
		}
		printDiskHandoffHead(stdout, &head)
		return 0
	}
	var res diskHandoffResult
	if err := json.Unmarshal(raw, &res); err != nil {
		fmt.Fprintf(stderr, "错误: 无法解析响应: %v\n", err)
		return 1
	}
	switch {
	case verb == "push" && res.Head.Owner == "":
		fmt.Fprintf(stdout, "已同步并交出磁盘 %s 的写权限\n", res.Head.DiskID)
	case verb == "push":
		fmt.Fprintf(stdout, "已同步磁盘 %s（写权限仍在本节点）\n", res.Head.DiskID)
	case res.Installed:
		fmt.Fprintf(stdout, "已拉取磁盘 %s 并认领写权限\n", res.Head.DiskID)
	default:
		fmt.Fprintf(stdout, "磁盘 %s 的写权限本来就在这个节点，未做改动\n", res.Head.DiskID)
	}
	if verb == "push" || res.Installed {
		fmt.Fprintf(stdout, "  传输   %s\n", res.transferSummary(verb))
	}
	printDiskHandoffHead(stdout, &res.Head)
	if res.KeptLocalDiskID != "" {
		fmt.Fprintf(stdout, "  本地未同步的写入已另存为磁盘 %s\n", res.KeptLocalDiskID)
		fmt.Fprintf(stdout, "  合并回来: bowl disk merge %s --into %s [--strategy fail|ours|theirs]\n", res.KeptLocalDiskID, res.Head.DiskID)
	}
	return 0
}

func printDiskHandoffHead(stdout io.Writer, head *diskHandoffHead) {
	owner := head.Owner
	if owner == "" {
		owner = "(已释放，等待认领)"
	}
	fmt.Fprintf(stdout, "  写者   %s\n", owner)
	fmt.Fprintf(stdout, "  快照   %s (第 %d 代，写世代 %d)\n", head.Snapshot, head.Generation, head.WriterEpoch)
}
