// Package handoff 实现"把一块盘从一个存储节点交接给另一个节点"所需的远端协议：
// 快照的上传与下载，以及记录当前写者的 HEAD。
//
// 这一层只认识一个对象存储形状的 Remote（Put/Get/Size/CompareAndSwap），不认识
// StoreActor、控制面数据库，也不认识具体的云厂商。原型阶段 Remote 用本地目录实现
// （DirRemote），之后换成 S3 只需要再实现一遍这四个方法。
//
// 远端布局（每块盘一个前缀）：
//
//	disks/<disk-id>/HEAD.json                    当前写者与最新快照，唯一会被改写的对象
//	objects/<sha[:2]>/<sha256>                   数据包，按内容寻址；不可变，跨快照、跨盘共享
//	disks/<disk-id>/snapshots/<name>/meta/...    该快照的 Pebble 切面文件
//	disks/<disk-id>/snapshots/<name>/manifest.json  快照清单，最后写入，存在即代表快照完整
package handoff

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	// ErrNotFound 表示远端不存在该对象。
	ErrNotFound = errors.New("handoff: remote object not found")
	// ErrConflict 表示 HEAD 在读取之后被别的节点改写过，本次交接作废。
	ErrConflict = errors.New("handoff: head changed concurrently")
	// ErrIncomplete 表示远端快照缺文件或大小不符。
	ErrIncomplete = errors.New("handoff: snapshot incomplete")
)

// Remote 是交接协议依赖的最小对象存储接口。
type Remote interface {
	// Put 写入一个对象，已存在则覆盖。
	Put(ctx context.Context, key string, r io.Reader) error
	// Get 读取一个对象；不存在返回 ErrNotFound。
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// ReadAt 读取对象的 [off, off+len(dst)) 这一段；不存在返回 ErrNotFound。
	// 对应对象存储的范围读取，是按需取块的基础。
	ReadAt(ctx context.Context, key string, off int64, dst []byte) error
	// Size 返回对象字节数；不存在返回 ErrNotFound。
	Size(ctx context.Context, key string) (int64, error)
	// List 列出键以 prefix 开头的全部对象。
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	// Delete 删除一个对象；对象不存在不算错误。
	Delete(ctx context.Context, key string) error
	// CompareAndSwap 仅当对象当前内容等于 old 时写入 next（old 为 nil 表示对象必须不存在），
	// 否则返回 ErrConflict。这是整个协议里唯一需要原子性的操作。
	CompareAndSwap(ctx context.Context, key string, old, next []byte) error
}

// ObjectInfo 是 List 返回的对象概要。
type ObjectInfo struct {
	Key     string
	Size    int64
	ModTime time.Time
}

// Head 记录一块盘在远端的权威状态：谁是当前写者、最新快照是哪个。
type Head struct {
	DiskID string `json:"disk_id"`
	Name   string `json:"name"`
	// Generation 每次改写加一，同时用作快照名的来源。
	Generation uint64 `json:"generation"`
	// Owner 是当前写者的节点 ID；为空表示已释放，任何节点都可以认领。
	Owner string `json:"owner"`
	// Snapshot 是最新一次完整上传的快照名。
	Snapshot string `json:"snapshot"`
	// WriterEpoch 是快照固化时盘内的写世代，仅用于排查。
	WriterEpoch uint64 `json:"writer_epoch"`
	// WriteSeq 是快照固化时盘内的写序号；与 WriterEpoch 一起标识"同步到了哪"。
	WriteSeq  uint64    `json:"write_seq"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy string    `json:"updated_by"`
}

// SnapshotName 由代数派生快照名，定宽以保证字典序即时间序。
func SnapshotName(generation uint64) string {
	return fmt.Sprintf("%020d", generation)
}

func diskPrefix(diskID string) string { return path.Join("disks", diskID) }
func headKey(diskID string) string    { return path.Join(diskPrefix(diskID), "HEAD.json") }
func snapshotPrefix(diskID, snapshot string) string {
	return path.Join(diskPrefix(diskID), "snapshots", snapshot)
}

// ReadHead 读取盘的 HEAD 及其原始字节（原始字节用于随后的 CompareAndSwap）。
// HEAD 不存在时返回 ErrNotFound。
func ReadHead(ctx context.Context, r Remote, diskID string) (Head, []byte, error) {
	rc, err := r.Get(ctx, headKey(diskID))
	if err != nil {
		return Head{}, nil, err
	}
	raw, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return Head{}, nil, err
	}
	var head Head
	if err := json.Unmarshal(raw, &head); err != nil {
		return Head{}, nil, fmt.Errorf("handoff: decode head: %w", err)
	}
	return head, raw, nil
}

// WriteHead 以 prevRaw 为前提改写 HEAD；prevRaw 为 nil 表示 HEAD 必须尚不存在。
func WriteHead(ctx context.Context, r Remote, prevRaw []byte, next *Head) error {
	raw, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	return r.CompareAndSwap(ctx, headKey(next.DiskID), prevRaw, raw)
}

type manifestFile struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	// SHA256 只对数据包填写：它既是远端对象名，也是判断"两端是不是同一个包"的唯一依据。
	SHA256 string `json:"sha256,omitempty"`
}

type manifest struct {
	Meta  []manifestFile `json:"meta"`
	Packs []manifestFile `json:"packs"`
}

// PushStats 汇总一次快照上传的工作量。
type PushStats struct {
	MetaFiles     int   `json:"meta_files"`
	MetaBytes     int64 `json:"meta_bytes"`
	Packs         int   `json:"packs"`
	PacksUploaded int   `json:"packs_uploaded"`
	PackBytes     int64 `json:"pack_bytes_uploaded"`
}

// PullStats 汇总一次快照下载的工作量。
type PullStats struct {
	Packs           int `json:"packs"`
	PacksReused     int `json:"packs_reused"`
	PacksDownloaded int `json:"packs_downloaded"`
	// PacksDeferred 是只留了占位、等读到时再取的包；DeferredBytes 是它们的总大小。
	PacksDeferred int   `json:"packs_deferred"`
	DeferredBytes int64 `json:"pack_bytes_deferred"`
	PackBytes     int64 `json:"pack_bytes_downloaded"`
	MetaBytes     int64 `json:"meta_bytes"`
}

// objectKey 是数据包在远端的内容寻址位置。
//
// 不能按包名存：包名（PackID）只在一个节点的控制面内保证不重复，一块盘换到另一个节点后，
// 那边完全可能分配出同名但内容不同的包。按内容存则永远不会覆盖已有对象，
// 任何旧快照都保持可读；同源 fork 出来的盘共享的包在远端也只存一份。
func objectKey(sha string) string {
	return path.Join("objects", sha[:2], sha)
}

// hashCache 记住已算过的包哈希，键是 (设备, inode, 大小, 修改时间)。
// 封口后的包不可变，切面目录里的包又是硬链接，所以同一个包在每一轮同步里只需要读一次。
var hashCache sync.Map

func fileSHA256(file string) (string, error) {
	info, err := os.Stat(file)
	if err != nil {
		return "", err
	}
	key := fileIdentity(info)
	if key != "" {
		if cached, ok := hashCache.Load(key); ok {
			if sum, isString := cached.(string); isString {
				return sum, nil
			}
		}
	}
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	if err := errors.Join(copyErr, f.Close()); err != nil {
		return "", err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if key != "" {
		hashCache.Store(key, sum)
	}
	return sum, nil
}

func fileIdentity(info os.FileInfo) string {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d:%d", st.Dev, st.Ino, info.Size(), info.ModTime().UnixNano())
}

// StubReader 判断 path 是否为某个数据包的占位文件，并给出它代表的包名、内容哈希与大小。
type StubReader func(path string) (packName, sha256 string, size int64, ok bool)

// DeferPack 为一个暂不下载的数据包在 packsDir 下留占位。
type DeferPack func(packsDir, packName, sha256 string, size int64) error

// FetchObject 把一个数据包对象完整下载到 dst 并校验内容。
func FetchObject(ctx context.Context, r Remote, sha string, size int64, dst string) error {
	return getFile(ctx, r, objectKey(sha), dst, size, sha)
}

// ReadObjectRange 读取数据包对象的一段。
func ReadObjectRange(ctx context.Context, r Remote, sha string, off int64, dst []byte) error {
	return r.ReadAt(ctx, objectKey(sha), off, dst)
}

// PushSnapshot 把一个 Checkpoint 切面目录上传为远端快照。
//
// 切面里的数据包都已封口、不可变，按内容寻址上传：远端已有同内容的对象就跳过，
// 所以每次同步只传新写出来的包。其余文件（Pebble 的 SST、MANIFEST 等）体量小，每个快照各存一份。
// manifest 最后写入：读到 manifest 即说明它列出的对象都已上传完毕。
//
// stubOf 用来识别切面里"还没下载到本地"的包的占位文件（可为 nil）：这种包本来就在远端，
// 直接记进清单，不需要也无法上传。
func PushSnapshot(ctx context.Context, r Remote, diskID, snapshot, dir string, stubOf StubReader) (PushStats, error) {
	var (
		stats PushStats
		mf    manifest
	)
	root := filepath.Clean(dir)
	walkErr := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if rel == "LOCK" {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		entry := manifestFile{Path: rel, Size: info.Size()}

		if name, isPack := strings.CutPrefix(rel, "packs/"); isPack {
			sum, sumErr := fileSHA256(p)
			if sumErr != nil {
				return sumErr
			}
			entry.Path, entry.SHA256 = name, sum
			mf.Packs = append(mf.Packs, entry)
			stats.Packs++
			if size, sizeErr := r.Size(ctx, objectKey(sum)); sizeErr == nil && size == info.Size() {
				return nil
			} else if sizeErr != nil && !errors.Is(sizeErr, ErrNotFound) {
				return sizeErr
			}
			stats.PacksUploaded++
			stats.PackBytes += info.Size()
			return putFile(ctx, r, objectKey(sum), p)
		}

		if strings.HasPrefix(rel, "packs/") && stubOf != nil {
			if packName, sum, size, ok := stubOf(p); ok {
				if remoteSize, sizeErr := r.Size(ctx, objectKey(sum)); sizeErr != nil || remoteSize != size {
					return fmt.Errorf("%w: pack %s is neither local nor in the remote", ErrIncomplete, packName)
				}
				mf.Packs = append(mf.Packs, manifestFile{Path: packName, Size: size, SHA256: sum})
				stats.Packs++
				return nil
			}
		}

		mf.Meta = append(mf.Meta, entry)
		stats.MetaFiles++
		stats.MetaBytes += info.Size()
		return putFile(ctx, r, path.Join(snapshotPrefix(diskID, snapshot), "meta", rel), p)
	})
	if walkErr != nil {
		return PushStats{}, walkErr
	}
	raw, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return PushStats{}, err
	}
	if err := r.Put(ctx, path.Join(snapshotPrefix(diskID, snapshot), "manifest.json"), bytes.NewReader(raw)); err != nil {
		return PushStats{}, err
	}
	return stats, nil
}

// PullSnapshot 把远端快照还原成一个可以直接被 StoreActor 打开的目录。
//
// reuseDir 是本地已有的同一块盘的目录（可为空）：其中内容与清单一致的数据包直接硬链接过来，
// 不再下载。"一致"按 SHA-256 判定而不是按包名，原因见 objectKey。
// 这让切回一个去过的节点时只需下载离开期间新写出来的包。
// deferPack 非 nil 时，本地没有的数据包不下载，只留占位，等真正读到时再取：
// 拉取耗时只和元数据大小有关，和盘里有多少数据无关。
// dstDir 必须不存在；任何一步失败都会清掉已下载的部分。
func PullSnapshot(ctx context.Context, r Remote, diskID, snapshot, dstDir, reuseDir string, deferPack DeferPack) (_ PullStats, retErr error) {
	var stats PullStats
	rc, err := r.Get(ctx, path.Join(snapshotPrefix(diskID, snapshot), "manifest.json"))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return stats, fmt.Errorf("%w: snapshot %s has no manifest", ErrIncomplete, snapshot)
		}
		return stats, err
	}
	raw, readErr := io.ReadAll(rc)
	if err := errors.Join(readErr, rc.Close()); err != nil {
		return stats, err
	}
	var mf manifest
	if err := json.Unmarshal(raw, &mf); err != nil {
		return stats, fmt.Errorf("handoff: decode manifest: %w", err)
	}

	dst := filepath.Clean(dstDir)
	if _, statErr := os.Stat(dst); statErr == nil {
		return stats, fmt.Errorf("handoff: destination %s already exists", dst)
	}
	if err := os.MkdirAll(filepath.Join(dst, "packs"), 0o755); err != nil {
		return stats, err
	}
	defer func() {
		if retErr != nil {
			_ = os.RemoveAll(dst)
		}
	}()

	for _, f := range mf.Meta {
		key := path.Join(snapshotPrefix(diskID, snapshot), "meta", f.Path)
		if err := getFile(ctx, r, key, localPath(dst, f.Path), f.Size, ""); err != nil {
			return stats, err
		}
		stats.MetaBytes += f.Size
	}
	for _, f := range mf.Packs {
		if len(f.SHA256) != sha256.Size*2 {
			return stats, fmt.Errorf("%w: pack %s has no content hash", ErrIncomplete, f.Path)
		}
		stats.Packs++
		target := localPath(filepath.Join(dst, "packs"), f.Path)
		if reuseLocalPack(reuseDir, f, target) {
			stats.PacksReused++
			continue
		}
		if deferPack != nil {
			if err := deferPack(filepath.Join(dst, "packs"), f.Path, f.SHA256, f.Size); err != nil {
				return stats, err
			}
			stats.PacksDeferred++
			stats.DeferredBytes += f.Size
			continue
		}
		if err := getFile(ctx, r, objectKey(f.SHA256), target, f.Size, f.SHA256); err != nil {
			return stats, err
		}
		stats.PacksDownloaded++
		stats.PackBytes += f.Size
	}
	return stats, syncDir(dst)
}

// reuseLocalPack 尝试用本地已有的同内容数据包满足清单里的一项。
func reuseLocalPack(reuseDir string, f manifestFile, target string) bool {
	if reuseDir == "" {
		return false
	}
	local := localPath(filepath.Join(reuseDir, "packs"), f.Path)
	info, err := os.Stat(local)
	if err != nil || info.Size() != f.Size {
		return false
	}
	sum, err := fileSHA256(local)
	if err != nil || sum != f.SHA256 {
		return false
	}
	return os.Link(local, target) == nil
}

// localPath 把清单里的相对路径落到 base 下，并拒绝跳出 base 的路径（清单来自远端，不可信）。
func localPath(base, rel string) string {
	return filepath.Join(base, filepath.FromSlash(path.Clean("/"+rel)))
}

func putFile(ctx context.Context, r Remote, key, file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	putErr := r.Put(ctx, key, f)
	return errors.Join(putErr, f.Close())
}

// getFile 下载一个对象；wantSHA 非空时边写边校验内容，防止远端对象损坏或被替换。
func getFile(ctx context.Context, r Remote, key, file string, wantSize int64, wantSHA string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rc, err := r.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("%w: missing %s", ErrIncomplete, key)
		}
		return err
	}
	defer rc.Close()
	if mkErr := os.MkdirAll(filepath.Dir(file), 0o755); mkErr != nil {
		return mkErr
	}
	out, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, h), rc)
	syncErr := out.Sync()
	closeErr := out.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		return err
	}
	if n != wantSize {
		return fmt.Errorf("%w: %s is %d bytes, manifest says %d", ErrIncomplete, key, n, wantSize)
	}
	if wantSHA != "" && hex.EncodeToString(h.Sum(nil)) != wantSHA {
		return fmt.Errorf("%w: %s content does not match its hash", ErrIncomplete, key)
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
