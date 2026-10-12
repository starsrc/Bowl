package utils

import (
	"context"
	"crypto/rand"
	"fmt"
	"mime"
	"os"
	"os/user"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
)

// Exists 检查指定路径的文件或目录是否存在
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

// SplitDir 使用默认路径分隔符或逗号拆分路径列表
func SplitDir(d string) []string {
	dd := strings.Split(d, string(os.PathListSeparator))
	if len(dd) == 1 {
		dd = strings.Split(dd[0], ",")
	}
	return dd
}

// WithTimeout 带有绝对超时的函数执行上下文封装
func WithTimeout(pCtx context.Context, f func(context.Context) error, timeout time.Duration) error {
	var done = make(chan int, 1)
	var t = time.NewTimer(timeout)
	var err error
	ctx, cancel := context.WithCancel(pCtx)
	go func() {
		err = f(ctx)
		done <- 1
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		t.Stop()
	case <-done:
		t.Stop()
	case <-t.C:
		err = fmt.Errorf("timeout after %s: %w", timeout, ErrFuncTimeout)
	}
	cancel()
	return err
}

// GuessMimeType 根据扩展名猜测文件的 MIME 类型
func GuessMimeType(key string) string {
	mimeType := mime.TypeByExtension(path.Ext(key))
	if !strings.ContainsRune(mimeType, '/') {
		mimeType = "application/octet-stream"
	}
	return mimeType
}

// StringContains 检查字符串数组中是否包含目标元素
func StringContains(s []string, e string) bool {
	for _, item := range s {
		if item == e {
			return true
		}
	}
	return false
}

// FormatBytes 格式化字节大小为人类可读格式
func FormatBytes(n uint64) string {
	if n < 1024 {
		return fmt.Sprintf("%d Bytes", n)
	}
	units := []string{"K", "M", "G", "T", "P", "E"}
	m := n
	i := 0
	for ; i < len(units)-1 && m >= 1<<20; i++ {
		m = m >> 10
	}
	return fmt.Sprintf("%.2f %siB (%d Bytes)", float64(m)/1024.0, units[i], n)
}

// SupportANSIColor 检测输出终端是否支持 ANSI 彩色高亮
func SupportANSIColor(fd uintptr) bool {
	return isatty.IsTerminal(fd) && runtime.GOOS != "windows"
}

var logger = GetLogger("juicefs")

// RandRead 从高保真随机源填充字节数组
func RandRead(buf []byte) {
	if _, err := rand.Read(buf); err != nil {
		logger.Fatalf("生成随机内容失败: %s", err)
	}
}

var uids = make(map[int]string)
var gids = make(map[int]string)
var users = make(map[string]int)
var groups = make(map[string]int)
var idMutex sync.Mutex

// UserName 根据 uid 获取用户名
func UserName(uid int) string {
	idMutex.Lock()
	defer idMutex.Unlock()
	name, ok := uids[uid]
	if !ok {
		if u, err := user.LookupId(strconv.Itoa(uid)); err == nil {
			name = u.Username
		} else {
			name = strconv.Itoa(uid)
		}
		uids[uid] = name
	}
	return name
}

// GroupName 根据 gid 获取组名
func GroupName(gid int) string {
	idMutex.Lock()
	defer idMutex.Unlock()
	name, ok := gids[gid]
	if !ok {
		if g, err := user.LookupGroupId(strconv.Itoa(gid)); err == nil {
			name = g.Name
		} else {
			name = strconv.Itoa(gid)
		}
		gids[gid] = name
	}
	return name
}

// LookupUser 根据用户名解析 uid
func LookupUser(name string) int {
	idMutex.Lock()
	defer idMutex.Unlock()
	if u, ok := users[name]; ok {
		return u
	}
	var uid = -1
	if u, err := user.Lookup(name); err == nil {
		uid, _ = strconv.Atoi(u.Uid)
	} else if g, e := strconv.Atoi(name); e == nil {
		uid = g
	}
	users[name] = uid
	return uid
}

// LookupGroup 根据组名解析 gid
func LookupGroup(name string) int {
	idMutex.Lock()
	defer idMutex.Unlock()
	if u, ok := groups[name]; ok {
		return u
	}
	var gid = -1
	if u, err := user.LookupGroup(name); err == nil {
		gid, _ = strconv.Atoi(u.Gid)
	} else if g, e := strconv.Atoi(name); e == nil {
		gid = g
	}
	groups[name] = gid
	return gid
}

// Duration 解析带单位的时间区间字符串
func Duration(s string) time.Duration {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return time.Microsecond * time.Duration(v*1e6)
	}

	var d time.Duration
	p := strings.Index(s, "d")
	if p >= 0 {
		v, err = strconv.ParseFloat(s[:p], 64)
	}
	if err == nil && s[p+1:] != "" {
		d, err = time.ParseDuration(s[p+1:])
	}
	if err != nil {
		return 0
	}
	return d + time.Hour*time.Duration(v*24)
}
