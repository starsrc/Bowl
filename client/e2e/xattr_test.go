//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestE2E_Xattr(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("xattr_crud_probes_and_flags", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "test-file.txt")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
		require.NoError(t, err, "创建测试文件成功")
		require.NoError(t, f.Close(), "关闭测试文件成功")

		dirPath := filepath.Join(mountpoint, "test-dir")
		err = os.Mkdir(dirPath, 0755)
		require.NoError(t, err, "创建测试目录成功")

		targets := []struct {
			name string
			path string
		}{
			{name: "regular_file", path: filePath},
			{name: "directory", path: dirPath},
			{name: "mount_root", path: mountpoint},
		}

		for _, tc := range targets {
			targetPath := tc.path
			t.Run(tc.name, func(t *testing.T) {
				attrFoo := "user.foo"
				attrBinary := "user.binary"
				attrNonExistent := "user.nonexistent"

				// 1. 获取不存在的属性返回 ENODATA
				_, err := unix.Getxattr(targetPath, attrNonExistent, nil)
				require.Error(t, err, "获取不存在的属性必须报错")
				require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

				// 2. 删除不存在的属性返回 ENODATA
				err = unix.Removexattr(targetPath, attrNonExistent)
				require.Error(t, err, "删除不存在属性必须报错")
				require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

				// 3. XATTR_REPLACE 对不存在的属性设置返回 ENODATA
				err = unix.Setxattr(targetPath, attrFoo, []byte("bar"), unix.XATTR_REPLACE)
				require.Error(t, err, "对不存在的属性使用 XATTR_REPLACE 必须报错")
				require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

				// 4. XATTR_CREATE 设置新属性必须成功
				valFoo := []byte("value-of-foo")
				err = unix.Setxattr(targetPath, attrFoo, valFoo, unix.XATTR_CREATE)
				require.NoError(t, err, "对不存在的属性使用 XATTR_CREATE 必须成功")

				// 5. XATTR_CREATE 再次设置已存在属性必须返回 EEXIST
				err = unix.Setxattr(targetPath, attrFoo, []byte("conflict"), unix.XATTR_CREATE)
				require.Error(t, err, "对已存在属性使用 XATTR_CREATE 必须报错")
				require.True(t, errors.Is(err, unix.EEXIST) || errors.Is(err, syscall.EEXIST), "错误必须为 EEXIST")

				// 6. XATTR_REPLACE 覆盖已存在属性成功
				valFooNew := []byte("value-of-foo-updated")
				err = unix.Setxattr(targetPath, attrFoo, valFooNew, unix.XATTR_REPLACE)
				require.NoError(t, err, "对已存在属性使用 XATTR_REPLACE 必须成功")

				// 7. 设置包含 '\x00' 的二进制值
				valBinary := []byte("prefix\x00middle\xff\x00suffix")
				err = unix.Setxattr(targetPath, attrBinary, valBinary, 0)
				require.NoError(t, err, "设置二进制属性值必须成功")

				// 8. 零长度 nil 探测 Getxattr
				sz, err := unix.Getxattr(targetPath, attrBinary, nil)
				require.NoError(t, err, "nil 探测 Getxattr 必须成功")
				require.Equal(t, len(valBinary), sz, "零长度探测应返回精确字节数")

				// 9. 小缓冲区 Getxattr 返回 ERANGE
				smallBuf := make([]byte, len(valBinary)-2)
				_, err = unix.Getxattr(targetPath, attrBinary, smallBuf)
				require.Error(t, err, "缓冲区过小 Getxattr 必须报错")
				require.True(t, errors.Is(err, unix.ERANGE) || errors.Is(err, syscall.ERANGE), "错误必须为 ERANGE")

				// 10. 充足缓冲区 Getxattr 完整读出二进制内容
				exactBuf := make([]byte, len(valBinary)+8)
				szRead, err := unix.Getxattr(targetPath, attrBinary, exactBuf)
				require.NoError(t, err, "充足缓冲区 Getxattr 必须成功")
				require.Equal(t, len(valBinary), szRead)
				require.Equal(t, valBinary, exactBuf[:szRead])

				// 11. 零长度 nil 探测 Listxattr
				listSz, err := unix.Listxattr(targetPath, nil)
				require.NoError(t, err, "nil 探测 Listxattr 必须成功")
				require.True(t, listSz >= len(attrFoo)+1+len(attrBinary)+1, "Listxattr 探测长度应包含所有属性名")

				// 12. 小缓冲区 Listxattr 返回 ERANGE
				_, err = unix.Listxattr(targetPath, make([]byte, listSz-1))
				require.Error(t, err, "缓冲区过小 Listxattr 必须报错")
				require.True(t, errors.Is(err, unix.ERANGE) || errors.Is(err, syscall.ERANGE), "错误必须为 ERANGE")

				// 13. 充足缓冲区 Listxattr 验证属性名列表包含两个属性
				listBuf := make([]byte, listSz+16)
				szListRead, err := unix.Listxattr(targetPath, listBuf)
				require.NoError(t, err, "充足缓冲区 Listxattr 必须成功")
				require.Equal(t, listSz, szListRead)

				rawList := string(listBuf[:szListRead])
				names := strings.Split(strings.TrimSuffix(rawList, "\x00"), "\x00")
				require.Contains(t, names, attrFoo)
				require.Contains(t, names, attrBinary)

				// 14. 超过 64 KiB 属性值返回 E2BIG
				largeVal := bytes.Repeat([]byte("A"), 65537)
				err = unix.Setxattr(targetPath, "user.toolarge", largeVal, 0)
				require.Error(t, err, "设置超 64 KiB 属性值必须报错")
				require.True(t, errors.Is(err, unix.E2BIG) || errors.Is(err, syscall.E2BIG), "错误必须为 E2BIG")

				// 15. Removexattr 删除属性
				err = unix.Removexattr(targetPath, attrFoo)
				require.NoError(t, err, "删除属性 user.foo 必须成功")

				// 16. 删除后再获取返回 ENODATA
				_, err = unix.Getxattr(targetPath, attrFoo, nil)
				require.Error(t, err, "删除后再获取必须报错")
				require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

				// 17. 删除后从 Listxattr 中消失
				newListSz, err := unix.Listxattr(targetPath, nil)
				require.NoError(t, err)
				newBuf := make([]byte, newListSz)
				if newListSz > 0 {
					_, err = unix.Listxattr(targetPath, newBuf)
					require.NoError(t, err)
					newNames := strings.Split(strings.TrimSuffix(string(newBuf), "\x00"), "\x00")
					require.NotContains(t, newNames, attrFoo)
					require.Contains(t, newNames, attrBinary)
				}

				// 清理残留的 attrBinary 保证同一 targetPath 可重复复用
				_ = unix.Removexattr(targetPath, attrBinary)
			})
		}
	})

	t.Run("xattr_read_only_mount", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)

		// 1. 先用读写模式挂载写入一个预置属性
		mountpointRW := t.TempDir()
		cmdRW, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpointRW, "--debug")
		require.NoError(t, waitForMount(mountpointRW, 10*time.Second), "挂载读写就绪")

		filePath := filepath.Join(mountpointRW, "ro-file.txt")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
		require.NoError(t, err, "创建只读测试文件成功")
		require.NoError(t, f.Close(), "关闭只读测试文件成功")
		require.NoError(t, unix.Setxattr(filePath, "user.ro_key", []byte("ro_value"), 0))
		stopBowlMountGracefully(t, cmdRW, mountpointRW, 5*time.Second)

		// 2. 以 --ro 只读模式重新挂载
		mountpointRO := t.TempDir()
		cmdRO, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpointRO, "--ro", "--debug")
		require.NoError(t, waitForMount(mountpointRO, 10*time.Second), "挂载只读就绪")
		defer stopBowlMountGracefully(t, cmdRO, mountpointRO, 5*time.Second)

		roFilePath := filepath.Join(mountpointRO, "ro-file.txt")

		// 验证只读模式下 Getxattr 与 Listxattr 正常可读
		valBuf := make([]byte, 32)
		sz, err := unix.Getxattr(roFilePath, "user.ro_key", valBuf)
		require.NoError(t, err, "只读挂载点读取已存在的 xattr 必须成功")
		require.Equal(t, "ro_value", string(valBuf[:sz]))

		listSz, err := unix.Listxattr(roFilePath, nil)
		require.NoError(t, err, "只读挂载点列出 xattr 必须成功")
		require.True(t, listSz > 0)

		// 验证只读模式下 Setxattr 返回 EROFS
		err = unix.Setxattr(roFilePath, "user.ro_new", []byte("val"), 0)
		require.Error(t, err, "只读挂载点 Setxattr 必须报错")
		require.True(t, errors.Is(err, unix.EROFS) || errors.Is(err, syscall.EROFS), "错误必须为 EROFS")

		// 验证只读模式下 Removexattr 返回 EROFS
		err = unix.Removexattr(roFilePath, "user.ro_key")
		require.Error(t, err, "只读挂载点 Removexattr 必须报错")
		require.True(t, errors.Is(err, unix.EROFS) || errors.Is(err, syscall.EROFS), "错误必须为 EROFS")
	})
}
