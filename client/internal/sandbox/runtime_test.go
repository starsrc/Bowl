package sandbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainerName_按沙箱ID派生容器名(t *testing.T) {
	assert.Equal(t, "bowl-sbx-abc", ContainerName("abc"))
	assert.Equal(t, "bowl-sbx-abc", ContainerName("bowl-sbx-abc"))
}

func TestStripID_接受裸ID与容器名(t *testing.T) {
	assert.Equal(t, "abc", StripID("abc"))
	assert.Equal(t, "abc", StripID("bowl-sbx-abc"))
	assert.Equal(t, "abc", StripID("  abc  "))
}

func TestContainerEndpoint_改写本机回环地址(t *testing.T) {
	cases := map[string]string{
		"http://127.0.0.1:9000":   "http://host.docker.internal:9000",
		"http://localhost:9100":   "http://host.docker.internal:9100",
		"http://0.0.0.0:9000":     "http://host.docker.internal:9000",
		"http://127.0.0.1":        "http://host.docker.internal",
		"https://storage.bowl.io": "https://storage.bowl.io",
		"http://10.0.0.5:9001":    "http://10.0.0.5:9001",
		"":                        "",
	}
	for in, want := range cases {
		assert.Equal(t, want, containerEndpoint(in), "endpoint %q", in)
	}
}

func TestParseRows_跳过标签缺失行(t *testing.T) {
	out := "disk-1\tUp 3 minutes\tnode:24-alpine\t/mnt/data\t2026-11-13 17:00:00 +0800 CST\n" +
		"\t\t\t\t\n" +
		"disk-2\tExited (0) 1 minute ago\tubuntu:24.04\t/mnt/data\t2026-11-13 17:05:00 +0800 CST\n"
	rows := parseRows(out)
	assert.Len(t, rows, 2)
	assert.Equal(t, "disk-1", rows[0].ID)
	assert.Equal(t, "Up 3 minutes", rows[0].Status)
	assert.Equal(t, "/mnt/data", rows[0].Mount)
	assert.Equal(t, "disk-2", rows[1].ID)
	assert.Empty(t, rows[0].Endpoint, "没有节点列的旧输出照常解析")

	withNode := parseRows("disk-3\tUp 1 second\tubuntu:24.04\t/mnt/data\t2026-11-14 10:00:00 +0800 CST\thttp://127.0.0.1:9200\n")
	assert.Equal(t, "http://127.0.0.1:9200", withNode[0].Endpoint)
}

func TestParseRows_空输出返回空列表(t *testing.T) {
	assert.Empty(t, parseRows(""))
	assert.Empty(t, parseRows("\n\n"))
}

func TestParseMounts_解析挂载表并识别只读(t *testing.T) {
	table := `bowl:disk-a /mnt/data fuse.bowl rw,nosuid,nodev,relatime,default_permissions 0 0
proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
bowl:disk-b /mnt/ro fuse.bowl ro,relatime 0 0
`
	mounts := parseMounts(table)
	assert.Len(t, mounts, 2)
	assert.Equal(t, "disk-a", mounts[0].DiskID)
	assert.Equal(t, "/mnt/data", mounts[0].Path)
	assert.False(t, mounts[0].ReadOnly)
	assert.True(t, mounts[1].ReadOnly)
}

func TestParseIDs_每行一个ID(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, parseIDs("a\n\nb\n"))
	assert.Empty(t, parseIDs(""))
}

func TestSplitPath_拆分目录与末级名字(t *testing.T) {
	cases := []struct{ in, dir, name string }{
		{"/mnt/data/src", "/mnt/data", "src"},
		{"/mnt/data/src/", "/mnt/data", "src"},
		{"/a.txt", "/", "a.txt"},
		{"a.txt", ".", "a.txt"},
		{"/", "/", ""},
	}
	for _, c := range cases {
		dir, name := splitPath(c.in)
		assert.Equal(t, c.dir, dir, "dir of %q", c.in)
		assert.Equal(t, c.name, name, "name of %q", c.in)
	}
}

func TestCommandScript_逐参数引用避免二次解释(t *testing.T) {
	script := commandScript("/mnt/data", []string{"sh", "-c", "echo 'hi there' > a.txt"})
	assert.Equal(t, `cd '/mnt/data' && exec 'sh' '-c' 'echo '\''hi there'\'' > a.txt'`, script)

	assert.Equal(t,
		`cd '/mnt/data' && exec 'npm' 'run' 'dev'`,
		commandScript("/mnt/data", []string{"npm", "run", "dev"}))
}

func TestHostArgs_按地址形式选择docker参数(t *testing.T) {
	assert.Nil(t, hostArgs(""))
	assert.Equal(t, []string{"-H", "ssh://me@cloud"}, hostArgs("ssh://me@cloud"))
	assert.Equal(t, []string{"-H", "tcp://127.0.0.1:2385"}, hostArgs("tcp://127.0.0.1:2385"))
	assert.Equal(t, []string{"--context", "prod"}, hostArgs("prod"))
}

func TestNormalizeArch(t *testing.T) {
	assert.Equal(t, "arm64", normalizeArch("aarch64\n"))
	assert.Equal(t, "amd64", normalizeArch("x86_64"))
	assert.Equal(t, "arm64", normalizeArch("arm64"))
}
