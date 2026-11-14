package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNode_登记后可用节点名代替地址并选到对应算力(t *testing.T) {
	t.Setenv("BOWL_CONFIG_DIR", t.TempDir())
	var out, errOut bytes.Buffer

	// 没有登记时：地址照常解析，算力是本机
	resolved, err := resolveEndpoint("http://127.0.0.1:9100")
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:9100", resolved)
	rt, _ := sboxRuntimeFor(resolved)
	assert.Same(t, sboxRuntime, rt)
	assert.Len(t, sboxAllRuntimes(), 1)

	require.Zero(t, executeNode([]string{"set", "local", "--endpoint", "http://127.0.0.1:9100"}, &out, &errOut), errOut.String())
	require.Zero(t, executeNode([]string{"set", "cloud", "--endpoint", "http://127.0.0.1:9200/",
		"--docker", "ssh://me@cloud", "--sandbox-endpoint", "http://10.0.0.5:9000"}, &out, &errOut), errOut.String())

	resolved, err = resolveEndpoint("cloud")
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:9200", resolved, "节点名解析为规范化后的地址")

	rt, node := sboxRuntimeFor(resolved)
	assert.Equal(t, "ssh://me@cloud", rt.Host)
	assert.Equal(t, "http://10.0.0.5:9000", node.SandboxEndpoint)
	rt, _ = sboxRuntimeFor("http://127.0.0.1:9100")
	assert.Same(t, sboxRuntime, rt, "没登记 docker 的节点用本机算力")
	assert.Len(t, sboxAllRuntimes(), 2)

	// 同名再 set 是覆盖；rm 之后名字不再可用
	require.Zero(t, executeNode([]string{"set", "cloud", "--endpoint", "http://127.0.0.1:9300"}, &out, &errOut))
	resolved, err = resolveEndpoint("cloud")
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:9300", resolved)
	assert.Len(t, sboxAllRuntimes(), 1)
	require.Zero(t, executeNode([]string{"rm", "cloud"}, &out, &errOut))
	assert.Equal(t, 1, executeNode([]string{"rm", "cloud"}, &out, &errOut))

	// 非法输入
	assert.Equal(t, 2, executeNode([]string{"set", "bad/name", "--endpoint", "http://x"}, &out, &errOut))
	assert.Equal(t, 2, executeNode([]string{"set", "noendpoint"}, &out, &errOut))
}
