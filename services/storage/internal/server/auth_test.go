package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequireBearerToken_配置令牌后拒绝缺失或错误的凭证(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	call := func(h http.Handler, header string) int {
		req := httptest.NewRequest(http.MethodPost, "/storage.v1.StorageService/ListDisks", http.NoBody)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	guarded := requireBearerToken(ok, "s3cret")
	require.Equal(t, http.StatusUnauthorized, call(guarded, ""))
	require.Equal(t, http.StatusUnauthorized, call(guarded, "Bearer wrong"))
	require.Equal(t, http.StatusUnauthorized, call(guarded, "s3cret"), "必须是 Bearer 方案")
	require.Equal(t, http.StatusUnauthorized, call(guarded, "Bearer s3cret-and-more"))
	require.Equal(t, http.StatusNoContent, call(guarded, "Bearer s3cret"))

	open := requireBearerToken(ok, "")
	require.Equal(t, http.StatusNoContent, call(open, ""), "未配置令牌时保持原有的不鉴权行为")
}
