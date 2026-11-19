package server

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// requireBearerToken 在配置了访问令牌时校验 Authorization: Bearer <token>，
// 覆盖公网端口上的全部路由（ConnectRPC 与交接管理路由）。token 为空时不做任何校验。
//
// ponytail: 全局单令牌，谁拿到都能访问所有磁盘。多租户需要按令牌区分身份与磁盘归属，
// 届时换成带主体信息的凭证并在控制面做授权。
func requireBearerToken(next http.Handler, token string) http.Handler {
	token = strings.TrimSpace(token)
	if token == "" {
		return next
	}
	want := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="bowl-storage"`)
			http.Error(w, "missing or invalid access token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
