package server

import "go.uber.org/fx"

// Module 提供 Server 构造并自动挂载 Fx 生命周期启停钩子。
var Module = fx.Module("server",
	fx.Provide(New),
	fx.Invoke(registerHooks),
)

// registerHooks 向 Fx 生命周期注册 Server 启停钩子。
func registerHooks(lc fx.Lifecycle, srv *Server, shutdowner fx.Shutdowner) {
	if srv == nil || lc == nil {
		return
	}
	srv.registerLifecycle(lc, shutdowner)
}
