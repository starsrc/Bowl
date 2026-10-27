package server

import "go.uber.org/fx"

// Module 提供 Server 构造并在 Fx 容器中完成生命周期装配。
var Module = fx.Module("server",
	fx.Provide(New),
	fx.Invoke(func(*Server) {}),
)
