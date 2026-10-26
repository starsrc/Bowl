package config

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

const (
	// DefaultServerHost 默认服务端监听主机。
	DefaultServerHost = "0.0.0.0"
	// DefaultServerPort 默认服务端监听端口。
	DefaultServerPort = 9000
	// DefaultReadHeaderTimeout 默认读取请求头超时时间。
	DefaultReadHeaderTimeout = 10 * time.Second
	// DefaultStopTimeout 默认停止服务总截止时间。
	DefaultStopTimeout = 10 * time.Second
	// DefaultReadMaxBytes 默认读取请求体最大字节限制（128 KiB）。
	DefaultReadMaxBytes int64 = 131072
)

// ServerConfig 定义 HTTP 服务端运行配置。
type ServerConfig struct {
	Host                 string        `env:"SERVER_HOST" envDefault:"0.0.0.0"`
	Port                 int           `env:"SERVER_PORT" envDefault:"9000"`
	ReadHeaderTimeoutVal time.Duration `env:"SERVER_READ_HEADER_TIMEOUT" envDefault:"10s"`
	// StopTimeoutVal 定义应用停机的全局总截止时间（传给 Fx 容器的 StopTimeout）。
	StopTimeoutVal  time.Duration `env:"SERVER_STOP_TIMEOUT" envDefault:"10s"`
	ReadMaxBytesVal int64         `env:"SERVER_READ_MAX_BYTES" envDefault:"131072"`

	shutdownTimeoutVal time.Duration
}

// Addr 返回服务器监听地址（如 0.0.0.0:9000）。
func (s ServerConfig) Addr() string {
	return net.JoinHostPort(s.Host, strconv.Itoa(s.Port))
}

// Validate 校验服务端监听配置与端口范围合法性。
func (s ServerConfig) Validate() error {
	if s.Port < 1 || s.Port > 65535 {
		return fmt.Errorf("invalid server port %d: must be between 1 and 65535", s.Port)
	}
	return nil
}

// ReadHeaderTimeout 返回读取请求头超时时间。
func (s ServerConfig) ReadHeaderTimeout() time.Duration {
	return s.ReadHeaderTimeoutVal
}

// StopTimeout 返回应用停机的全局总截止时间。
func (s ServerConfig) StopTimeout() time.Duration {
	return s.StopTimeoutVal
}

// ShutdownTimeout 返回 HTTP 服务专属的停机预算上限。
// 若未显式设置，则动态折算为 StopTimeout 的 70%（s.StopTimeout() * 7 / 10）。
func (s ServerConfig) ShutdownTimeout() time.Duration {
	if s.shutdownTimeoutVal > 0 {
		return s.shutdownTimeoutVal
	}
	return (s.StopTimeout() * 7) / 10
}

// SetShutdownTimeoutForTest 用于测试中显式覆盖停机预算。
func (s *ServerConfig) SetShutdownTimeoutForTest(d time.Duration) {
	s.shutdownTimeoutVal = d
}

// ReadMaxBytes 返回读取请求体的最大字节数限制。若配置值 <= 0 则回退至默认 131072 字节。
func (s ServerConfig) ReadMaxBytes() int64 {
	if s.ReadMaxBytesVal <= 0 {
		return DefaultReadMaxBytes
	}
	return s.ReadMaxBytesVal
}
