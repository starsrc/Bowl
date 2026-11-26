package config

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
)

type ServerConfig struct {
	ServerHost              string        `env:"SERVER_HOST" envDefault:"0.0.0.0"`
	ServerPort              int           `env:"SERVER_PORT" envDefault:"8080"`
	InternalServerHost      string        `env:"INTERNAL_SERVER_HOST" envDefault:"0.0.0.0"`
	InternalServerPort      int           `env:"INTERNAL_SERVER_PORT" envDefault:"8090"`
	ServerReadHeaderTimeout time.Duration `env:"SERVER_READ_HEADER_TIMEOUT" envDefault:"10s"`
	ServerStopTimeout       time.Duration `env:"SERVER_STOP_TIMEOUT" envDefault:"10s"`
	ServerReadMaxBytes      int64         `env:"SERVER_READ_MAX_BYTES" envDefault:"131072"`
}

func (c ServerConfig) ServerAddress() string {
	return net.JoinHostPort(c.ServerHost, strconv.Itoa(c.ServerPort))
}

func (c ServerConfig) InternalServerAddress() string {
	return net.JoinHostPort(c.InternalServerHost, strconv.Itoa(c.InternalServerPort))
}

func (c ServerConfig) Addr() string {
	return c.ServerAddress()
}

func (c ServerConfig) InternalAddr() string {
	return c.InternalServerAddress()
}

func (c ServerConfig) Port() int {
	return c.ServerPort
}

func (c *ServerConfig) ApplyDefaultPort(defaultPort int) {
	if c == nil {
		return
	}
	envVal, envSet := os.LookupEnv("SERVER_PORT")
	if (!envSet || strings.TrimSpace(envVal) == "") && (c.ServerPort == 0 || c.ServerPort == 8080) {
		c.ServerPort = defaultPort
	}
}

func (c ServerConfig) WithDefaultPort(defaultPort int) ServerConfig {
	c.ApplyDefaultPort(defaultPort)
	return c
}

func (c ServerConfig) ReadHeaderTimeout() time.Duration {
	if c.ServerReadHeaderTimeout <= 0 {
		return 10 * time.Second
	}
	return c.ServerReadHeaderTimeout
}

func (c ServerConfig) StopTimeout() time.Duration {
	if c.ServerStopTimeout <= 0 {
		return 10 * time.Second
	}
	return c.ServerStopTimeout
}

func (c ServerConfig) ShutdownTimeout() time.Duration {
	return (c.StopTimeout() * 7) / 10
}

func (c ServerConfig) ReadMaxBytes() int64 {
	if c.ServerReadMaxBytes <= 0 {
		return 128 << 10
	}
	return c.ServerReadMaxBytes
}

func (c ServerConfig) Validate() error {
	return c.ValidateSinglePort()
}

func (c ServerConfig) ValidateSinglePort() error {
	if c.ServerPort < 1 || c.ServerPort > 65535 {
		return fmt.Errorf("invalid SERVER_PORT %d: must be between 1 and 65535", c.ServerPort)
	}
	return nil
}

func isHostConflict(h1, h2 string) bool {
	h1 = strings.Trim(strings.TrimSpace(h1), "[]")
	h2 = strings.Trim(strings.TrimSpace(h2), "[]")

	if h1 == "" || h2 == "" {
		return true
	}

	addr1, err1 := netip.ParseAddr(h1)
	addr2, err2 := netip.ParseAddr(h2)

	if (err1 == nil && addr1.IsUnspecified()) || (err2 == nil && addr2.IsUnspecified()) {
		return true
	}

	if err1 == nil && err2 == nil {
		return addr1.Unmap() == addr2.Unmap()
	}

	return strings.EqualFold(h1, h2)
}

func (c ServerConfig) ValidateDualPorts() error {
	if c.ServerPort < 1 || c.ServerPort > 65535 {
		return fmt.Errorf("invalid SERVER_PORT %d: must be between 1 and 65535", c.ServerPort)
	}
	if c.InternalServerPort < 1 || c.InternalServerPort > 65535 {
		return fmt.Errorf("invalid INTERNAL_SERVER_PORT %d: must be between 1 and 65535", c.InternalServerPort)
	}

	if c.ServerPort == c.InternalServerPort && isHostConflict(c.ServerHost, c.InternalServerHost) {
		return fmt.Errorf("public and internal server addresses conflict: %s vs %s", c.ServerAddress(), c.InternalServerAddress())
	}

	return nil
}
