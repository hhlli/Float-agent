// internal/dockerproxy/dial_unix.go
//go:build !windows

package dockerproxy

import (
	"context"
	"net"
)

// getDialer 供 proxy.go 调用
func getDialer(daemonSocket string) func(context.Context, string, string) (net.Conn, error) {
	return func(_ context.Context, _, _ string) (net.Conn, error) {
		if daemonSocket == "" {
			daemonSocket = "/var/run/docker.sock"
		}
		return net.Dial("unix", daemonSocket)
	}
}