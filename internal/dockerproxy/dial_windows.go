// internal/dockerproxy/dial_windows.go
//go:build windows

package dockerproxy

import (
	"context"
	"net"

	"github.com/Microsoft/go-winio"
)

// getDialer 供 proxy.go 调用
func getDialer(daemonSocket string) func(context.Context, string, string) (net.Conn, error) {
	return func(_ context.Context, _, _ string) (net.Conn, error) {
		if daemonSocket == "" {
			daemonSocket = `\\.\pipe\docker_engine`
		}
		return winio.DialPipe(daemonSocket, nil)
	}
}