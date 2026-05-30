// internal/dockerproxy/proxy.go
package dockerproxy

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"strings"
)

func Start(listenAddr string, daemonSocket string) error {
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = "docker"
		},
		Transport: &http.Transport{
			// 🌟 将底层的拨号逻辑抽离为一个单独的函数 getDialer
			DialContext: getDialer(daemonSocket),
		},
	}

	secureHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		allowedPaths := []string{"/containers/json", "/containers/"}
		isAllowed := false
		for _, p := range allowedPaths {
			if strings.Contains(r.URL.Path, p) {
				isAllowed = true
				break
			}
		}

		if !isAllowed {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		proxy.ServeHTTP(w, r)
	})

	fmt.Printf("Docker Proxy Listening at: %s (Target: %s)\n", listenAddr, daemonSocket)
	return http.ListenAndServe(listenAddr, secureHandler)
}