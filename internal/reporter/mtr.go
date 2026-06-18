package reporter

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"Float-agent/internal/logger"
	"go.uber.org/zap"
)

const mtrExecutionTimeout = 60 * time.Second

// executeMTRAndReport 独立执行 MTR 插件并上报结果
func (c *WSClient) executeMTRAndReport(target string) {
	// 开启异步协程，防止阻塞 WebSocket 导致心跳超时断连
	go func(target string) {
		pluginPath := "./plugins/mtr-plugin"
		if runtime.GOOS == "windows" {
			pluginPath = "./plugins/mtr-plugin.exe"
		}

		var resultData json.RawMessage

		if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
			resultData = json.RawMessage(fmt.Sprintf(`{"target":"%s","error":"MTR 插件未安装"}`, target))
			c.reportMTRResultViaHTTP(target, resultData)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), mtrExecutionTimeout)
		defer cancel()

		cmd := exec.CommandContext(ctx, pluginPath, "-target", target)
		output, err := cmd.Output()

		if ctx.Err() == context.DeadlineExceeded {
			resultData = json.RawMessage(fmt.Sprintf(`{"target":"%s","error":"插件执行超时，子进程已被强制回收"}`, target))
		} else if err != nil {
			if len(output) > 0 && json.Valid(output) {
				resultData = json.RawMessage(output)
			} else {
				safeErr, _ := json.Marshal(fmt.Sprintf("执行失败: %v | 详情: %s", err, string(output)))
				resultData = json.RawMessage(fmt.Sprintf(`{"target":"%s","error":%s}`, target, string(safeErr)))
			}
		} else {
			if len(output) > 0 && json.Valid(output) {
				resultData = json.RawMessage(output)
			} else {
				safeOutput, _ := json.Marshal(string(output))
				resultData = json.RawMessage(fmt.Sprintf(`{"target":"%s","error":%s}`, target, string(safeOutput)))
			}
		}

		c.reportMTRResultViaHTTP(target, resultData)
	}(target)
}

// 独立 HTTP 上报方法
func (c *WSClient) reportMTRResultViaHTTP(target string, resultData json.RawMessage) {
	payload := map[string]interface{}{
		"node_id":   c.nodeID,
		"target":    target,
		"timestamp": time.Now().Unix(),
		"result":    resultData,
	}
	body, _ := json.Marshal(payload)

	// 拼接服务端 HTTP 接收接口
	apiEndpoint := strings.TrimRight(c.serverURL, "/") + "/api/agent/mtr/report"

	req, err := http.NewRequest("POST", apiEndpoint, bytes.NewBuffer(body))
	if err != nil {
		logger.Log.Error("构建 MTR 上报请求失败", zap.Error(err))
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	// 如果探针配置了忽略证书，HTTP 客户端同步保持忽略
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if c.insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Log.Error("MTR 结果 HTTP 上报失败", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Log.Error("MTR 上报被服务端拒绝", zap.Int("status", resp.StatusCode))
	} else {
		logger.Log.Info("MTR 结果 HTTP 上报成功", zap.String("target", target))
	}
}

// handleExtensionInstallation 负责探针端插件的自动化生命周期部署
func (c *WSClient) handleExtensionInstallation(extID string) {
	if extID != "mtr-plugin" {
		return
	}

	logger.Log.Info("开始部署插件", zap.String("ext_id", extID))

	pluginDir := "./plugins"
	if err := os.MkdirAll(pluginDir, 0755); err != nil {
		logger.Log.Error("创建插件目录失败", zap.Error(err))
		return
	}

	fileName := "mtr-plugin"
	downloadName := fmt.Sprintf("float-mtr-plugin-%s-%s", runtime.GOOS, runtime.GOARCH)

	if runtime.GOOS == "windows" {
		fileName += ".exe"
		downloadName += ".exe"
	}

	pluginPath := filepath.Join(pluginDir, fileName)
	tmpPath := pluginPath + ".tmp"
	downloadURL := fmt.Sprintf("https://github.com/hhlli/float-mtr-plugin/releases/latest/download/%s", downloadName)

	// Linux 下的防掉权保护
	if runtime.GOOS == "linux" && os.Geteuid() != 0 {
		if stat, err := os.Stat(pluginPath); err == nil && !stat.IsDir() {
			logger.Log.Warn("探针非 Root 权限运行，已阻断自动覆写操作以保护底层特权。请手动部署。", zap.String("path", pluginPath))
			return
		}
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 30 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
		},
		// 不设 Timeout，让 body 下载不受总时限约束
	}
	resp, err := httpClient.Get(downloadURL)
	if err != nil {
		logger.Log.Error("下载请求失败", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Log.Error("下载失败", zap.Int("status", resp.StatusCode))
		return
	}

	out, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		logger.Log.Error("创建临时文件失败", zap.Error(err))
		return
	}

	_, err = io.Copy(out, resp.Body)
	out.Close() // 强制提前关闭文件描述符

	if err != nil {
		os.Remove(tmpPath)
		logger.Log.Error("写入数据流失败，已清理临时文件", zap.Error(err))
		return
	}

	if err := os.Rename(tmpPath, pluginPath); err != nil {
		os.Remove(tmpPath)
		logger.Log.Error("原子部署替换失败", zap.Error(err))
		return
	}

	// Linux Root 环境下自动赋权
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		os.Chmod(pluginPath, 0755)
		cmd := exec.Command("setcap", "cap_net_raw+ep", pluginPath)
		if err := cmd.Run(); err == nil {
			logger.Log.Info("已自动为新部署的插件赋予 cap_net_raw 权限")
		}
	}

	logger.Log.Info("插件部署完成", zap.String("path", pluginPath))
}

// handleExtensionUninstallation 负责探针端插件的卸载清理
func (c *WSClient) handleExtensionUninstallation(extID string) {
	if extID != "mtr-plugin" {
		return
	}

	pluginPath := "./plugins/mtr-plugin"
	if runtime.GOOS == "windows" {
		pluginPath = "./plugins/mtr-plugin.exe"
	}

	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		logger.Log.Info("插件文件不存在，无需卸载", zap.String("ext_id", extID))
		return
	}

	if err := os.Remove(pluginPath); err != nil {
		logger.Log.Error("卸载插件失败", zap.String("ext_id", extID), zap.Error(err))
		return
	}

	logger.Log.Info("插件已卸载", zap.String("ext_id", extID), zap.String("path", pluginPath))
}