package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"Float-agent/internal/logger"
	"go.uber.org/zap"
)

// executePluginAndReport 独立执行通用插件并上报结果
func (c *WSClient) executePluginAndReport(taskID int64, extID string, args []string) {
	// 开启异步协程，防止阻塞 WebSocket 导致心跳超时断连
	go func() {
		pluginPath := fmt.Sprintf("./plugins/%s", extID)
		if runtime.GOOS == "windows" {
			pluginPath += ".exe"
		}

		var resultData json.RawMessage

		if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
			resultData = json.RawMessage(fmt.Sprintf(`{"error":"插件 %s 未安装"}`, extID))
			c.sendPluginResponseWS(taskID, extID, resultData)
			return
		}

		// 默认 60 秒全局超时
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, pluginPath, args...)
		// 泛化架构下，同时捕获标准输出和标准错误
		output, err := cmd.CombinedOutput()

		if ctx.Err() == context.DeadlineExceeded {
			resultData = json.RawMessage(`{"error":"插件执行超时，子进程已被强制回收"}`)
		} else if err != nil {
			// 若执行发生系统级错误，尝试解析已有的输出，否则封装为通用错误格式
			if len(output) > 0 && json.Valid(output) {
				resultData = json.RawMessage(output)
			} else {
				safeErr, _ := json.Marshal(fmt.Sprintf("执行失败: %v | 详情: %s", err, string(output)))
				resultData = json.RawMessage(fmt.Sprintf(`{"error":%s}`, string(safeErr)))
			}
		} else {
			// 若执行成功，若本身是标准 JSON 则透传，否则装入 raw_output 中
			if len(output) > 0 && json.Valid(output) {
				resultData = json.RawMessage(output)
			} else {
				safeOutput, _ := json.Marshal(string(output))
				resultData = json.RawMessage(fmt.Sprintf(`{"raw_output":%s}`, string(safeOutput)))
			}
		}

		c.sendPluginResponseWS(taskID, extID, resultData)
	}()
}

// 辅助方法：封装发送逻辑以保持代码整洁
func (c *WSClient) sendPluginResponseWS(taskID int64, extID string, resultData json.RawMessage) {
	payload := map[string]interface{}{
		"node_id":   c.nodeID,
		"ext_id":    extID,
		"timestamp": time.Now().Unix(),
		"result":    resultData,
	}

	responsePayload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      taskID,
		"result":  payload,
	}

	c.connMu.Lock()
	if c.conn != nil {
		_ = c.conn.WriteJSON(responsePayload)
	}
	c.connMu.Unlock()
}

// handleExtensionInstallation 负责探针端插件的自动化生命周期部署
func (c *WSClient) handleExtensionInstallation(taskID int64, extID, downloadURL string, requirePrivilege bool) {
	logger.Log.Info("开始部署插件", zap.String("ext_id", extID))

	pluginDir := "./plugins"
	if err := os.MkdirAll(pluginDir, 0755); err != nil {
		logger.Log.Error("创建插件目录失败", zap.Error(err))
		c.sendExtensionResponseWS(taskID, "install", extID, fmt.Errorf("创建插件目录失败: %v", err))
		return
	}

	fileName := extID
	if runtime.GOOS == "windows" {
		fileName += ".exe"
	}

	pluginPath := filepath.Join(pluginDir, fileName)
	tmpPath := pluginPath + ".tmp"

    // 动态拼接带有操作系统和架构的真实下载地址
	finalDownloadURL := fmt.Sprintf("%s-%s-%s", downloadURL, runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		finalDownloadURL += ".exe"
	}

	// Linux 下的防掉权保护 (仅对需要特权的插件生效)
	if runtime.GOOS == "linux" && os.Geteuid() != 0 && requirePrivilege {
		if stat, err := os.Stat(pluginPath); err == nil && !stat.IsDir() {
			logger.Log.Warn("探针非 Root 权限运行，已阻断自动覆写操作以保护底层特权", zap.String("path", pluginPath))
			c.sendExtensionResponseWS(taskID, "install", extID, fmt.Errorf("探针无 Root 权限，无法自动部署涉及底层网络的特权插件"))
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
	}
	
	resp, err := httpClient.Get(finalDownloadURL)
	if err != nil {
		logger.Log.Error("下载请求失败", zap.Error(err))
		c.sendExtensionResponseWS(taskID, "install", extID, fmt.Errorf("下载网络请求失败: %v", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Log.Error("下载失败", zap.Int("status", resp.StatusCode))
		c.sendExtensionResponseWS(taskID, "install", extID, fmt.Errorf("镜像源返回错误码: %d", resp.StatusCode))
		return
	}

	out, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		logger.Log.Error("创建临时文件失败", zap.Error(err))
		c.sendExtensionResponseWS(taskID, "install", extID, fmt.Errorf("磁盘写入拒绝: %v", err))
		return
	}

	_, err = io.Copy(out, resp.Body)
	out.Close() // 强制提前关闭文件描述符

	if err != nil {
		os.Remove(tmpPath)
		logger.Log.Error("写入数据流失败，已清理临时文件", zap.Error(err))
		c.sendExtensionResponseWS(taskID, "install", extID, fmt.Errorf("文件数据流损坏: %v", err))
		return
	}

	if err := os.Rename(tmpPath, pluginPath); err != nil {
		os.Remove(tmpPath)
		logger.Log.Error("原子部署替换失败", zap.Error(err))
		c.sendExtensionResponseWS(taskID, "install", extID, fmt.Errorf("重命名覆盖失败: %v", err))
		return
	}

	// 根据下发参数决定是否执行特权赋予
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		os.Chmod(pluginPath, 0755)
		if requirePrivilege {
			cmd := exec.Command("setcap", "cap_net_raw+ep", pluginPath)
			if err := cmd.Run(); err == nil {
				logger.Log.Info("已自动为新部署的插件赋予 cap_net_raw 权限")
			}
		}
	}

	logger.Log.Info("插件部署完成", zap.String("path", pluginPath))
	c.sendExtensionResponseWS(taskID, "install", extID, nil)
}

// handleExtensionUninstallation 负责探针端插件的卸载清理
func (c *WSClient) handleExtensionUninstallation(taskID int64, extID string) {
	pluginPath := fmt.Sprintf("./plugins/%s", extID)
	if runtime.GOOS == "windows" {
		pluginPath += ".exe"
	}

	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		logger.Log.Info("插件文件不存在，无需卸载", zap.String("ext_id", extID))
		c.sendExtensionResponseWS(taskID, "uninstall", extID, nil)
		return
	}

	if err := os.Remove(pluginPath); err != nil {
		logger.Log.Error("卸载插件失败", zap.String("ext_id", extID), zap.Error(err))
		c.sendExtensionResponseWS(taskID, "uninstall", extID, fmt.Errorf("删除文件失败: %v", err))
		return
	}
	pluginDir := filepath.Dir(pluginPath)
if entries, err := os.ReadDir(pluginDir); err == nil && len(entries) == 0 {
    os.Remove(pluginDir) // 仅当目录为空时才会删除成功
}

	logger.Log.Info("插件已卸载", zap.String("ext_id", extID), zap.String("path", pluginPath))
	c.sendExtensionResponseWS(taskID, "uninstall", extID, nil)
}

// 辅助方法：发送插件操作的 WebSocket 响应
func (c *WSClient) sendExtensionResponseWS(taskID int64, action, extID string, err error) {
	var responsePayload map[string]interface{}

	if err != nil {
		responsePayload = map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      taskID,
			"error": map[string]interface{}{
				"code":    -32000,
				"message": err.Error(),
			},
		}
	} else {
		responsePayload = map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      taskID,
			"result": map[string]interface{}{
				"node_id": c.nodeID,
				"action":  action,
				"ext_id":  extID,
				"status":  "success",
			},
		}
	}

	c.connMu.Lock()
	if c.conn != nil {
		_ = c.conn.WriteJSON(responsePayload)
	}
	c.connMu.Unlock()
}