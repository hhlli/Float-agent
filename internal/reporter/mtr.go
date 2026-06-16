package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"Float-agent/internal/logger"
	"go.uber.org/zap"
)

const mtrExecutionTimeout = 45 * time.Second

// executeMTRAndReport 独立执行 MTR 插件并上报结果
func (c *WSClient) executeMTRAndReport(target string) {
	pluginPath := "./plugins/mtr-plugin"
	if runtime.GOOS == "windows" {
		pluginPath = "./plugins/mtr-plugin.exe"
	}

	var resultData json.RawMessage
	params := map[string]interface{}{
		"target":    target,
		"timestamp": time.Now().Unix(),
	}

	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		resultData = json.RawMessage(fmt.Sprintf(`{"target":"%s","error":"MTR 插件未安装"}`, target))
		params["result"] = resultData
		c.send("mtr.report", params)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), mtrExecutionTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, pluginPath, "-target", target)
	output, err := cmd.Output()

	if ctx.Err() == context.DeadlineExceeded {
		resultData = json.RawMessage(fmt.Sprintf(`{"target":"%s","error":"插件执行超时，子进程已被强制回收"}`, target))
	} else if err != nil {
		resultData = json.RawMessage(fmt.Sprintf(`{"target":"%s","error":"插件执行失败: %s"}`, target, err.Error()))
	} else {
		resultData = json.RawMessage(output)
	}

	params["result"] = resultData
	c.send("mtr.report", params)
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

	resp, err := http.Get(downloadURL)
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