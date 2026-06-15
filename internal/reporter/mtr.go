package reporter

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"time"
)

// executeMTRAndReport 独立执行 MTR 插件并上报结果
// 作为 WSClient 的方法，它可以直接复用 reporter.go 中的 c.send 通道
func (c *WSClient) executeMTRAndReport(target string) {
	pluginPath := "./plugins/mtr-plugin"
	if runtime.GOOS == "windows" {
		pluginPath = "./plugins/mtr-plugin.exe"
	}

	var resultData json.RawMessage

	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		errJSON := fmt.Sprintf(`{"target":"%s","error":"MTR plugin not installed on this agent"}`, target)
		resultData = json.RawMessage(errJSON)
	} else {
		cmd := exec.Command(pluginPath, "-target", target)
		output, err := cmd.Output()
		if err != nil {
			errJSON := fmt.Sprintf(`{"target":"%s","error":"Plugin execution failed or timed out"}`, target)
			resultData = json.RawMessage(errJSON)
		} else {
			resultData = json.RawMessage(output)
		}
	}

	params := map[string]interface{}{
		"target":    target,
		"timestamp": time.Now().Unix(),
		"result":    resultData,
	}

	c.send("mtr.report", params)
}