package terminal

import (
	"encoding/json"
	"os/exec"
	"runtime"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
	"Float-agent/internal/logger"
	"go.uber.org/zap"
)

// ResizeMsg 定义前端调整窗口大小的结构
type ResizeMsg struct {
	Type string `json:"type"`
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

// Start 启动一个 PTY 会话并与 WebSocket 绑定
func Start(ws *websocket.Conn) {
	defer ws.Close()

	if runtime.GOOS == "windows" {
		// ─── Windows 环境：轻量级管道 (强制 UTF-8) ───
		// 启动一个隐藏 chcp 提示的 UTF-8 cmd 进程
		cmd := exec.Command("cmd.exe", "/k", "@chcp 65001 >nul")

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			logger.Log.Error("获取 stdout 失败", 
				zap.String("module", "Terminal"), 
				zap.Error(err),
			)
			return
		}
		
		stdin, err := cmd.StdinPipe()
		if err != nil {
			logger.Log.Error("获取 stdin 失败", 
				zap.String("module", "Terminal"), 
				zap.Error(err),
			)
			return
		}
		cmd.Stderr = cmd.Stdout
		
		if err := cmd.Start(); err != nil {
			logger.Log.Error("Windows CMD 启动失败", 
				zap.String("module", "Terminal"), 
				zap.Error(err),
			)
			return
		}
		defer func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}()

		// 1. 读取 CMD 输出 (已被强制为 UTF-8) -> 写入 WebSocket
		go func() {
			buf := make([]byte, 4096)
			for {
				n, err := stdout.Read(buf)
				if err != nil {
					return
				}
				if err := ws.WriteMessage(websocket.BinaryMessage, buf[:n]); err != nil {
					return
				}
			}
		}()

		// 2. 读取 WebSocket 输入 (UTF-8 流) -> 写入 CMD
		for {
			msgType, p, err := ws.ReadMessage()
			if err != nil {
				break
			}

			// 忽略 Resize 指令，因为标准管道不支持调整终端大小
			if msgType == websocket.TextMessage && len(p) > 0 && p[0] == '{' {
				var resizeMsg ResizeMsg
				if err := json.Unmarshal(p, &resizeMsg); err == nil && resizeMsg.Type == "resize" {
					continue
				}
			}

			_, err = stdin.Write(p)
			if err != nil {
				break
			}
		}

	} else {
		// ─── macOS / Linux 环境：使用 pty 设备 ───
		shell := "bash"
		if runtime.GOOS == "darwin" {
			shell = "zsh"
		}

		if _, err := exec.LookPath(shell); err != nil {
			shell = "sh"
		}
		cmd := exec.Command(shell)

		ptmx, err := pty.Start(cmd)
		if err != nil {
			logger.Log.Error("PTY 启动失败", 
				zap.String("module", "Terminal"), 
				zap.Error(err),
			)
			return
		}
		defer func() {
			_ = ptmx.Close()
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}()

		// 1. 将 PTY 的输出直接写入 WebSocket (Linux/macOS 原生 UTF-8)
		go func() {
			buf := make([]byte, 4096)
			for {
				n, err := ptmx.Read(buf)
				if err != nil {
					return
				}
				if err := ws.WriteMessage(websocket.BinaryMessage, buf[:n]); err != nil {
					return
				}
			}
		}()

		// 2. 将 WebSocket 的输入直接写入 PTY
		for {
			msgType, p, err := ws.ReadMessage()
			if err != nil {
				break
			}

			// 处理窗口 Resize 指令
			if msgType == websocket.TextMessage && len(p) > 0 && p[0] == '{' {
				var resizeMsg ResizeMsg
				if err := json.Unmarshal(p, &resizeMsg); err == nil && resizeMsg.Type == "resize" {
					pty.Setsize(ptmx, &pty.Winsize{
						Rows: resizeMsg.Rows,
						Cols: resizeMsg.Cols,
					})
					continue
				}
			}

			_, err = ptmx.Write(p)
			if err != nil {
				break
			}
		}
	}
}