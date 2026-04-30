package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"Float-agent/internal/collector"
	"Float-agent/internal/config"
	"Float-agent/internal/reporter"
)

// ── 任务执行引擎 ─────────────────────────────────────────
var (
	pingRegex    = regexp.MustCompile(`time=([\d.]+) ms`)
	currentTasks []reporter.Task
	tasksMu      sync.RWMutex
)

func executeSingleTask(t reporter.Task) float64 {
	start := time.Now()

	switch strings.ToUpper(t.Type) {
	case "TCP":
		target := strings.TrimSpace(t.Target)
		target = strings.TrimPrefix(target, "http://")
		target = strings.TrimPrefix(target, "https://")

		if !strings.Contains(target, ":") {
			target = target + ":80"
		}

		log.Printf("[DEBUG-TCP] 开始测速 -> 目标: '%s'", target)

		conn, err := net.DialTimeout("tcp", target, 5*time.Second)
		if err != nil {
			log.Printf("[ERROR-TCP] 拨号失败 -> 目标: '%s', 详细原因: %v", target, err)
			return -1
		}
		conn.Close()

		latency := float64(time.Since(start).Microseconds()) / 1000.0
		log.Printf("[SUCCESS-TCP] 测速成功 -> 目标: '%s', 延迟: %.2f ms", target, latency)
		return latency

	case "HTTP":
		target := strings.TrimSpace(t.Target)
		if !strings.HasPrefix(target, "http") {
			target = "http://" + target
		}
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(target)
		if err != nil {
			return -1
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			return -1
		}
		return float64(time.Since(start).Microseconds()) / 1000.0

	case "ICMP", "PING":
		cmd := exec.Command("ping", "-c", "1", "-W", "2", strings.TrimSpace(t.Target))
		output, err := cmd.CombinedOutput()
		if err != nil {
			return -1
		}
		matches := pingRegex.FindStringSubmatch(string(output))
		if len(matches) > 1 {
			latency, _ := strconv.ParseFloat(matches[1], 64)
			return latency
		}
		return float64(time.Since(start).Microseconds()) / 1000.0
	}

	return -1
}

func startMonitorWorker(ws *reporter.WSClient, nodeID string) {
	log.Println("[监测引擎] 已启动")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		tasksMu.RLock()
		if len(currentTasks) == 0 {
			tasksMu.RUnlock()
			continue
		}

		now := time.Now().Unix()
		for _, t := range currentTasks {
			interval := int64(t.Interval)
			if interval <= 0 {
				interval = 60
			}
			if now%interval == 0 {
				go func(task reporter.Task) {
					pingMs := executeSingleTask(task)
					if pingMs >= 0 {
						ws.SendTaskResult(task.ID, nodeID, pingMs)
					}
				}(t)
			}
		}
		tasksMu.RUnlock()
	}
}

// ── NodeID 管理 ──────────────────────────────────────────
func getLocalNodeID() string {
	idFile := ".node_id"
	if data, err := os.ReadFile(idFile); err == nil && len(data) > 0 {
		return string(data)
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "linux-server"
	}
	newID := fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()%10000)
	_ = os.WriteFile(idFile, []byte(newID), 0644)
	return newID
}

// ── 主程序 ───────────────────────────────────────────────
func main() {
	// 基础参数
	nodeID := flag.String("i", "", "节点 ID")
	token := flag.String("t", "", "通信 Token (必填)")
	serverURL := flag.String("s", "http://localhost:8080", "服务端地址")

	// 🌟 新增高级参数解析
	insecure := flag.Bool("insecure", false, "忽略不安全证书")
	noUpdate := flag.Bool("no-update", false, "禁用自动更新")
	includeBuffer := flag.Bool("include-buffer", false, "包含缓冲区内存")
	disableRPC := flag.Bool("disable-rpc", false, "禁用远程控制")
	netInclude := flag.String("net-include", "", "只监测特定网卡")
	netExclude := flag.String("net-exclude", "", "排除特定网卡")
	diskMounts := flag.String("disk-mounts", "/", "只监测特定挂载点")

	flag.Parse()

	if *token == "" {
		log.Fatal("错误: 必须提供通信 Token (-t)")
	}

	finalNodeID := *nodeID
	if finalNodeID == "" {
		finalNodeID = getLocalNodeID()
	}

	// 组装配置
	cfg := &config.Config{
		NodeID:        finalNodeID,
		AuthToken:     *token,
		ServerURL:     *serverURL,
		Interval:      5 * time.Second,
		DiskPath:      *diskMounts, // 🌟 挂载点直接覆盖原来写死的 "/"
		Insecure:      *insecure,
		NoUpdate:      *noUpdate,
		IncludeBuffer: *includeBuffer,
		DisableRPC:    *disableRPC,
		NetInclude:    *netInclude,
		NetExclude:    *netExclude,
	}

	log.Printf("探针启动成功 [v%s] | NodeID: %s | Server: %s\n", collector.ProbeVersion, cfg.NodeID, cfg.ServerURL)

	// 创建 WebSocket 客户端
	ws := reporter.NewWSClient(cfg.ServerURL, cfg.NodeID, cfg.AuthToken, cfg.Insecure)

	// 🌟 拦截功能：如果不禁用远程控制，才启动接收与测速任务
	if !cfg.DisableRPC {
		ws.OnTasks = func(tasks []reporter.Task) {
			tasksMu.Lock()
			currentTasks = tasks
			tasksMu.Unlock()
			log.Printf("[WS] 收到任务更新，共 %d 个任务\n", len(tasks))
		}
		go startMonitorWorker(ws, cfg.NodeID)
	} else {
		log.Println("[INFO] 已开启 --disable-rpc：禁用远程控制，本机拒绝执行测速下发任务")
	}

	// 自动重连（后台运行）
	go ws.ConnectWithRetry()

	// 等待连接就绪
	for !ws.IsConnected() {
		time.Sleep(500 * time.Millisecond)
	}

	// 主循环：定时采集并上报
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for range ticker.C {
		if !ws.IsConnected() {
			log.Println("等待重连...")
			continue
		}

		// 采集数据
		metric, err := collector.Collect(cfg)
		if err != nil {
			log.Println("指标采集失败:", err)
			continue
		}

		// 上报数据
		tasks, err := ws.Report(metric)
		if err != nil {
			log.Println("上报失败:", err)
			continue
		}

		// 🌟 如果禁用了 RPC，就不要更新并执行服务端下发的任务列表
		if !cfg.DisableRPC && len(tasks) > 0 {
			tasksMu.Lock()
			currentTasks = tasks
			tasksMu.Unlock()
		}
	}
}