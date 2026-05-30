package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"math"       // 🌟 新增：用于统计计算
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"       // 🌟 新增：用于分位数排序
	"strconv"
	"strings"
	"sync"
	"time"
	"runtime"

	"Float-agent/internal/collector"
	"Float-agent/internal/config"
	"Float-agent/internal/dockerproxy"
	"Float-agent/internal/reporter"
	"Float-agent/internal/logger"
	"go.uber.org/zap"
)

// 🌟 新增函数：用于探针获取自身所在网络的真实公网出口 IP
func getPublicIP() string {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("https://api.ipify.org?format=text")
	if err != nil {
		resp, err = client.Get("https://ifconfig.me/ip")
		if err != nil {
			return ""
		}
	}
	defer resp.Body.Close()
	ipBytes, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(ipBytes))
}

// ── 任务执行引擎 ─────────────────────────────────────────
var (
	pingRegex    = regexp.MustCompile(`time=([\d.]+) ms`)
	currentTasks []reporter.Task
	tasksMu      sync.RWMutex
)

// 🌟 新增：统计计算逻辑
func CalculateMetrics(latencies []float64, totalSent int) (avg, loss, jitter, p50, p99, minMs, maxMs float64) {
	if totalSent == 0 {
		return 0, 100, 0, 0, 0, 0, 0
	}

	received := len(latencies)
	loss = (float64(totalSent-received) / float64(totalSent)) * 100.0

	if received == 0 {
		return 0, loss, 0, 0, 0, 0, 0
	}

	var sum float64
	var jitterSum float64
	for i := 0; i < received; i++ {
		sum += latencies[i]
		if i > 0 {
			jitterSum += math.Abs(latencies[i] - latencies[i-1])
		}
	}
	avg = sum / float64(received)
	if received > 1 {
		jitter = jitterSum / float64(received-1)
	} else {
		jitter = 0
	}

	sorted := make([]float64, received)
	copy(sorted, latencies)
	sort.Float64s(sorted)

	minMs = sorted[0]
	maxMs = sorted[received-1]

	p50Idx := int(math.Ceil(float64(received)*0.50)) - 1
	p99Idx := int(math.Ceil(float64(received)*0.99)) - 1
	if p50Idx < 0 { p50Idx = 0 }
	if p99Idx < 0 { p99Idx = 0 }
	
	p50 = sorted[p50Idx]
	p99 = sorted[p99Idx]

	return avg, loss, jitter, p50, p99, minMs, maxMs
}

// 🌟 重构：将 executeSingleTask 改为 executeTaskBatch，支持连续发包
func executeTaskBatch(t reporter.Task, count int) ([]float64, int, string) {
	var latencies []float64
	status := "online"

	for i := 0; i < count; i++ {
		start := time.Now()
		var success bool
		var latency float64

		switch strings.ToUpper(t.Type) {
		case "TCP":
			target := strings.TrimSpace(t.Target)
			target = strings.TrimPrefix(target, "http://")
			target = strings.TrimPrefix(target, "https://")
			if !strings.Contains(target, ":") {
				target = target + ":80"
			}
			conn, err := net.DialTimeout("tcp", target, 3*time.Second)
			if err == nil {
				conn.Close()
				latency = float64(time.Since(start).Microseconds()) / 1000.0
				success = true
			}

		case "HTTP":
			target := strings.TrimSpace(t.Target)
			if !strings.HasPrefix(target, "http") {
				target = "http://" + target
			}
			client := &http.Client{Timeout: 3 * time.Second}
			resp, err := client.Get(target)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode < 400 {
					latency = float64(time.Since(start).Microseconds()) / 1000.0
					success = true
				}
			}

		case "ICMP", "PING":
			cmd := exec.Command("ping", "-c", "1", "-W", "2", strings.TrimSpace(t.Target))
			output, err := cmd.CombinedOutput()
			if err == nil {
				matches := pingRegex.FindStringSubmatch(string(output))
				if len(matches) > 1 {
					latency, _ = strconv.ParseFloat(matches[1], 64)
					success = true
				} else {
					latency = float64(time.Since(start).Microseconds()) / 1000.0
					success = true
				}
			}
		}

		if success {
			latencies = append(latencies, latency)
		}

		// 每次发包间隔 200ms
		if i < count-1 {
			time.Sleep(200 * time.Millisecond)
		}
	}

	if len(latencies) == 0 {
		status = "offline"
	} else if len(latencies) < count {
		status = "warning" // 部分丢包
	}

	return latencies, count, status
}

// 🌟 修改：使用重构后的发包逻辑，并上报全量数据
func startMonitorWorker(ws *reporter.WSClient, nodeID string) {
	logger.Log.Info("监测引擎已启动", zap.String("module", "Monitor"))
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
					// 设定单次任务发包数为 5 (可根据需要调整)
					packetCount := 5
					latencies, total, status := executeTaskBatch(task, packetCount)
					
					// 计算全部指标
					avg, loss, jitter, p50, p99, minMs, maxMs := CalculateMetrics(latencies, total)
					
					// 调用上一步你在 reporter.go 中修改过的方法
					ws.SendTaskResult(task.ID, nodeID, avg, loss, jitter, p50, p99, minMs, maxMs, status)
				}(t)
			}
		}
		tasksMu.RUnlock()
	}
}

// ── 主程序 ───────────────────────────────────────────────
func main() {
	// 注册 debug 参数，防止后续全局 flag.Parse() 时报未定义错误
	flag.Bool("debug", false, "Enable debug logging")

	// 手动提取 debug 标志以初始化日志，禁止在此处提早调用 flag.Parse()
	isDebug := false
	for _, arg := range os.Args {
		if arg == "-debug" || arg == "--debug" {
			isDebug = true
			break
		}
	}
	logger.Init(isDebug)
	// 🌟 跨平台重构：启动模式拦截。如果是 docker-proxy 模式，直接接管
	if len(os.Args) > 1 && os.Args[1] == "docker-proxy" {
		listenAddr := "127.0.0.1:23750"
		daemonSocket := ""

		// 1. 设置系统级别的默认 Socket 路径
		if runtime.GOOS == "windows" {
			daemonSocket = `\\.\pipe\docker_engine`
		} else {
			daemonSocket = "/var/run/docker.sock" // Linux 默认
		}

		// 2. 允许通过启动参数覆盖默认 Socket 路径 (针对 macOS 等特殊环境)
		for i, arg := range os.Args {
			if arg == "-listen" && i+1 < len(os.Args) {
				listenAddr = os.Args[i+1]
			}
			if arg == "-socket" && i+1 < len(os.Args) {
				daemonSocket = os.Args[i+1]
			}
		}

		// 3. 传入双参数启动代理
		if err := dockerproxy.Start(listenAddr, daemonSocket); err != nil {
			logger.Log.Error("Docker 代理启动失败", zap.Error(err))
			return
		}
		return // 阻断执行，防止代理模式下继续执行主探针逻辑
	} // 🌟 补上此处缺失的右大括号

	// === 以下为原主探针逻辑 ===

	// 基础参数
	nodeID := flag.String("i", "", "节点 ID (可选，优先于本地持久化缓存)")
	token := flag.String("t", "", "通信 Token (可选，可通过注册自动获取)")
	serverURL := flag.String("s", "http://localhost:8080", "服务端地址")
	registerToken := flag.String("register", "", "自动发现密钥 (用于首次注册自动分配 ID)") // 🌟 新增

	// 高级参数解析
	insecure := flag.Bool("insecure", false, "忽略不安全证书")
	noUpdate := flag.Bool("no-update", false, "禁用自动更新")
	includeBuffer := flag.Bool("include-buffer", false, "包含缓冲区内存")
	disableRPC := flag.Bool("disable-rpc", false, "禁用远程控制")
	netInclude := flag.String("net-include", "", "只监测特定网卡")
	netExclude := flag.String("net-exclude", "", "排除特定网卡")
	diskMounts := flag.String("disk-mounts", "/", "只监测特定挂载点")
	dockerEndpoint := flag.String("docker-endpoint", "", "Docker本地只读代理地址")
	dockerStats := flag.Bool("docker-stats", false, "是否采集容器详细资源")
	enableTerminal := flag.Bool("enable-terminal", false, "允许 Web 终端控制 (高风险)")

	flag.Parse()

	var finalNodeID string
	var finalToken = *token
	var cachedPublicIP string

	// 1. 尝试读取本地持久化的 NodeID 和 Token
	idFilePath := filepath.Join(filepath.Dir(os.Args[0]), ".node_id")
	if data, err := os.ReadFile(idFilePath); err == nil {
		content := strings.TrimSpace(string(data))
		parts := strings.SplitN(content, ",", 2)
		finalNodeID = parts[0]
		// 若文件内包含 Token 且启动参数未指定，则加载缓存 Token
		if len(parts) > 1 && finalToken == "" {
			finalToken = parts[1]
		}
	}

	// 2. 命令行显式传入的 ID 优先级最高
	if *nodeID != "" {
		finalNodeID = *nodeID
	}

	// 3. 核心触发器：本地无 ID，或者虽有 ID 但缺少 Token，且提供了注册密钥时执行注册
	if finalNodeID == "" || (finalToken == "" && *registerToken != "") {
		logger.Log.Info("满足自动发现条件，开始向服务端申请注册...")
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "Unknown-Node"
		}

		reqBody, _ := json.Marshal(map[string]string{
			"discovery_token": *registerToken,
			"hostname":        hostname,
			"public_ip":       getPublicIP(),
		})

		resp, err := http.Post(strings.TrimRight(*serverURL, "/")+"/agent/register", "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			logger.Log.Error("网络错误: 注册请求发送失败, 5秒后退出", zap.Error(err))
			time.Sleep(5 * time.Second)
			os.Exit(1)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			logger.Log.Error("注册被拒绝 (请检查数据库结构完整性或密钥)", zap.Int("status_code", resp.StatusCode))
			time.Sleep(5 * time.Second)
			os.Exit(1)
		}

		var resData struct {
			NodeID      string `json:"node_id"`
			ServerToken string `json:"server_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
			logger.Log.Error("解析注册响应数据失败", zap.Error(err))
			time.Sleep(5 * time.Second)
			os.Exit(1)
		}

		if resData.NodeID != "" && resData.ServerToken != "" {
			finalNodeID = resData.NodeID
			finalToken = resData.ServerToken

			// 写入本地持久化文件
			saveData := finalNodeID + "," + finalToken
			if err := os.WriteFile(idFilePath, []byte(saveData), 0644); err != nil {
				logger.Log.Warn("无法将配置持久化到本地文件", zap.Error(err))
			} else {
				logger.Log.Info("注册成功并持久化", zap.String("node_id", finalNodeID), zap.String("file_path", idFilePath))
			}
		}
	}

	// 4. 最终关卡校验
	if finalNodeID == "" || finalToken == "" {
		logger.Log.Error("节点未完成有效初始化: 必须提供通信 Token 或正确的注册密钥")
		time.Sleep(5 * time.Second)
		os.Exit(1)
	}

	// 5. 组装配置
	cfg := &config.Config{
		NodeID:         finalNodeID, // 🌟 使用处理后的 ID
		AuthToken:      finalToken,  // 🌟 使用处理后的 Token
		ServerURL:      *serverURL,
		Interval:       3 * time.Second,
		DiskPath:       *diskMounts,
		Insecure:       *insecure,
		NoUpdate:       *noUpdate,
		IncludeBuffer:  *includeBuffer,
		DisableRPC:     *disableRPC,
		NetInclude:     *netInclude,
		NetExclude:     *netExclude,
		DockerEndpoint: *dockerEndpoint,
		DockerStats:    *dockerStats,
		EnableTerminal: *enableTerminal,
	}

    logger.Log.Info("探针启动成功", 
	        zap.String("version", collector.ProbeVersion), 
	        zap.String("node_id", cfg.NodeID), 
	        zap.String("server", cfg.ServerURL),
    )
    
    if cfg.DockerEndpoint != "" {
        logger.Log.Info("已启用 Docker 监控", 
	        zap.String("module", "Docker"), 
	        zap.String("endpoint", cfg.DockerEndpoint), 
	        zap.Bool("detailed_stats", cfg.DockerStats),
    )
        
        // 启动独立的 Docker 后台采集协程
        interval := 5 * time.Second
        if cfg.DockerStats {
            interval = 15 * time.Second
        }
        go collector.StartDockerCollector(cfg.DockerEndpoint, interval, cfg.DockerStats)
    }

	// === 修复此处：确保缓存变量在任何启动模式下都能拿到公网 IP ===
	if cachedPublicIP == "" {
		cachedPublicIP = getPublicIP()
	}
	
	// 创建 WebSocket 客户端
	ws := reporter.NewWSClient(cfg.ServerURL, cfg.NodeID, cfg.AuthToken, cfg.Insecure)

	if !cfg.DisableRPC {
		ws.OnTasks = func(tasks []reporter.Task) {
			tasksMu.Lock()
			currentTasks = tasks
			tasksMu.Unlock()
			logger.Log.Info("收到任务更新", zap.String("module", "WS"), zap.Int("task_count", len(tasks)))
		}
		go startMonitorWorker(ws, cfg.NodeID)
	} else {
		logger.Log.Info("已开启 --disable-rpc: 禁用远程控制")
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
			logger.Log.Debug("等待重连...")
			continue
		}

		// 采集数据
		metric, err := collector.Collect(cfg)
		if err != nil {
			logger.Log.Error("指标采集失败", zap.Error(err))
			continue
		}

		// 上报数据
		tasks, err := ws.Report(metric)
		if err != nil {
			logger.Log.Error("上报失败", zap.Error(err))
			continue
		}

		if !cfg.DisableRPC && len(tasks) > 0 {
			tasksMu.Lock()
			currentTasks = tasks
			tasksMu.Unlock()
		}
	}
}