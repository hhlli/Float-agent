package collector

import (
	"fmt"
	"math"
	"strings"
	"time"
	stdnet "net"

	"Float-agent/internal/config" // 引入 config

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

var ProbeVersion = "dev"

type Metric struct {
	Timestamp    int64  `json:"timestamp"`
	AgentVersion string `json:"agent_version"`
	IPv4         string `json:"ipv4"`
	IPv6         string `json:"ipv6"`

	// 基础资源
	CPU       float64 `json:"cpu"`
	Mem       float64 `json:"mem"`
	MemUsed   float64 `json:"mem_used"`
	MemTotal  float64 `json:"mem_total"`
	SwapUsed  float64 `json:"swap_used"`
	SwapTotal float64 `json:"swap_total"`

	Disk      float64 `json:"disk"`
	DiskUsed  float64 `json:"disk_used"`
	DiskTotal float64 `json:"disk_total"`

	// 系统信息
	Uptime   int64  `json:"uptime"`
	OS       string `json:"os"`
	Kernel   string `json:"kernel"`
	Arch     string `json:"arch"`
	Virt     string `json:"virt"`
	CPUModel string `json:"cpu_model"`

	// 网络
	NetRxSpeed float64 `json:"net_rx_speed"`
	NetTxSpeed float64 `json:"net_tx_speed"`
	NetRxTotal float64 `json:"net_rx_total"`
	NetTxTotal float64 `json:"net_tx_total"`

	TCPConn int `json:"tcp_conn"`
	UDPConn int `json:"udp_conn"`

	// 运行状态
	Processes int     `json:"processes"`
	Load1     float64 `json:"load_1"`
	Load5     float64 `json:"load_5"`
	Load15    float64 `json:"load_15"`
	// 新增 Docker 字段
	DockerContainers []DockerContainer `json:"docker_containers,omitempty"`
	// 新增字段：标识探针是否允许远程控制
	TerminalEnabled  bool        `json:"terminal_enabled"`
}

var (
	lastRx   float64
	lastTx   float64
	lastTime time.Time
	firstRun = true
)

// 🌟 修改：接收完整的 cfg 配置对象
func Collect(cfg *config.Config) (*Metric, error) {
	ipv4, ipv6 := getIPs()
	m := &Metric{
		Timestamp:    time.Now().Unix(),
		AgentVersion: ProbeVersion,
		IPv4:         ipv4,
		IPv6:         ipv6,
	}

	// 1. CPU
	cpuPercent, err := cpu.Percent(0, false)
	if err == nil && len(cpuPercent) > 0 {
		m.CPU = cpuPercent[0]
	}

	cpuInfo, err := cpu.Info()
	if err == nil && len(cpuInfo) > 0 {
		m.CPUModel = fmt.Sprintf("%s (x%d)", cpuInfo[0].ModelName, len(cpuInfo))
	}

	// 2. 内存
	vm, err := mem.VirtualMemory()
	if err == nil {
		m.MemTotal = float64(vm.Total)
		m.SwapUsed = float64(vm.SwapTotal - vm.SwapFree)
		m.SwapTotal = float64(vm.SwapTotal)

		// 🌟 内存计算逻辑分支
		if cfg.IncludeBuffer {
			// 将 buffer/cache 视作已使用内存 (Total - Free)
			m.MemUsed = float64(vm.Total - vm.Free)
			m.Mem = (m.MemUsed / m.MemTotal) * 100.0
		} else {
			// 默认逻辑：排除 buffer/cache
			m.MemUsed = float64(vm.Used)
			m.Mem = vm.UsedPercent
		}
	}

	// 3. 磁盘
	d, err := disk.Usage(cfg.DiskPath)
	if err == nil {
		m.Disk = d.UsedPercent
		m.DiskUsed = float64(d.Used)
		m.DiskTotal = float64(d.Total)
	}

	// 4. 系统信息
	h, err := host.Info()
	if err == nil {
		m.Uptime = int64(h.Uptime)
		m.OS = fmt.Sprintf("%s %s", h.Platform, h.PlatformVersion)
		m.Kernel = h.KernelVersion
		m.Arch = h.KernelArch
		m.Virt = h.VirtualizationSystem
		m.Processes = int(h.Procs)
	}

	// 5. 负载
	ld, err := load.Avg()
	if err == nil {
		m.Load1 = ld.Load1
		m.Load5 = ld.Load5
		m.Load15 = ld.Load15
	}

	// 6. 🌟 网络流量过滤逻辑
	// 传入 true 获取所有独立网卡数据，而非合并数据
	netStats, err := net.IOCounters(true)
	if err == nil {
		var rx, tx float64
		includes := splitAndTrim(cfg.NetInclude, ",")
		excludes := splitAndTrim(cfg.NetExclude, ",")

		for _, stat := range netStats {
			name := stat.Name

			// 黑名单判断
			if len(excludes) > 0 && matchInterface(name, excludes) {
				continue
			}
			// 白名单判断
			if len(includes) > 0 && !matchInterface(name, includes) {
				continue
			}
			// 默认排除本地环回接口，除非白名单显式声明
			if name == "lo" && !matchInterface("lo", includes) {
				continue
			}

			rx += float64(stat.BytesRecv)
			tx += float64(stat.BytesSent)
		}

		m.NetRxTotal = rx
		m.NetTxTotal = tx

		now := time.Now()
		if !firstRun {
			sec := now.Sub(lastTime).Seconds()
			if sec > 0 {
				m.NetRxSpeed = math.Max(0, (rx-lastRx)/sec)
				m.NetTxSpeed = math.Max(0, (tx-lastTx)/sec)
			}
		} else {
			firstRun = false
		}

		lastRx = rx
		lastTx = tx
		lastTime = now
	}

	// 7. TCP/UDP连接数
	conns, err := net.Connections("all")
	if err == nil {
		for _, c := range conns {
			if c.Type == 1 {
				m.TCPConn++
			} else if c.Type == 2 {
				m.UDPConn++
			}
		}
	}
// === 重构后的 Docker 读取逻辑 (非阻塞) ===
if cfg.DockerEndpoint != "" {
	// 直接从内存缓存中读取，耗时接近 0ms
	m.DockerContainers = GetCachedDockerStats()
}
// === 新增结束 ===
// 🌟 在这里注入远程控制开关状态
m.TerminalEnabled = cfg.EnableTerminal
	return m, nil
}

// 提取本地 IP 逻辑
func getIPs() (string, string) {
	var ipv4, ipv6 string
	interfaces, err := stdnet.Interfaces()
	if err != nil {
		return "", ""
	}
	for _, iface := range interfaces {
		if iface.Flags&stdnet.FlagUp == 0 || iface.Flags&stdnet.FlagLoopback != 0 {
			continue
		}
		if strings.HasPrefix(iface.Name, "docker") || strings.HasPrefix(iface.Name, "veth") || strings.HasPrefix(iface.Name, "lo") {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip stdnet.IP
			switch v := addr.(type) {
			case *stdnet.IPNet:
				ip = v.IP
			case *stdnet.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			if ip.To4() != nil {
				if ipv4 == "" {
					ipv4 = ip.String()
				}
			} else if ip.To16() != nil {
				if ipv6 == "" {
					ipv6 = ip.String()
				}
			}
		}
	}
	return ipv4, ipv6
}

// 分割并清理网卡名称字符串
func splitAndTrim(s, sep string) []string {
	if s == "" {
		return nil
	}
	var res []string
	for _, part := range strings.Split(s, sep) {
		p := strings.TrimSpace(part)
		if p != "" {
			res = append(res, p)
		}
	}
	return res
}

// 检查网卡名是否包含于列表中（支持前缀匹配，如 "veth" 匹配 "vethxxxxx"）
func matchInterface(name string, list []string) bool {
	for _, item := range list {
		if item != "" && strings.Contains(name, item) {
			return true
		}
	}
	return false
}