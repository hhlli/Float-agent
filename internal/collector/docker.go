package collector

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"Float-agent/internal/logger"
	"go.uber.org/zap"
)

type DockerContainer struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	State  string   `json:"state"`
	Status string   `json:"status"`
	CPU    *float64 `json:"cpu,omitempty"`
	Mem    *float64 `json:"mem,omitempty"`
	MemPct *float64 `json:"mem_pct,omitempty"`
}

var (
	cachedContainers []DockerContainer
	dockerCacheMu    sync.RWMutex
)

func StartDockerCollector(endpoint string, interval time.Duration, enableStats bool) {
	if endpoint == "" {
		return
	}

	updateDockerCache(endpoint, enableStats)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		updateDockerCache(endpoint, enableStats)
	}
}

func updateDockerCache(endpoint string, enableStats bool) {
	containers, err := FetchDockerStats(endpoint, enableStats)
	if err != nil {
		logger.Log.Error("采集出错", 
			zap.String("module", "Docker"), 
			zap.Error(err),
		)
		return
	}

	dockerCacheMu.Lock()
	cachedContainers = containers
	dockerCacheMu.Unlock()
}

func GetCachedDockerStats() []DockerContainer {
	dockerCacheMu.RLock()
	defer dockerCacheMu.RUnlock()
	return cachedContainers
}

func FetchDockerStats(endpoint string, enableStats bool) ([]DockerContainer, error) {
	if endpoint == "" {
		return nil, nil
	}

	cli, err := client.NewClientWithOpts(
		client.WithHost(endpoint),
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	containers, err := cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, err
	}

	var result []DockerContainer
	for _, c := range containers {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}

		dc := DockerContainer{
			ID:     c.ID[:12],
			Name:   name,
			State:  c.State,
			Status: c.Status,
		}

		// 如果开启了详细资源收集且容器正在运行，则请求 stats 接口
		if enableStats && c.State == "running" {
			cpu, mem, memPct := getContainerStats(cli, ctx, c.ID)
			dc.CPU = &cpu
			dc.Mem = &mem
			dc.MemPct = &memPct
		}

		result = append(result, dc)
	}

	return result, nil
}

// 🌟 修改点：使用匿名结构体替代官方的 types.StatsJSON，实现向下/向上完全兼容
func getContainerStats(cli *client.Client, ctx context.Context, id string) (float64, float64, float64) {
	stats, err := cli.ContainerStats(ctx, id, false)
	if err != nil {
		return 0, 0, 0
	}
	defer stats.Body.Close()

	// 局部定义需要解析的 JSON 字段，绕过 SDK 版本差异
	var v struct {
		CPUStats struct {
			CPUUsage struct {
				TotalUsage  uint64   `json:"total_usage"`
				PercpuUsage []uint64 `json:"percpu_usage"`
			} `json:"cpu_usage"`
			SystemUsage uint64 `json:"system_cpu_usage"`
			OnlineCPUs  uint32 `json:"online_cpus"`
		} `json:"cpu_stats"`
		PreCPUStats struct {
			CPUUsage struct {
				TotalUsage uint64 `json:"total_usage"`
			} `json:"cpu_usage"`
			SystemUsage uint64 `json:"system_cpu_usage"`
		} `json:"precpu_stats"`
		MemoryStats struct {
			Usage uint64            `json:"usage"`
			Limit uint64            `json:"limit"`
			Stats map[string]uint64 `json:"stats"`
		} `json:"memory_stats"`
	}

	if err := json.NewDecoder(stats.Body).Decode(&v); err != nil {
		return 0, 0, 0
	}

	var cpuPercent float64
	cpuDelta := float64(v.CPUStats.CPUUsage.TotalUsage) - float64(v.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(v.CPUStats.SystemUsage) - float64(v.PreCPUStats.SystemUsage)
	onlineCPUs := float64(v.CPUStats.OnlineCPUs)
	if onlineCPUs == 0.0 {
		onlineCPUs = float64(len(v.CPUStats.CPUUsage.PercpuUsage))
	}
	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * onlineCPUs * 100.0
	}

	cache := v.MemoryStats.Stats["cache"]
	if cache == 0 {
		cache = v.MemoryStats.Stats["inactive_file"]
	}
	usedMem := float64(v.MemoryStats.Usage) - float64(cache)
	if usedMem < 0 {
		usedMem = 0
	}
	limitMem := float64(v.MemoryStats.Limit)
	memPercent := 0.0
	if limitMem > 0 {
		memPercent = (usedMem / limitMem) * 100.0
	}

	return cpuPercent, usedMem, memPercent
}