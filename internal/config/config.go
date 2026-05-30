package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Interval      time.Duration `yaml:"interval"`
	DiskPath      string        `yaml:"disk_path"`
	ServerURL     string        `yaml:"server_url"`
	NodeID        string        `yaml:"node_id"`
	AuthToken     string        `yaml:"auth_token"`

	// 🌟 新增的高级传参字段
	Insecure      bool   `yaml:"insecure"`
	NoUpdate      bool   `yaml:"no_update"`
	IncludeBuffer bool   `yaml:"include_buffer"`
	DisableRPC    bool   `yaml:"disable_rpc"`
	NetInclude    string `yaml:"net_include"`
	NetExclude    string `yaml:"net_exclude"`
	DockerEndpoint string `yaml:"docker_endpoint"`
	DockerStats   bool   `yaml:"docker_stats"` // 🌟 修复此处的标签
	// 新增终端控制开关
	EnableTerminal bool `yaml:"enable_terminal"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}