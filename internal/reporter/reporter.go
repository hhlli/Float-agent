package reporter

import (
	"crypto/tls" // 🌟 新增：用于配置 TLS
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"net/http"

	"github.com/gorilla/websocket"
	"Float-agent/internal/terminal" // 🌟 必须添加这一行
	"Float-agent/internal/collector"
	"Float-agent/internal/logger"
	"go.uber.org/zap"
)

// ── JSON-RPC 结构 ───────────────────────────────────────
type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int         `json:"id,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      int             `json:"id,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ── 任务结构 ────────────────────────────────────────────
type Task struct {
	ID       int    `json:"id"`
	Type     string `json:"type"`
	Target   string `json:"target"`
	Interval int    `json:"interval"`
}

type reportPayload struct {
	NodeID string            `json:"node_id"`
	Data   *collector.Metric `json:"data"`
}

// ── WebSocket 客户端 ────────────────────────────────────
type WSClient struct {
	serverURL string
	nodeID    string
	token     string
	insecure  bool // 🌟 新增：是否忽略证书

	conn      *websocket.Conn
	connMu    sync.Mutex
	idCounter atomic.Int32

	// 等待中的 RPC 响应，key=id
	pending   map[int]chan *rpcResponse
	pendingMu sync.Mutex

	OnTasks func([]Task)
	stopCh  chan struct{}
}

// 🌟 修改：构造函数增加 insecure 参数
func NewWSClient(serverURL, nodeID, token string, insecure bool) *WSClient {
	return &WSClient{
		serverURL: serverURL,
		nodeID:    nodeID,
		token:     token,
		insecure:  insecure,
		pending:   make(map[int]chan *rpcResponse),
		stopCh:    make(chan struct{}),
	}
}

func toWSURL(serverURL, token string) string {
	u, err := url.Parse(strings.TrimSuffix(serverURL, "/"))
	if err != nil {
		return serverURL
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = "/agent/ws"
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()
	return u.String()
}

// Connect 建立连接并启动唯一读循环
func (c *WSClient) Connect() error {
	wsURL := toWSURL(c.serverURL, c.token)

	// 🌟 核心修改：配置拨号器
	dialer := websocket.DefaultDialer
	if c.insecure {
		dialer = &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 45 * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // 核心：忽略不安全证书
			},
		}
	}

	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	logger.Log.Info("Connected to server", zap.String("server", c.serverURL), zap.Bool("insecure", c.insecure))

	// 唯一读循环
	go func() {
        defer func() {
            conn.Close()
            c.connMu.Lock()
            c.conn = nil
            c.connMu.Unlock()

            c.pendingMu.Lock()
            for id, ch := range c.pending {
                ch <- nil
                delete(c.pending, id)
            }
            c.pendingMu.Unlock()
            logger.Log.Warn("WebSocket connection lost", zap.String("server", c.serverURL))
        }()

        for {
            // 🌟 核心修改：使用 map 接收以区分 Request 和 Response
            var rawMsg map[string]interface{}
            if err := conn.ReadJSON(&rawMsg); err != nil {
                return
            }

            // A. 处理服务端主动下发的指令 (Request)
            if method, ok := rawMsg["method"].(string); ok {
                switch method {
                case "tasks.push":
                    // 兼容处理：将 params 转为 Task 列表
                    if params, exists := rawMsg["params"]; exists {
                        var tasks []Task
                        pBytes, _ := json.Marshal(params)
                        if err := json.Unmarshal(pBytes, &tasks); err == nil && c.OnTasks != nil {
                            c.OnTasks(tasks)
                        }
                    }
                case "terminal.request":
                    // 🌟 新增：处理远程控制请求
                    if params, ok := rawMsg["params"].(map[string]interface{}); ok {
                        sessionID, _ := params["session_id"].(string)
                        if sessionID != "" {
							logger.Log.Info("Terminal session requested", zap.String("session_id", sessionID))
							go c.connectTerminalWS(sessionID)
						}
                    }
				case "mtr.request":
					if params, ok := rawMsg["params"].(map[string]interface{}); ok {
						target, _ := params["target"].(string)
						if target != "" {
							go c.executeMTRAndReport(target) // 调用拆分到 mtr.go 的方法
						}
					}
                }
                continue // 处理完 Request 直接跳过
            }

            // B. 处理服务端返回的响应 (Response，对应 Agent 发出的 report 等)
            // JSON 数字在 map 中默认是 float64
            if idFloat, ok := rawMsg["id"].(float64); ok && idFloat != 0 {
                id := int(idFloat)
                c.pendingMu.Lock()
                ch, ok := c.pending[id]
                if ok {
                    delete(c.pending, id)
                }
                c.pendingMu.Unlock()
                
                if ok {
                    // 将 map 转回 rpcResponse 结构体以保持现有逻辑兼容
                    var msg rpcResponse
                    msgBytes, _ := json.Marshal(rawMsg)
                    json.Unmarshal(msgBytes, &msg)
                    ch <- &msg
                }
            }
        }
    }()

    return nil
}

func (c *WSClient) IsConnected() bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn != nil
}

func (c *WSClient) send(method string, params interface{}) (*rpcResponse, error) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("未连接")
	}

	id := int(c.idCounter.Add(1))
	ch := make(chan *rpcResponse, 1)

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()

	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	c.connMu.Lock()
	err := conn.WriteJSON(req)
	c.connMu.Unlock()
	if err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, err
	}

	select {
	case resp := <-ch:
		if resp == nil {
			return nil, fmt.Errorf("连接断开")
		}
		return resp, nil
	case <-time.After(10 * time.Second):
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("响应超时")
	}
}

func (c *WSClient) Report(metric *collector.Metric) ([]Task, error) {
	resp, err := c.send("report", reportPayload{NodeID: c.nodeID, Data: metric})
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", resp.Error.Code, resp.Error.Message)
	}

	var result struct {
		Status string `json:"status"`
		Tasks  []Task `json:"tasks"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, err
	}
	return result.Tasks, nil
}

func (c *WSClient) SendTaskResult(taskID int, nodeID string, pingMs, loss, jitter, p50, p99, minMs, maxMs float64, status string) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return
	}

	id := int(c.idCounter.Add(1))
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  "task.result",
		Params: map[string]interface{}{
			"node_id": nodeID,
			"task_id": taskID,
			"ping_ms": pingMs, // 可作为平均延迟
			"loss":    loss,
			"jitter":  jitter,
			"p50":     p50,
			"p99":     p99,
			"min_ms":  minMs,
			"max_ms":  maxMs,
			"status":  status,
		},
		ID: id,
	}
	c.connMu.Lock()
	conn.WriteJSON(req)
	c.connMu.Unlock()
}

func (c *WSClient) ConnectWithRetry() {
	backoff := 2 * time.Second
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if err := c.Connect(); err != nil {
			logger.Log.Error("Connection failed", zap.Error(err), zap.Duration("next_retry", backoff))
		} else {
			for c.IsConnected() {
				time.Sleep(500 * time.Millisecond)
			}
			backoff = 2 * time.Second
			logger.Log.Debug("Preparing to reconnect...")
			continue
		}

		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *WSClient) Stop() {
	close(c.stopCh)
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()
}

// 🌟 新增：发起终端专用连接
// 需要确保你已经在 internal/terminal 包中实现了 Start 方法
// 并在 reporter.go 顶部 import 了 "Float-agent/internal/terminal"
func (c *WSClient) connectTerminalWS(sessionID string) {
	u, err := url.Parse(strings.TrimSuffix(c.serverURL, "/"))
	if err != nil {
		return
	}
	// 转换协议为 ws/wss
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	
	// 设置终端专用路由
	u.Path = "/agent/terminal/ws"
	q := u.Query()
	q.Set("token", c.token)
	q.Set("session_id", sessionID)
	u.RawQuery = q.Encode()

	dialer := websocket.DefaultDialer
	if c.insecure {
		dialer = &websocket.Dialer{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	termConn, _, err := dialer.Dial(u.String(), nil)
    if err != nil {
        logger.Log.Error("Terminal dial failed", zap.Error(err))
        return
}
	terminal.Start(termConn)
}