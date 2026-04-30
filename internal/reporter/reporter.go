package reporter

import (
	"crypto/tls" // 🌟 新增：用于配置 TLS
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"net/http"

	"github.com/gorilla/websocket"
	"Float-agent/internal/collector"
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

	log.Printf("[WS] 已连接到服务端: %s (Insecure: %v)\n", c.serverURL, c.insecure)

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

			log.Println("[WS] 连接断开")
		}()

		for {
			var msg rpcResponse
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}

			if msg.Method == "tasks.push" && msg.ID == 0 {
				var tasks []Task
				if err := json.Unmarshal(msg.Result, &tasks); err == nil && c.OnTasks != nil {
					c.OnTasks(tasks)
				}
				continue
			}

			if msg.ID != 0 {
				c.pendingMu.Lock()
				ch, ok := c.pending[msg.ID]
				if ok {
					delete(c.pending, msg.ID)
				}
				c.pendingMu.Unlock()
				if ok {
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

func (c *WSClient) SendTaskResult(taskID int, nodeID string, pingMs float64) {
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
			"ping_ms": pingMs,
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
			log.Printf("[WS] 连接失败，%v 后重试: %v\n", backoff, err)
		} else {
			for c.IsConnected() {
				time.Sleep(500 * time.Millisecond)
			}
			backoff = 2 * time.Second
			log.Println("[WS] 准备重连...")
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