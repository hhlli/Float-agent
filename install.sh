#!/bin/bash

# 1. 解析命令行的 -i (节点ID) 和 -t (Token) 和 -s (服务端地址)
while getopts "i:t:s:" opt; do
  case $opt in
    i) NODE_ID="$OPTARG" ;;
    t) TOKEN="$OPTARG" ;;
    s) SERVER_URL="$OPTARG" ;;
  esac
done

if [ -z "$NODE_ID" ] || [ -z "$TOKEN" ]; then
  echo "错误: 缺少节点 ID 或 Token"
  exit 1
fi

# 如果没传服务端地址，就尝试通过当前下载来源推断（这里为测试简写）
if [ -z "$SERVER_URL" ]; then
    SERVER_URL="http://127.0.0.1:8080" # 占位
fi

echo "=> 正在从 $SERVER_URL 下载探针..."
# 下载刚才编译好的 Linux 探针
curl -fsSL "$SERVER_URL/probe-linux" -o /tmp/probe-linux
chmod +x /tmp/probe-linux

echo "=> 正在启动探针服务..."
# 杀死可能存在的旧进程，并使用 nohup 让探针在后台静默运行（测试专用，生产环境会用 systemd）
pkill -f "probe-linux"
nohup /tmp/probe-linux -i "$NODE_ID" -t "$TOKEN" -s "$SERVER_URL" > /tmp/probe.log 2>&1 &

echo "================================"
echo "探针部署完成！"
echo "节点 ID: $NODE_ID"
echo "您可以查看 /tmp/probe.log 获取运行日志。"
echo "================================"