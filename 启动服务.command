#!/bin/bash
cd /Users/szp/.qclaw/workspace/barrel-management
# caffeinate -disu: 全模式防休眠（显示器/空闲/强制休眠/磁盘）
nohup caffeinate -disu python3 -m uvicorn server:app --host 0.0.0.0 --port 8080 > /tmp/barrel-server.log 2>&1 &
sleep 1
IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | awk '{print $2}' | head -1)
echo "✅ 桶号管理服务已启动（全模式防休眠）"
echo "📱 PDA 访问: http://${IP}:8080"
