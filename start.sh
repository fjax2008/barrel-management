#!/bin/bash
# ═══════════════════════════════════════════════
# 生管部在制品桶号管理系统 - 启动脚本
# ═══════════════════════════════════════════════

set -e
cd "$(dirname "$0")"

echo "════════════════════════════════════════"
echo "  🏭 生管部在制品桶号管理系统"
echo "════════════════════════════════════════"
echo ""

# 检查 Python
if ! command -v python3 &>/dev/null; then
    echo "❌ 未找到 python3，请先安装 Python"
    exit 1
fi

echo "🐍 Python: $(python3 --version)"

# 安装依赖
if [ ! -d "venv" ]; then
    echo "📦 创建虚拟环境..."
    python3 -m venv venv
fi

source venv/bin/activate
echo "📦 安装依赖..."
pip install -q fastapi uvicorn[standard]

# 获取本机 IP
IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | awk '{print $2}' | head -1)
if [ -z "$IP" ]; then
    IP="localhost"
fi

echo ""
echo "════════════════════════════════════════"
echo "  ✅ 服务启动中..."
echo ""
echo "  📡 本机访问: http://localhost:8080"
echo "  📱 PDA 访问: http://${IP}:8080"
echo ""
echo "  请确保 PDA 与电脑在同一 WiFi 下"
echo "════════════════════════════════════════"
echo ""

uvicorn server:app --host 0.0.0.0 --port 8080 --reload
