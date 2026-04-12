#!/bin/bash

echo "========================================"
echo "  涌金阁 - 多市场量化分析平台 v1.0.0"
echo "========================================"
echo ""

OS="$(uname -s)"
if command -v python3 &> /dev/null; then
    PYTHON="python3"
elif command -v python &> /dev/null; then
    PYTHON="python"
else
    echo "[错误] 未检测到Python，请先安装Python 3.11+"
    if [ "$OS" = "Darwin" ]; then
        echo "       macOS 推荐: brew install python@3.11"
    fi
    exit 1
fi

echo "[信息] 检查Python环境..."
$PYTHON --version

# 确保 lof1/config.py 存在（GitHub 克隆后首次启动时自动初始化）
if [ ! -f "lof1/config.py" ] && [ -f "lof1/config.example.py" ]; then
    cp lof1/config.example.py lof1/config.py
    echo "[信息] 已初始化 lof1/config.py（从 config.example.py 复制）"
fi

echo "[信息] 启动服务..."
echo "[信息] 服务地址: http://localhost:8588"
echo "[信息] 按 Ctrl+C 停止服务"
echo ""

$PYTHON start_prod.py

