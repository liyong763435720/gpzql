#!/bin/bash

echo "========================================"
echo "  涌金阁 - 多市场量化分析平台 v1.0.0 安装脚本"
echo "========================================"
echo ""

# 检测操作系统
OS="$(uname -s)"
ARCH="$(uname -m)"
if [ "$OS" = "Darwin" ]; then
    echo "[信息] 检测到 macOS（$ARCH）"
    # Apple Silicon 提示
    if [ "$ARCH" = "arm64" ]; then
        echo "[提示] Apple Silicon 架构：baostock 可能无法安装，建议使用 akshare/yfinance 数据源"
    fi
    # 检查 Xcode Command Line Tools
    if ! xcode-select -p &> /dev/null; then
        echo "[警告] 未检测到 Xcode Command Line Tools，部分依赖可能编译失败"
        echo "       请运行: xcode-select --install"
    fi
else
    echo "[信息] 检测到 Linux（$ARCH）"
fi

# 检查Python
if ! command -v python3 &> /dev/null; then
    echo "[错误] 未检测到Python3，请先安装Python 3.11+"
    if [ "$OS" = "Darwin" ]; then
        echo "       macOS 推荐: brew install python@3.11"
    fi
    exit 1
fi

echo "[信息] Python版本: $(python3 --version)"

# 检查pip
if ! command -v pip3 &> /dev/null; then
    echo "[错误] 未检测到pip3，请先安装pip"
    exit 1
fi

# 升级pip
echo "[信息] 升级pip..."
pip3 install --upgrade pip -q

# 安装依赖
echo "[信息] 安装依赖包..."
pip3 install -r requirements.txt

if [ $? -eq 0 ]; then
    echo ""
    echo "[成功] 安装完成！"
    echo ""

    # 初始化 lof1 配置文件
    if [ ! -f "lof1/config.py" ] && [ -f "lof1/config.example.py" ]; then
        cp lof1/config.example.py lof1/config.py
        echo "[信息] 已初始化 lof1/config.py（从 config.example.py 复制）"
    fi

    # Linux 下自动安装 systemd 服务（macOS 跳过）
    if [ "$OS" = "Linux" ] && command -v systemctl &> /dev/null && [ "$EUID" -eq 0 ]; then
        WORKDIR="$(pwd)"
        PYTHON="$(command -v python3)"
        SERVICE_USER="$(logname 2>/dev/null || echo root)"
        sed -e "s|__WORKDIR__|$WORKDIR|g" \
            -e "s|__PYTHON__|$PYTHON|g" \
            -e "s|__USER__|$SERVICE_USER|g" \
            stock-insight.service > /etc/systemd/system/stock-insight.service
        systemctl daemon-reload
        systemctl enable stock-insight
        systemctl restart stock-insight
        echo "[systemd] 服务已注册并启动，开机自启已开启"
        echo "[systemd] 查看状态: systemctl status stock-insight"
        echo "[systemd] 查看日志: journalctl -u stock-insight -f"
    elif [ "$OS" = "Linux" ] && command -v systemctl &> /dev/null && [ "$EUID" -ne 0 ]; then
        echo "[提示] 以 root 运行可自动注册为 systemd 服务（开机自启）："
        echo "       sudo bash install.sh"
        echo ""
        echo "启动方式（前台）："
        echo "  ./start.sh"
    else
        echo "启动方式："
        echo "  ./start.sh"
    fi
    echo ""
else
    echo "[错误] 依赖安装失败"
    if [ "$OS" = "Darwin" ] && [ "$ARCH" = "arm64" ]; then
        echo "       Apple Silicon 用户可尝试跳过 baostock："
        echo "       pip3 install \$(grep -v baostock requirements.txt | tr '\n' ' ')"
    fi
    exit 1
fi

