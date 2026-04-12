#!/bin/bash
set -e

# 支持外部环境变量覆盖，未设置时使用默认值
DATA_DIR="${DATA_DIR:-/app/data}"
DB_PATH="${DB_PATH:-$DATA_DIR/stock_data.db}"
CONFIG_PATH="${CONFIG_PATH:-$DATA_DIR/config.json}"

mkdir -p "$DATA_DIR"

# ── 主数据库初始化 ─────────────────────────────────────────
if [ ! -f "$DB_PATH" ]; then
    echo "[init] 初始化主数据库: $DB_PATH"
    python -c "from app.database import Database; Database(db_path='$DB_PATH')"
fi

# ── 默认配置初始化 ─────────────────────────────────────────
if [ ! -f "$CONFIG_PATH" ]; then
    echo "[init] 创建默认配置: $CONFIG_PATH"
    python -c "from app.config import Config; c = Config(config_file='$CONFIG_PATH'); c.save_config()"
fi

export DB_PATH="$DB_PATH"
export CONFIG_PATH="$CONFIG_PATH"

# ── lof1 数据库持久化（软链接到 data 目录）────────────────
LOF1_DB_HOST="$DATA_DIR/lof_arbitrage.db"
LOF1_DB_APP="/app/lof1/lof_arbitrage.db"

# 首次启动：若容器内存在真实的 lof1 db 文件，迁移到持久化目录
if [ -f "$LOF1_DB_APP" ] && [ ! -L "$LOF1_DB_APP" ]; then
    echo "[init] 迁移 lof1 数据库到持久化目录"
    mv "$LOF1_DB_APP" "$LOF1_DB_HOST"
fi

# 确保持久化目录中存在 lof1 db（空文件即可，lof1 启动时会自动建表）
if [ ! -f "$LOF1_DB_HOST" ]; then
    echo "[init] 初始化 lof1 数据库: $LOF1_DB_HOST"
    touch "$LOF1_DB_HOST"
fi

# 建立软链接
if [ ! -L "$LOF1_DB_APP" ]; then
    ln -sf "$LOF1_DB_HOST" "$LOF1_DB_APP"
fi

echo "[start] 启动涌金阁，端口 8588..."
exec python start_prod.py
