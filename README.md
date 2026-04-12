# 涌金阁 · 多市场量化分析平台 v1.0.0

支持 A股 / 港股 / 美股 三大市场的月K线量化分析平台。

## 功能特性

- **月份胜率统计**：按月份统计历史涨跌概率，最早追溯至 2000 年
- **月份筛选**：找出在特定月份历史胜率最高的股票
- **行业分析**：从行业维度筛选季节性规律（申万/中信）
- **LOF 基金套利**：实时折溢价监控与套利记录
- **多数据源**：akshare / yfinance / Tushare / BaoStock / JQData / Alpha Vantage
- **断点续更**：数据更新中断后可从断点继续

---

## 系统要求

| 项目 | 要求 |
|------|------|
| Python | 3.11+ |
| 内存 | 2GB+ |
| 磁盘 | 2GB+（数据库随数据量增长，A+港+美全量约 600MB） |
| 操作系统 | Windows 10+ / Linux / macOS / Docker |
| 网络 | 首次全量更新需要访问外网数据源 |

---

## 部署方式

### 方式一：Windows

```cmd
install.bat     # 安装 Python 依赖
start.bat       # 启动服务（前台）
```

> 长期后台运行推荐使用桌面安装包 `涌金阁_Setup_v1.0.0.exe`，安装后自动注册为 Windows 服务，开机自启。

### 方式二：Linux / macOS

```bash
chmod +x install.sh start.sh

# 普通启动（前台）
./install.sh && ./start.sh

# Linux 开机自启（需 root，自动注册 systemd）
sudo bash install.sh
```

systemd 服务管理：
```bash
systemctl status stock-insight    # 查看状态
journalctl -u stock-insight -f    # 查看日志
systemctl restart stock-insight   # 重启
systemctl stop stock-insight      # 停止
```

### 方式三：Docker（推荐，自动后台 + 开机自启）

```bash
docker-compose up -d        # 构建并后台启动
docker-compose logs -f      # 查看日志
docker-compose down         # 停止
docker-compose up -d --build  # 更新代码后重建
```

> 数据自动持久化到宿主机 `./data/` 目录，容器崩溃或服务器重启均自动恢复。

启动后访问：**http://localhost:8588**

---

## 默认账号

| 账号 | 密码 |
|------|------|
| `admin` | `admin123` |

**首次登录后请立即修改密码！**

---

## 配置文件

复制 `config.json.example` 为 `config.json`，按需填写：

```json
{
  "data_source": "akshare",
  "market_data_sources": {
    "A": "akshare",
    "HK": "akshare",
    "US": "yfinance"
  },
  "tushare": {
    "token": ""
  },
  "jqdata": {
    "username": "",
    "password": ""
  },
  "alpha_vantage": {
    "api_key": "",
    "requests_per_minute": 5
  },
  "yfinance": {
    "timeout": 30,
    "retry_times": 3,
    "retry_delay": 1
  },
  "update_frequency": "monthly"
}
```

### 数据源选择建议

| 市场 | 推荐数据源 | 说明 |
|------|-----------|------|
| A股 | `akshare` | 免费，无需配置 |
| 港股 | `akshare` | 免费，覆盖全量 2700+ 只；yfinance 仅约 900 只 |
| 美股 | `yfinance` | 免费，覆盖 8000+ 只，批量下载约 40 分钟 |

需要 Tushare 的在 [tushare.pro](https://tushare.pro) 注册获取 Token；Alpha Vantage 免费版限 25 次/天，不适合全量更新。

### 环境变量（Docker / 生产环境）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DB_PATH` | `stock_data.db` | 主数据库路径 |
| `CONFIG_PATH` | `config.json` | 配置文件路径 |
| `DATA_DIR` | `/app/data` | 数据目录（Docker 专用） |

---

## 数据存储

| 文件 | 说明 |
|------|------|
| `stock_data.db` | 主数据库，存储三市场全量K线 |
| `lof1/lof_arbitrage.db` | LOF套利数据库 |
| `config.json` | 运行时配置 |
| `update_checkpoint.json` | 断点续更进度记录 |

> **定期备份 `stock_data.db`**，全量更新耗时较长（A股约 20 分钟，港股约 30 分钟，美股约 40 分钟）。

---

## 技术栈

| 层 | 技术 |
|----|------|
| 后端 | Python 3.11 · FastAPI · SQLite |
| 子应用 | Flask（LOF套利模块） |
| 前端 | Bootstrap · 原生 JS |
| 部署 | uvicorn · Docker · systemd · Windows Service |

---

## 文档

- [使用说明](USAGE.md) — 功能详解、数据源配置、常见问题
- [部署指南](DEPLOYMENT.md) — 各平台详细部署步骤、Nginx 反向代理、VPS 配置
