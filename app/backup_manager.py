"""
备份与还原管理器
支持三种备份类型：
  - user_data : 用户业务数据（lof_arbitrage.db + config.json）
  - full      : 全量备份（上面 + stock_data.db）
  - config    : 仅配置文件（config.json）
"""

import os
import json
import zipfile
import sqlite3
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

# ── 路径常量 ──────────────────────────────────────────────────────────────────
_ROOT        = Path(__file__).parent.parent          # C:/gpzql
BACKUP_DIR   = _ROOT / "backups"
LOF_DB_PATH  = _ROOT / "lof1" / "lof_arbitrage.db"
STOCK_DB_PATH= _ROOT / "stock_data.db"
CONFIG_PATH  = _ROOT / "config.json"
VERSION_FILE = _ROOT / "VERSION.txt"

BACKUP_DIR.mkdir(exist_ok=True)

# ── 备份类型描述 ───────────────────────────────────────────────────────────────
BACKUP_TYPES = {
    "user_data": "用户数据（账号/订单/套利记录/公告等 + 配置）",
    "full":      "全量备份（用户数据 + 行情库，体积较大）",
    "config":    "仅配置文件",
}


def _get_version() -> str:
    try:
        return VERSION_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        return "unknown"


def _backup_sqlite(src: Path, dst: Path):
    """用 SQLite 内置 backup API 安全备份数据库（支持在线备份）"""
    src_conn = sqlite3.connect(str(src))
    dst_conn = sqlite3.connect(str(dst))
    src_conn.backup(dst_conn)
    src_conn.close()
    dst_conn.close()


def create_backup(backup_type: str = "user_data") -> dict:
    """
    创建备份，返回 {"success": True, "filename": "...", "size": N}
    """
    if backup_type not in BACKUP_TYPES:
        return {"success": False, "message": f"不支持的备份类型: {backup_type}"}

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"backup_{backup_type}_{ts}.zip"
    zip_path = BACKUP_DIR / filename

    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            files_included = []

            # ── 按类型收集文件 ────────────────────────────────────────────────
            if backup_type in ("user_data", "full"):
                if LOF_DB_PATH.exists():
                    dst = tmp / "lof_arbitrage.db"
                    _backup_sqlite(LOF_DB_PATH, dst)
                    files_included.append("lof_arbitrage.db")

            if backup_type == "full":
                if STOCK_DB_PATH.exists():
                    dst = tmp / "stock_data.db"
                    _backup_sqlite(STOCK_DB_PATH, dst)
                    files_included.append("stock_data.db")

            if backup_type in ("user_data", "full", "config"):
                if CONFIG_PATH.exists():
                    shutil.copy2(CONFIG_PATH, tmp / "config.json")
                    files_included.append("config.json")

            # ── 写 manifest ───────────────────────────────────────────────────
            manifest = {
                "backup_type":  backup_type,
                "created_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "app_version":  _get_version(),
                "files":        files_included,
                "description":  BACKUP_TYPES[backup_type],
            }
            (tmp / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            # ── 打包 zip ──────────────────────────────────────────────────────
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                for f in tmp.iterdir():
                    zf.write(f, f.name)

        size = zip_path.stat().st_size
        return {"success": True, "filename": filename, "size": size}

    except Exception as e:
        if zip_path.exists():
            zip_path.unlink()
        return {"success": False, "message": f"备份失败: {e}"}


def list_backups() -> list:
    """列出所有备份，按时间倒序"""
    result = []
    for f in sorted(BACKUP_DIR.glob("backup_*.zip"), reverse=True):
        manifest = {}
        try:
            with zipfile.ZipFile(f, "r") as zf:
                if "manifest.json" in zf.namelist():
                    manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        except Exception:
            pass
        result.append({
            "filename":    f.name,
            "size":        f.stat().st_size,
            "created_at":  manifest.get("created_at", ""),
            "backup_type": manifest.get("backup_type", "unknown"),
            "app_version": manifest.get("app_version", ""),
            "description": manifest.get("description", ""),
            "files":       manifest.get("files", []),
        })
    return result


def delete_backup(filename: str) -> dict:
    """删除指定备份文件"""
    # 安全校验：只允许删除 backup_*.zip
    if not filename.startswith("backup_") or not filename.endswith(".zip") or "/" in filename or "\\" in filename:
        return {"success": False, "message": "非法文件名"}
    path = BACKUP_DIR / filename
    if not path.exists():
        return {"success": False, "message": "文件不存在"}
    path.unlink()
    return {"success": True}


def restore_backup(zip_bytes: bytes, restore_types: list = None) -> dict:
    """
    从 zip 字节还原备份。
    restore_types: 指定只还原哪些文件，None 表示全部。
    还原前会自动将当前数据备份一份（safety_backup）。
    """
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            zip_path = tmp / "upload.zip"
            zip_path.write_bytes(zip_bytes)

            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                if "manifest.json" not in names:
                    return {"success": False, "message": "无效的备份文件（缺少 manifest.json）"}
                manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
                zf.extractall(tmp)

            files = manifest.get("files", [])
            if restore_types:
                files = [f for f in files if f in restore_types]

            if not files:
                return {"success": False, "message": "没有可还原的文件"}

            # ── 还原前安全备份 ────────────────────────────────────────────────
            safety = create_backup("user_data")
            safety_note = f"（已自动保存安全备份：{safety.get('filename','')}）"

            restored = []
            errors   = []

            dest_map = {
                "lof_arbitrage.db": LOF_DB_PATH,
                "stock_data.db":    STOCK_DB_PATH,
                "config.json":      CONFIG_PATH,
            }

            for fname in files:
                src = tmp / fname
                dst = dest_map.get(fname)
                if dst is None or not src.exists():
                    errors.append(f"{fname}: 跳过（不在还原映射中）")
                    continue
                try:
                    if fname.endswith(".db"):
                        _backup_sqlite(src, dst)
                    else:
                        shutil.copy2(src, dst)
                    restored.append(fname)
                except Exception as e:
                    errors.append(f"{fname}: {e}")

            if not restored:
                return {"success": False, "message": "还原失败，没有文件被写入" + safety_note}

            msg = f"已还原 {len(restored)} 个文件：{', '.join(restored)}{safety_note}"
            if errors:
                msg += f"；警告：{'; '.join(errors)}"
            return {"success": True, "message": msg, "restored": restored}

    except zipfile.BadZipFile:
        return {"success": False, "message": "无效的 zip 文件"}
    except Exception as e:
        return {"success": False, "message": f"还原失败: {e}"}


def cleanup_old_backups(keep: int = 10):
    """保留最新 keep 份，删除多余的备份"""
    files = sorted(BACKUP_DIR.glob("backup_*.zip"), reverse=True)
    for f in files[keep:]:
        try:
            f.unlink()
        except Exception:
            pass


# ── 股票数据导出 / 导入 ────────────────────────────────────────────────────────

def export_kline_backup(market: str = None) -> bytes:
    """
    将 stocks + monthly_kline 表导出为 gzip 压缩的 SQLite 文件字节流。
    market: None=全部, 'A'/'HK'/'US'=指定市场
    """
    import gzip, uuid
    # 使用项目内 tmp/ 目录，避免 Windows 系统临时目录的 8.3 短路径（如 ADMINI~1）
    # 导致 SQLite 报 [WinError 267] 问题
    tmp_dir = Path(__file__).parent.parent / "tmp"
    tmp_dir.mkdir(exist_ok=True)
    export_db = tmp_dir / f"klinedata_{uuid.uuid4().hex}.db"

    try:
        src_conn  = sqlite3.connect(str(STOCK_DB_PATH))
        dst_conn  = sqlite3.connect(str(export_db))
        src_cur   = src_conn.cursor()
        dst_cur   = dst_conn.cursor()

        # 创建目标表
        dst_cur.executescript("""
            CREATE TABLE IF NOT EXISTS stocks (
                ts_code TEXT PRIMARY KEY, symbol TEXT, name TEXT,
                area TEXT, industry TEXT, list_date TEXT,
                delist_date TEXT, is_hs TEXT, exchange TEXT,
                market TEXT, currency TEXT
            );
            CREATE TABLE IF NOT EXISTS monthly_kline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
                year INTEGER, month INTEGER,
                open REAL, close REAL, high REAL, low REAL,
                vol REAL, amount REAL, pct_chg REAL,
                data_source TEXT, market TEXT, currency TEXT,
                UNIQUE(ts_code, trade_date, data_source)
            );
        """)

        # 导出 stocks
        if market:
            src_cur.execute("SELECT * FROM stocks WHERE market=?", (market,))
        else:
            src_cur.execute("SELECT * FROM stocks")
        rows = src_cur.fetchall()
        dst_cur.executemany(
            "INSERT OR REPLACE INTO stocks VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows
        )

        # 导出 monthly_kline（跳过自增 id）
        cols = "ts_code,trade_date,year,month,open,close,high,low,vol,amount,pct_chg,data_source,market,currency"
        if market:
            src_cur.execute(f"SELECT {cols} FROM monthly_kline WHERE market=?", (market,))
        else:
            src_cur.execute(f"SELECT {cols} FROM monthly_kline")
        kline_rows = src_cur.fetchall()
        dst_cur.executemany(
            f"INSERT OR IGNORE INTO monthly_kline ({cols}) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            kline_rows
        )

        dst_conn.commit()
        src_conn.close()
        dst_conn.close()

        raw = export_db.read_bytes()
        return gzip.compress(raw, compresslevel=6)
    finally:
        # 清理临时文件（含 SQLite WAL/SHM 辅助文件）
        for suffix in ('', '-wal', '-shm'):
            p = Path(str(export_db) + suffix)
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass


def import_kline_backup(gz_bytes: bytes, mode: str = "merge", market: str = None) -> dict:
    """
    从 gzip 压缩的 SQLite 字节流还原股票数据。
    mode:   'merge'  = INSERT OR IGNORE（保留已有数据，补充缺失）
            'replace' = 先清空同市场数据，再全量写入
    market: None=全部, 'A'/'HK'/'US'=只还原指定市场
    """
    import gzip, uuid
    try:
        raw = gzip.decompress(gz_bytes)
    except Exception:
        return {"success": False, "message": "文件解压失败，请确认是有效的 .db.gz 备份文件"}

    tmp_dir = Path(__file__).parent.parent / "tmp"
    tmp_dir.mkdir(exist_ok=True)
    src_db = tmp_dir / f"import_{uuid.uuid4().hex}.db"
    src_db.write_bytes(raw)

    try:
        try:
            src_conn = sqlite3.connect(str(src_db))
            dst_conn = sqlite3.connect(str(STOCK_DB_PATH))
            src_cur  = src_conn.cursor()
            dst_cur  = dst_conn.cursor()

            # 校验来源文件包含目标表
            src_cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            src_tables = {r[0] for r in src_cur.fetchall()}
            if "stocks" not in src_tables or "monthly_kline" not in src_tables:
                src_conn.close(); dst_conn.close()
                return {"success": False, "message": "备份文件格式不正确（缺少 stocks 或 monthly_kline 表）"}

            # 确定要操作的市场列表
            if market:
                markets_to_import = [market]
            else:
                src_cur.execute("SELECT DISTINCT market FROM stocks WHERE market IS NOT NULL")
                markets_to_import = [r[0] for r in src_cur.fetchall()]

            if mode == "replace":
                for m in markets_to_import:
                    dst_cur.execute("DELETE FROM monthly_kline WHERE market=?", (m,))
                    dst_cur.execute("DELETE FROM stocks WHERE market=?", (m,))

            # 写入 stocks
            if market:
                src_cur.execute("SELECT * FROM stocks WHERE market=?", (market,))
            else:
                src_cur.execute("SELECT * FROM stocks")
            stock_rows = src_cur.fetchall()
            dst_cur.executemany(
                "INSERT OR REPLACE INTO stocks VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                stock_rows
            )

            # 写入 monthly_kline（跳过自增 id）
            cols = "ts_code,trade_date,year,month,open,close,high,low,vol,amount,pct_chg,data_source,market,currency"
            if market:
                src_cur.execute(f"SELECT {cols} FROM monthly_kline WHERE market=?", (market,))
            else:
                src_cur.execute(f"SELECT {cols} FROM monthly_kline")
            kline_rows = src_cur.fetchall()
            dst_cur.executemany(
                f"INSERT OR IGNORE INTO monthly_kline ({cols}) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                kline_rows
            )

            dst_conn.commit()
            src_conn.close()
            dst_conn.close()

            return {
                "success": True,
                "message": f"{'全量替换' if mode == 'replace' else '合并'}成功",
                "stocks":  len(stock_rows),
                "klines":  len(kline_rows),
            }
        except Exception as e:
            return {"success": False, "message": f"还原失败: {e}"}
    finally:
        for suffix in ('', '-wal', '-shm'):
            p = Path(str(src_db) + suffix)
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass
