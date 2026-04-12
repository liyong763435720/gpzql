# -*- coding: utf-8 -*-
"""
涌金阁桌面启动器
优先使用 NSSM 服务，服务不存在时直接启动后端进程（内嵌模式）
"""
import sys
import os
import time
import subprocess
import ctypes
import threading

# ── 安装目录 ──────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    APP_DIR = os.path.dirname(sys.executable)
else:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))

PYTHON    = os.path.join(APP_DIR, "python", "python.exe")
SCRIPT    = os.path.join(APP_DIR, "start_prod.py")
NSSM      = os.path.join(APP_DIR, "nssm.exe")
SERVICE   = "YongJinGe"
URL       = "http://localhost:8588"
PING_URL  = f"{URL}/api/auth/current-user"
TIMEOUT   = 40

_backend_proc = None   # 直接启动的后端进程（内嵌模式）


# ── 工具函数 ──────────────────────────────────────────────
def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False


def service_state() -> str:
    """返回 RUNNING / STOPPED / PAUSED / NOT_FOUND"""
    try:
        out = subprocess.check_output(
            ["sc", "query", SERVICE],
            stderr=subprocess.DEVNULL, text=True
        )
        for line in out.splitlines():
            if "STATE" in line and ":" in line:
                return line.split(":")[-1].strip().split()[1]
    except Exception:
        pass
    return "NOT_FOUND"


def start_service():
    subprocess.run(["sc", "start", SERVICE],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def start_backend_direct():
    """不依赖服务，直接启动 Python 后端进程（桌面模式）"""
    global _backend_proc
    if not os.path.exists(PYTHON):
        show_error(f"找不到 Python 解释器：\n{PYTHON}\n\n请重新运行安装程序。")
        sys.exit(1)
    env = os.environ.copy()
    env["YONGJINGE_DESKTOP"] = "1"
    _backend_proc = subprocess.Popen(
        [PYTHON, SCRIPT],
        cwd=APP_DIR,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )


def wait_ready() -> bool:
    try:
        import urllib.request
        for _ in range(TIMEOUT):
            try:
                urllib.request.urlopen(PING_URL, timeout=1)
                return True
            except Exception:
                time.sleep(1)
    except Exception:
        pass
    return False


def show_error(msg: str):
    ctypes.windll.user32.MessageBoxW(0, msg, "涌金阁 - 启动失败", 0x10)


def on_window_closed():
    """窗口关闭时，如果是内嵌模式则停止后端"""
    if _backend_proc and _backend_proc.poll() is None:
        _backend_proc.terminate()


# ── 主流程 ────────────────────────────────────────────────
def main():
    state = service_state()

    if state == "NOT_FOUND":
        # 没有服务 → 直接启动后端（内嵌模式）
        start_backend_direct()

    elif state in ("STOPPED", "PAUSED"):
        # 服务存在但未运行 → 尝试启动服务
        if not is_admin():
            ctypes.windll.shell32.ShellExecuteW(
                None, "runas", sys.executable, "", None, 1
            )
            sys.exit(0)
        start_service()

    # 等待后端就绪
    if not wait_ready():
        if _backend_proc:
            _backend_proc.terminate()
        show_error(
            "后端服务启动超时，请检查：\n"
            f"  1. 端口 8588 是否被占用\n"
            f"  2. 手动运行查看报错：\n"
            f"     {PYTHON}\n"
            f"     {SCRIPT}"
        )
        sys.exit(1)

    # 打开原生窗口
    import webview
    window = webview.create_window(
        title="涌金阁 - 多市场量化分析平台",
        url=URL,
        width=1440,
        height=900,
        min_size=(1024, 680),
        resizable=True,
    )
    webview.start(on_window_closed)


if __name__ == "__main__":
    main()
