# -*- coding: utf-8 -*-
"""
lof1 Flask 应用的包装模块。
将 lof1 目录加入 sys.path 后导入 Flask app，
由 FastAPI 通过 WSGIMiddleware 挂载至 /lof1。
"""

import sys
import os
import shutil

LOF1_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lof1")

# 若 lof1/config.py 不存在，自动从 config.example.py 复制
_config_py = os.path.join(LOF1_DIR, "config.py")
_config_example = os.path.join(LOF1_DIR, "config.example.py")
if not os.path.exists(_config_py) and os.path.exists(_config_example):
    shutil.copy2(_config_example, _config_py)
    print("[lof1] 未检测到 config.py，已自动从 config.example.py 复制")

# 确保 lof1 目录优先于其他路径，避免与主项目 app 包冲突
if LOF1_DIR not in sys.path:
    sys.path.insert(0, LOF1_DIR)

# 以模块名 "lof1_app_module" 加载，防止与主项目 "app" 包命名冲突
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "lof1_app_module",
    os.path.join(LOF1_DIR, "app.py"),
)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

# 导出 Flask app 实例
lof1_flask_app = _module.app

# 启动后台基金数据更新器（仅在集成模式下手动启动）
_background_updater = getattr(_module, "background_updater", None)
if _background_updater is not None:
    try:
        _background_updater.start()
        print("[lof1] 后台基金数据更新器已启动")
    except Exception as e:
        print(f"[lof1] 启动后台更新器失败: {e}")
