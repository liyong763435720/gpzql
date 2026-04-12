"""
主程序入口（开发模式，支持自动重载）
"""
import sys
import os

# 确保项目根目录在 sys.path 中（uvicorn reloader 子进程需要）
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.api:app", host="0.0.0.0", port=8588, reload=True,
                reload_dirs=[_ROOT])

