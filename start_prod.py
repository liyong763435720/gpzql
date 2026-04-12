"""
生产环境启动脚本（不使用自动重载）
"""
import sys
import os
# 确保脚本所在目录在 sys.path 中（嵌入式 Python 不自动添加）
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.api:app",
        host="0.0.0.0",
        port=8588,
        reload=False,
        workers=1
    )


