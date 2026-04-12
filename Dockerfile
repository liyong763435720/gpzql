# 使用Python 3.11官方镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 安装系统依赖（gcc + libffi-dev 用于编译型依赖）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制并安装Python依赖（先于代码，充分利用层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建数据目录并赋权
RUN mkdir -p /app/data && chmod 777 /app/data

# 设置启动脚本权限
RUN chmod +x /app/docker-entrypoint.sh

# 暴露端口
EXPOSE 8588

ENTRYPOINT ["/app/docker-entrypoint.sh"]
