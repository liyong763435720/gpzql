# Linux部署指南

## 快速开始

### 方式一：一键部署（推荐）

1. **运行部署脚本**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

2. **启动服务**
   - 前台运行：`./start_service.sh`
   - 后台运行：`./start_service_background.sh`
   - systemd服务（推荐）：`sudo ./install_service.sh && sudo systemctl start lof-arbitrage`

3. **访问应用**
   - 打开浏览器访问：`http://服务器IP:8505`
   - 默认管理员账号：`admin` / `admin123`

### 方式二：安装为systemd服务（开机自启）

1. **以root权限运行**
   ```bash
   sudo ./install_service.sh
   ```

2. **服务管理**
   - 启动服务：`sudo systemctl start lof-arbitrage`
   - 停止服务：`sudo systemctl stop lof-arbitrage`
   - 查看状态：`sudo systemctl status lof-arbitrage`
   - 查看日志：`sudo journalctl -u lof-arbitrage -f`
   - 重启服务：`sudo systemctl restart lof-arbitrage`

3. **卸载服务**
   ```bash
   sudo ./uninstall_service.sh
   ```

## 系统要求

- **操作系统**：Linux（Ubuntu 18.04+, CentOS 7+, Debian 10+等）
- **Python版本**：Python 3.11 或更高版本（推荐3.11）
- **权限**：普通用户权限（安装systemd服务需要root权限）

## 部署步骤详解

### 1. 检查Python环境

部署脚本会自动检查：
- Python 3.11是否已安装
- Python版本是否符合要求（3.7+）
- pip是否可用

如果未安装Python 3.11：

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install python3.11 python3.11-venv python3-pip
```

**CentOS/RHEL:**
```bash
sudo yum install python311 python311-pip
# 或使用dnf（CentOS 8+）
sudo dnf install python3.11 python3.11-pip
```

### 2. 安装依赖

部署脚本会自动执行：
```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

主要依赖包：
- Flask >= 2.3.0
- requests >= 2.28.0
- tushare >= 1.2.89
- akshare >= 1.11.0
- baostock >= 0.8.8

### 3. 创建目录结构

部署脚本会自动创建：
- `logs/` - 日志目录
- `data/` - 数据目录

### 4. 创建启动脚本

部署脚本会自动创建：
- `start_service.sh` - 前台启动
- `start_service_background.sh` - 后台启动（使用nohup）
- `stop_service.sh` - 停止服务
- `install_service.sh` - 安装systemd服务

## 服务配置

### 端口配置

默认端口：`8505`

如需修改端口，编辑 `app.py` 文件：
```python
app.run(debug=True, host='0.0.0.0', port=8505)  # 修改端口号
```

### 防火墙设置

如果无法访问，请检查防火墙：

**Ubuntu/Debian (ufw):**
```bash
sudo ufw allow 8505/tcp
sudo ufw reload
```

**CentOS/RHEL (firewalld):**
```bash
sudo firewall-cmd --permanent --add-port=8505/tcp
sudo firewall-cmd --reload
```

**iptables:**
```bash
sudo iptables -A INPUT -p tcp --dport 8505 -j ACCEPT
sudo iptables-save
```

### 开机自启

**方式一：systemd服务（推荐）**
- 运行 `sudo ./install_service.sh`
- 服务将随系统自动启动

**方式二：使用nohup（不推荐用于生产环境）**
- 使用 `./start_service_background.sh`
- 需要手动添加到启动脚本

## 故障排除

### 问题1：Python未找到

**解决方案**：
1. 确认Python 3.11已安装：`python3 --version`
2. 检查PATH环境变量
3. 使用完整路径：`/usr/bin/python3.11`

### 问题2：依赖安装失败

**解决方案**：
1. 检查网络连接
2. 使用国内镜像源：
   ```bash
   pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
   ```
3. 升级pip：`python3 -m pip install --upgrade pip`

### 问题3：端口被占用

**解决方案**：
1. 查找占用端口的进程：
   ```bash
   sudo netstat -tlnp | grep :8505
   # 或
   sudo lsof -i :8505
   ```
2. 结束进程或修改应用端口

### 问题4：服务无法启动

**解决方案**：
1. 检查Python路径是否正确
2. 检查app.py文件是否存在
3. 查看错误日志：
   ```bash
   tail -f logs/service.log
   # 或（如果使用systemd）
   sudo journalctl -u lof-arbitrage -f
   ```
4. 尝试前台运行查看详细错误信息

### 问题5：无法访问Web界面

**解决方案**：
1. 确认服务已启动：`ps aux | grep app.py`
2. 检查防火墙设置
3. 尝试访问 `http://127.0.0.1:8505`
4. 检查端口是否被占用
5. 检查app.py中的host设置（应为`0.0.0.0`以允许外部访问）

### 问题6：systemd服务启动失败

**解决方案**：
1. 检查服务文件：`cat /etc/systemd/system/lof-arbitrage.service`
2. 检查用户权限：确保服务用户有权限访问项目目录
3. 查看详细日志：`sudo journalctl -u lof-arbitrage -n 50`
4. 检查工作目录和Python路径是否正确

## 数据备份

重要数据文件：
- `users.json` - 用户数据
- `arbitrage_records.json` - 套利记录
- `notifications.json` - 通知数据
- `user_config.json` - 用户配置
- `logs/` - 日志目录

建议定期备份这些文件：
```bash
tar -czf backup-$(date +%Y%m%d).tar.gz users.json arbitrage_records.json notifications.json user_config.json logs/
```

## 卸载

### 完全卸载

1. **停止服务**
   ```bash
   sudo systemctl stop lof-arbitrage  # 如果安装了systemd服务
   ./stop_service.sh  # 如果使用后台脚本
   ```

2. **删除数据**
   ```bash
   ./uninstall.sh
   ```

3. **卸载systemd服务**（如果已安装）
   ```bash
   sudo ./uninstall_service.sh
   ```

4. **删除项目目录**（可选）
   ```bash
   rm -rf /path/to/lof1
   ```

### 保留数据卸载

如果只想卸载服务但保留数据：
1. 仅运行 `sudo ./uninstall_service.sh`（如果安装了systemd服务）
2. 不要运行 `./uninstall.sh`

## 高级配置

### 修改服务端口

编辑 `app.py`：
```python
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8505)  # 修改这里
```

然后重启服务：
```bash
sudo systemctl restart lof-arbitrage
```

### 修改服务用户

编辑systemd服务文件：
```bash
sudo nano /etc/systemd/system/lof-arbitrage.service
```

修改 `User=` 行，然后：
```bash
sudo systemctl daemon-reload
sudo systemctl restart lof-arbitrage
```

### 使用虚拟环境（推荐）

如果使用虚拟环境：

1. 创建虚拟环境：
   ```bash
   python3.11 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. 修改systemd服务文件中的ExecStart：
   ```ini
   ExecStart=/path/to/project/venv/bin/python /path/to/project/app.py
   ```

### 使用Nginx反向代理（生产环境推荐）

1. 安装Nginx：
   ```bash
   sudo apt-get install nginx  # Ubuntu/Debian
   sudo yum install nginx      # CentOS/RHEL
   ```

2. 创建Nginx配置：
   ```nginx
   server {
       listen 80;
       server_name your-domain.com;
       
       location / {
           proxy_pass http://127.0.0.1:8505;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
       }
   }
   ```

3. 启用配置并重启Nginx：
   ```bash
   sudo ln -s /etc/nginx/sites-available/lof-arbitrage /etc/nginx/sites-enabled/
   sudo nginx -t
   sudo systemctl restart nginx
   ```

## 性能优化

### 使用Gunicorn（生产环境推荐）

1. 安装Gunicorn：
   ```bash
   pip install gunicorn
   ```

2. 修改systemd服务文件：
   ```ini
   ExecStart=/path/to/venv/bin/gunicorn -w 4 -b 0.0.0.0:8505 app:app
   ```

### 日志轮转

创建logrotate配置：
```bash
sudo nano /etc/logrotate.d/lof-arbitrage
```

内容：
```
/path/to/project/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 user group
}
```

## 技术支持

如遇问题，请检查：
1. Python版本是否符合要求（3.11+）
2. 所有依赖是否安装成功
3. 端口是否被占用
4. 防火墙是否允许访问
5. 查看应用日志文件
6. 查看systemd日志（如果使用systemd）
