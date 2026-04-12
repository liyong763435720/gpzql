# Windows部署指南

## 快速开始

### 方式一：一键部署（推荐）

1. **运行部署脚本**
   ```
   双击运行 deploy.bat
   ```

2. **启动服务**
   - 前台运行：双击 `start_service.bat`
   - 后台运行：双击 `start_service_background.bat`

3. **访问应用**
   - 打开浏览器访问：`http://localhost:8505`
   - 默认管理员账号：`admin` / `admin123`

### 方式二：安装为Windows服务（开机自启）

1. **以管理员身份运行**
   ```
   右键点击 install_windows_service.bat
   选择"以管理员身份运行"
   ```

2. **服务管理**
   - 启动服务：`net start LOFArbitrageService`
   - 停止服务：`net stop LOFArbitrageService`
   - 查看状态：`sc query LOFArbitrageService`

3. **卸载服务**
   ```
   以管理员身份运行 uninstall_windows_service.bat
   ```

## 系统要求

- **操作系统**：Windows 7/8/10/11
- **Python版本**：Python 3.7 或更高版本
- **权限**：普通用户权限（安装服务需要管理员权限）

## 部署步骤详解

### 1. 检查Python环境

部署脚本会自动检查：
- Python是否已安装
- Python版本是否符合要求（3.7+）
- pip是否可用

如果未安装Python：
1. 访问 https://www.python.org/downloads/
2. 下载并安装Python 3.7+
3. **重要**：安装时勾选"Add Python to PATH"

### 2. 安装依赖

部署脚本会自动执行：
```bash
pip install --upgrade pip
pip install -r requirements.txt
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
- `start_service.bat` - 前台启动
- `start_service_background.bat` - 后台启动
- `stop_service.bat` - 停止服务

## 服务配置

### 端口配置

默认端口：`8505`

如需修改端口，编辑 `app.py` 文件：
```python
app.run(debug=True, host='0.0.0.0', port=8505)  # 修改端口号
```

### 防火墙设置

如果无法访问，请检查Windows防火墙：
1. 打开"Windows Defender 防火墙"
2. 点击"高级设置"
3. 添加入站规则，允许端口8505

### 开机自启

**方式一：Windows服务（推荐）**
- 运行 `install_windows_service.bat`（需要管理员权限）
- 服务将随系统自动启动

**方式二：任务计划程序**
1. 打开"任务计划程序"
2. 创建基本任务
3. 触发器：当计算机启动时
4. 操作：启动程序 `start_service_background.bat`

## 故障排除

### 问题1：Python未找到

**解决方案**：
1. 确认Python已安装
2. 检查环境变量PATH是否包含Python路径
3. 重新安装Python并勾选"Add Python to PATH"

### 问题2：依赖安装失败

**解决方案**：
1. 检查网络连接
2. 使用国内镜像源：
   ```bash
   pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
   ```
3. 升级pip：`python -m pip install --upgrade pip`

### 问题3：端口被占用

**解决方案**：
1. 查找占用端口的进程：
   ```bash
   netstat -ano | findstr :8505
   ```
2. 结束进程或修改应用端口

### 问题4：服务无法启动

**解决方案**：
1. 检查Python路径是否正确
2. 检查app.py文件是否存在
3. 查看错误日志
4. 尝试前台运行查看详细错误信息

### 问题5：无法访问Web界面

**解决方案**：
1. 确认服务已启动
2. 检查防火墙设置
3. 尝试访问 `http://127.0.0.1:8505`
4. 检查端口是否被占用

## 数据备份

重要数据文件：
- `users.json` - 用户数据
- `arbitrage_records.json` - 套利记录
- `notifications.json` - 通知数据
- `user_config.json` - 用户配置

建议定期备份这些文件。

## 卸载

### 完全卸载

1. **停止服务**
   ```
   运行 stop_service.bat
   或
   net stop LOFArbitrageService（如果安装了服务）
   ```

2. **删除数据**
   ```
   运行 uninstall.bat
   ```

3. **卸载Windows服务**（如果已安装）
   ```
   以管理员身份运行 uninstall_windows_service.bat
   ```

4. **删除项目目录**（可选）
   - 手动删除整个项目文件夹

### 保留数据卸载

如果只想卸载服务但保留数据：
1. 仅运行 `uninstall_windows_service.bat`（如果安装了服务）
2. 不要运行 `uninstall.bat`

## 高级配置

### 修改服务端口

编辑 `app.py`：
```python
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8505)  # 修改这里
```

### 修改服务名称

编辑 `install_windows_service.bat`：
```batch
sc create LOFArbitrageService ^
    DisplayName= "你的服务名称" ^
    ...
```

### 使用NSSM安装服务（更稳定）

如果使用sc命令安装失败，可以使用NSSM：

1. 下载NSSM：https://nssm.cc/download
2. 解压到项目目录
3. 运行：
   ```batch
   nssm install LOFArbitrageService "python" "app.py"
   nssm set LOFArbitrageService AppDirectory "项目路径"
   nssm start LOFArbitrageService
   ```

## 技术支持

如遇问题，请检查：
1. Python版本是否符合要求
2. 所有依赖是否安装成功
3. 端口是否被占用
4. 防火墙是否允许访问
5. 查看应用日志文件
