# -*- coding: utf-8 -*-
"""
SSE数据自动下载器
每天自动从上海证券交易所网站下载LOF基金列表和净值数据
"""
import os
import time
import requests
from datetime import datetime
from typing import Optional

class SSEDownloader:
    """SSE数据下载器"""
    
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.ensure_data_dir()
        self.download_urls = {
            'fund_list': 'https://fund.sse.org.cn/marketdata/lof/index.html',
            'nav_list': 'https://fund.sse.org.cn/marketdata/lof/index.html'  # 可能需要调整
        }
    
    def ensure_data_dir(self):
        """确保数据目录存在"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def download_fund_list(self) -> bool:
        """
        下载LOF基金列表
        
        Returns:
            是否下载成功
        """
        try:
            # 尝试使用Selenium下载
            return self._download_with_selenium('fund_list')
        except Exception as e:
            print(f"Selenium下载失败: {e}")
            # 如果Selenium失败，尝试其他方法
            return False
    
    def download_nav_list(self) -> bool:
        """
        下载LOF最新净值列表
        
        Returns:
            是否下载成功
        """
        try:
            return self._download_with_selenium('nav_list')
        except Exception as e:
            print(f"Selenium下载失败: {e}")
            return False
    
    def _download_with_selenium(self, file_type: str) -> bool:
        """
        使用Selenium下载文件
        
        Args:
            file_type: 文件类型 ('fund_list' 或 'nav_list')
            
        Returns:
            是否下载成功
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.action_chains import ActionChains
            import shutil
            
            # 配置Chrome选项
            chrome_options = Options()
            # 设置下载目录
            download_dir = os.path.abspath(self.data_dir)
            prefs = {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_argument('--headless')  # 无头模式
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            
            driver = webdriver.Chrome(options=chrome_options)
            try:
                # 访问页面
                url = 'https://fund.sse.org.cn/marketdata/lof/index.html'
                print(f"正在访问: {url}")
                driver.get(url)
                
                # 等待页面加载
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'report-container'))
                )
                time.sleep(5)  # 等待数据完全加载
                
                # 查找所有下载按钮
                # 可能的下载按钮选择器
                download_selectors = [
                    "//button[contains(text(), '下载')]",
                    "//a[contains(text(), '下载')]",
                    "//button[contains(text(), '导出')]",
                    "//a[contains(text(), '导出')]",
                    "//*[contains(@class, 'download')]",
                    "//*[contains(@class, 'export')]",
                    "//*[contains(@onclick, 'download')]",
                    "//*[contains(@onclick, 'export')]",
                ]
                
                all_download_buttons = []
                for selector in download_selectors:
                    try:
                        elements = driver.find_elements(By.XPATH, selector)
                        if elements:
                            all_download_buttons.extend(elements)
                    except:
                        continue
                
                print(f"找到 {len(all_download_buttons)} 个下载按钮")
                
                # 打印所有按钮的信息，用于调试
                for i, btn in enumerate(all_download_buttons):
                    try:
                        btn_text = btn.text.strip()
                        btn_id = btn.get_attribute('id') or ''
                        btn_class = btn.get_attribute('class') or ''
                        btn_onclick = btn.get_attribute('onclick') or ''
                        print(f"  按钮{i+1}: 文本='{btn_text}', id='{btn_id}', class='{btn_class}', onclick='{btn_onclick[:50]}'")
                    except:
                        pass
                
                # 根据file_type选择正确的下载按钮
                download_button = None
                if file_type == 'fund_list':
                    # 查找"LOF基金列表"相关的下载按钮
                    for btn in all_download_buttons:
                        btn_text = btn.text.strip()
                        btn_id = btn.get_attribute('id') or ''
                        btn_class = btn.get_attribute('class') or ''
                        btn_onclick = btn.get_attribute('onclick') or ''
                        # 检查按钮文本、id、class或onclick是否包含"基金列表"或"列表"（排除"净值"）
                        if (('基金列表' in btn_text or '列表' in btn_text) and '净值' not in btn_text) or \
                           ('基金列表' in btn_id or ('列表' in btn_id and '净值' not in btn_id)) or \
                           ('基金列表' in btn_class or ('列表' in btn_class and '净值' not in btn_class)) or \
                           ('基金列表' in btn_onclick or ('列表' in btn_onclick and '净值' not in btn_onclick)):
                            download_button = btn
                            print(f"找到LOF基金列表下载按钮: {btn_text}")
                            break
                    # 如果没找到，使用第一个按钮（假设第一个是基金列表）
                    if not download_button and all_download_buttons:
                        download_button = all_download_buttons[0]
                        print(f"使用第一个下载按钮（基金列表）: {download_button.text}")
                elif file_type == 'nav_list':
                    # 查找"LOF最新净值列表"相关的下载按钮
                    for btn in all_download_buttons:
                        btn_text = btn.text.strip()
                        btn_id = btn.get_attribute('id') or ''
                        btn_class = btn.get_attribute('class') or ''
                        btn_onclick = btn.get_attribute('onclick') or ''
                        # 检查按钮文本、id、class或onclick是否包含"净值"
                        if '净值' in btn_text or '净值' in btn_id or '净值' in btn_class or '净值' in btn_onclick:
                            download_button = btn
                            print(f"找到LOF最新净值列表下载按钮: {btn_text}")
                            break
                    # 如果没找到，使用第二个按钮（假设第一个是基金列表，第二个是净值列表）
                    if not download_button and len(all_download_buttons) >= 2:
                        download_button = all_download_buttons[1]
                        print(f"使用第二个下载按钮（净值列表）: {download_button.text}")
                    elif not download_button and all_download_buttons:
                        download_button = all_download_buttons[0]
                        print(f"使用第一个下载按钮（净值列表）: {download_button.text}")
                
                if not download_button:
                    # 如果找不到下载按钮，尝试查找表格并导出
                    print("未找到下载按钮，尝试查找数据表格...")
                    tables = driver.find_elements(By.TAG_NAME, 'table')
                    if tables:
                        print(f"找到 {len(tables)} 个表格")
                        # 可以尝试从表格中提取数据
                        # 这里先返回False，后续可以改进
                        return False
                
                # 点击下载按钮
                if download_button:
                    # 滚动到按钮位置
                    driver.execute_script("arguments[0].scrollIntoView(true);", download_button)
                    time.sleep(1)
                    
                    # 尝试点击
                    try:
                        download_button.click()
                    except:
                        # 如果普通点击失败，使用JavaScript点击
                        driver.execute_script("arguments[0].click();", download_button)
                    
                    # 记录点击前的时间戳，用于识别新下载的文件
                    before_download_time = time.time()
                    
                    # 等待下载完成
                    print("等待文件下载...")
                    time.sleep(10)
                    
                    # 检查下载的文件
                    downloaded_files = []
                    for filename in os.listdir(download_dir):
                        if filename.endswith(('.xlsx', '.xls', '.csv')):
                            file_path = os.path.join(download_dir, filename)
                            # 检查文件是否在点击后下载（最近1分钟内，且修改时间在点击之后）
                            file_mtime = os.path.getmtime(file_path)
                            if file_mtime > before_download_time - 5:  # 允许5秒的误差
                                downloaded_files.append((filename, file_path, file_mtime))
                    
                    if downloaded_files:
                        # 选择最新下载的文件
                        latest_file = max(downloaded_files, key=lambda x: x[2])
                        source_file = latest_file[1]
                        print(f"检测到新下载的文件: {latest_file[0]}")
                        print(f"源文件路径: {source_file}")
                        
                        # 重命名文件
                        target_filename = 'LOF基金列表.xlsx' if file_type == 'fund_list' else 'LOF最新净值列表.xlsx'
                        target_path = os.path.join(download_dir, target_filename)
                        print(f"目标文件路径: {target_path}")
                        
                        # 确保源文件存在
                        if not os.path.exists(source_file):
                            print(f"错误: 源文件不存在: {source_file}")
                            return False
                        
                        # 检查源文件和目标文件是否相同
                        source_abs = os.path.abspath(source_file)
                        target_abs = os.path.abspath(target_path)
                        
                        if source_abs == target_abs:
                            # 源文件和目标文件相同，说明下载的文件名已经是目标文件名
                            print(f"源文件和目标文件相同，无需移动: {target_path}")
                            # 验证文件确实存在
                            if os.path.exists(target_path):
                                print(f"下载成功: {target_path}")
                                return True
                            else:
                                print(f"错误: 文件不存在: {target_path}")
                                return False
                        else:
                            # 源文件和目标文件不同，需要移动
                            # 如果目标文件已存在，先备份
                            if os.path.exists(target_path):
                                backup_path = target_path + f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
                                try:
                                    shutil.move(target_path, backup_path)
                                    print(f"已备份旧文件: {backup_path}")
                                except Exception as e:
                                    print(f"备份文件失败: {e}")
                                    # 如果备份失败，尝试删除旧文件
                                    try:
                                        os.remove(target_path)
                                        print(f"已删除旧文件: {target_path}")
                                    except:
                                        pass
                            
                            # 移动文件
                            try:
                                shutil.move(source_file, target_path)
                                print(f"下载成功: {target_path}")
                                return True
                            except Exception as e:
                                print(f"移动文件失败: {e}")
                                # 尝试复制而不是移动
                                try:
                                    # 如果目标文件已存在，先删除
                                    if os.path.exists(target_path):
                                        os.remove(target_path)
                                    shutil.copy2(source_file, target_path)
                                    # 如果源文件和目标文件不同，删除源文件
                                    if os.path.abspath(source_file) != os.path.abspath(target_path):
                                        try:
                                            os.remove(source_file)
                                        except:
                                            pass
                                    print(f"下载成功（使用复制）: {target_path}")
                                    return True
                                except Exception as e2:
                                    print(f"复制文件也失败: {e2}")
                                    import traceback
                                    traceback.print_exc()
                                    return False
                    else:
                        print("未检测到新下载的文件")
                        # 列出所有Excel文件，帮助调试
                        excel_files = [f for f in os.listdir(download_dir) if f.endswith(('.xlsx', '.xls', '.csv'))]
                        if excel_files:
                            print(f"当前目录中的Excel文件: {excel_files}")
                        return False
                
                return False
                
            finally:
                driver.quit()
                
        except ImportError:
            print("Selenium未安装，无法自动下载。请安装: pip install selenium")
            return False
        except Exception as e:
            print(f"下载失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def download_all(self) -> dict:
        """
        下载所有数据
        
        Returns:
            下载结果字典
        """
        results = {
            'fund_list': False,
            'nav_list': False,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"\n开始下载SSE数据 ({results['timestamp']})...")
        
        print("\n1. 下载LOF基金列表...")
        results['fund_list'] = self.download_fund_list()
        
        print("\n2. 下载LOF最新净值列表...")
        results['nav_list'] = self.download_nav_list()
        
        print(f"\n下载完成:")
        print(f"  基金列表: {'成功' if results['fund_list'] else '失败'}")
        print(f"  净值列表: {'成功' if results['nav_list'] else '失败'}")
        
        return results


def is_trading_day(date: datetime = None) -> bool:
    """
    判断是否为交易日（简单判断：排除周末）
    注意：此方法不包含节假日判断，如需精确判断需要接入交易日历API
    
    Args:
        date: 要判断的日期，默认为今天
        
    Returns:
        是否为交易日
    """
    if date is None:
        date = datetime.now()
    
    # 排除周末（周六=5, 周日=6）
    weekday = date.weekday()
    if weekday >= 5:  # 周六或周日
        return False
    
    # TODO: 可以接入交易日历API（如akshare的交易日历）来精确判断节假日
    # 目前只排除周末，节假日需要手动处理或接入交易日历
    
    return True


def schedule_daily_download(hour: int = 9, minute: int = 0, only_trading_days: bool = True):
    """
    安排每天自动下载任务
    
    Args:
        hour: 每天下载的小时（默认9点）
        minute: 每天下载的分钟（默认0分）
        only_trading_days: 是否只在交易日下载（默认True）
    """
    import threading
    from datetime import datetime, timedelta
    
    downloader = SSEDownloader()
    
    def run_download():
        """执行下载任务"""
        try:
            # 如果设置了只在交易日下载，先检查是否为交易日
            if only_trading_days and not is_trading_day():
                print(f"今天不是交易日，跳过下载任务")
                return
            
            downloader.download_all()
        except Exception as e:
            print(f"自动下载任务失败: {e}")
    
    def schedule_next():
        """安排下一次下载"""
        now = datetime.now()
        target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # 如果设置了只在交易日下载，找到下一个交易日
        if only_trading_days:
            # 如果今天的时间已过，从明天开始找
            if target_time <= now:
                target_time += timedelta(days=1)
            
            # 找到下一个交易日（最多查找7天，避免无限循环）
            for _ in range(7):
                if is_trading_day(target_time):
                    break
                target_time += timedelta(days=1)
            else:
                print("警告: 7天内未找到交易日，使用原定时间")
        
        # 如果今天的时间已过且不限制交易日，安排明天
        elif target_time <= now:
            target_time += timedelta(days=1)
        
        # 计算等待时间（秒）
        wait_seconds = (target_time - now).total_seconds()
        
        print(f"下次自动下载时间: {target_time.strftime('%Y-%m-%d %H:%M:%S')} ({'交易日' if is_trading_day(target_time) else '非交易日'})")
        print(f"等待时间: {wait_seconds / 3600:.2f} 小时")
        
        # 创建定时器
        timer = threading.Timer(wait_seconds, run_and_reschedule)
        timer.daemon = True
        timer.start()
        return timer
    
    def run_and_reschedule():
        """执行下载并安排下一次"""
        run_download()
        schedule_next()
    
    # 立即安排第一次下载
    return schedule_next()


if __name__ == '__main__':
    # 测试下载
    downloader = SSEDownloader()
    downloader.download_all()
