# -*- coding: utf-8 -*-
"""
后台基金数据更新器
定时更新数据库中的基金数据
"""

import threading
import time
from datetime import datetime
from typing import List, Dict
from fund_data_manager_db import FundDataManagerDB
from data_fetcher import LOFDataFetcher
from arbitrage_calculator import ArbitrageCalculator
from config import TRADE_FEES, ARBITRAGE_THRESHOLD, DATA_SOURCE
from webhook_notifier import webhook_notifier


class BackgroundFundUpdater:
    """后台基金数据更新器"""
    
    def __init__(self, db_path: str = "lof_arbitrage.db", update_interval: int = 60):
        """
        初始化后台更新器
        
        Args:
            db_path: 数据库文件路径
            update_interval: 更新间隔（秒），默认60秒
        """
        self.fund_data_manager = FundDataManagerDB(db_path)
        self.data_fetcher = LOFDataFetcher()
        self.calculator = ArbitrageCalculator(threshold=ARBITRAGE_THRESHOLD, fees=TRADE_FEES)
        self.update_interval = update_interval
        self.running = False
        self.thread = None
        self.last_update_time = None
        # 最近一次批量刷新申购状态的时间（用于控制刷新频率）
        self.last_purchase_limit_update_time = None
        
        # 从配置中读取数据时效性设置
        data_freshness = DATA_SOURCE.get('data_freshness', {})
        self.price_nav_max_age_seconds = data_freshness.get('price_nav_max_age_seconds', 300)  # 默认5分钟
        self.purchase_limit_max_age_seconds = data_freshness.get('purchase_limit_max_age_seconds', 600)  # 默认10分钟
        self.purchase_limit_update_interval = data_freshness.get('purchase_limit_update_interval', 600)  # 默认10分钟
        
        # 从配置中读取备份清理设置
        backup_cleanup = DATA_SOURCE.get('backup_cleanup', {})
        self.backup_cleanup_enabled = backup_cleanup.get('enabled', True)
        self.backup_retention_days = backup_cleanup.get('retention_days', 30)  # 默认保留30天
        self.backup_cleanup_interval_hours = backup_cleanup.get('cleanup_interval_hours', 24)  # 默认每天检查一次
        self.last_backup_cleanup_time = None  # 上次清理备份的时间

        # 套利机会提醒：记录每只基金上次发送 Webhook 的时间，防刷（冷却 60 分钟）
        self._alert_cooldown: Dict[str, datetime] = {}  # key: "{username}_{fund_code}"
        self._alert_lock = threading.Lock()

    def _check_and_alert(self, fund_code: str, fund_name: str, arbitrage_result: Dict):
        """
        检测套利机会，对满足个性化阈值的用户触发 Webhook 推送和站内通知。
        带冷却机制：同一用户同一基金 60 分钟内不重复推送。
        """
        if not arbitrage_result or not arbitrage_result.get('has_opportunity'):
            return

        profit_rate = arbitrage_result.get('profit_rate', 0)
        arbitrage_type = arbitrage_result.get('arbitrage_type', '')
        # 统一为内部标识符
        arb_type_key = 'premium' if '溢价' in arbitrage_type else 'discount'

        try:
            from database_models import get_db_manager
            db = get_db_manager(self.fund_data_manager.db_manager.db_path)
            session = db.get_session()
            try:
                from database_models import User
                users = session.query(User).all()
                for user in users:
                    settings = user.settings or {}
                    favorites = user.favorites or []
                    # 只对自选基金用户推送
                    if fund_code not in favorites:
                        continue

                    # 取该基金的个性化阈值（未设置则用用户全局阈值，再降级用系统默认）
                    fund_thresholds = settings.get('fund_alert_thresholds', {})
                    threshold = fund_thresholds.get(fund_code)
                    if threshold is None:
                        arb_cfg = settings.get('arbitrage_threshold', {})
                        threshold = arb_cfg.get('min_profit_rate', ARBITRAGE_THRESHOLD.get('min_profit_rate', 0.005))
                        threshold = threshold * 100  # 转为百分比

                    if profit_rate < threshold:
                        continue

                    # 冷却检查
                    cooldown_minutes = settings.get('alert_cooldown_minutes', 60)
                    cooldown_key = f"{user.username}_{fund_code}"
                    with self._alert_lock:
                        last_sent = self._alert_cooldown.get(cooldown_key)
                        now = datetime.now()
                        if last_sent and (now - last_sent).total_seconds() < cooldown_minutes * 60:
                            continue
                        self._alert_cooldown[cooldown_key] = now

                    # 构造推送内容
                    title, content = webhook_notifier.format_arbitrage_message(
                        fund_code=fund_code,
                        fund_name=fund_name,
                        arbitrage_type=arb_type_key,
                        profit_rate=profit_rate,
                        annualized_rate=arbitrage_result.get('annualized_rate', 0),
                        price=arbitrage_result.get('price', 0),
                        nav=arbitrage_result.get('nav', 0),
                        price_diff_pct=arbitrage_result.get('price_diff_pct', 0),
                        holding_days=arbitrage_result.get('holding_days', 3),
                        net_profit_10k=arbitrage_result.get('net_profit_10k', 0),
                    )

                    # 发送 Webhook
                    webhook_cfg = settings.get('webhook', {})
                    if webhook_cfg.get('enabled') and webhook_cfg.get('url'):
                        try:
                            result = webhook_notifier.send(webhook_cfg, title, content)
                            if not result['success']:
                                print(f"[Webhook] 推送失败 {user.username}: {result['message']}")
                        except Exception as e:
                            print(f"[Webhook] 推送异常 {user.username}: {e}")

                    # 写站内通知
                    try:
                        from notification_manager_db import NotificationManagerDB, NotificationType
                        notif_mgr = NotificationManagerDB(self.fund_data_manager.db_manager.db_path)
                        notif_mgr.create_notification(
                            username=user.username,
                            notification_type=NotificationType.ARBITRAGE_OPPORTUNITY,
                            title=title,
                            content=content,
                            data={
                                'fund_code': fund_code,
                                'fund_name': fund_name,
                                'arbitrage_type': arb_type_key,
                                'profit_rate': profit_rate,
                                'annualized_rate': arbitrage_result.get('annualized_rate', 0),
                            }
                        )
                    except Exception as e:
                        print(f"[Alert] 写站内通知失败 {user.username}: {e}")

            finally:
                session.close()
        except Exception as e:
            print(f"[Alert] 套利提醒检测失败 {fund_code}: {e}")

    def update_single_fund(self, fund_code: str, fund_name: str = None) -> bool:
        """更新单个基金数据"""
        try:
            # 获取基金信息（先获取，因为可能需要从中提取名称）
            fund_info = self.data_fetcher.get_fund_info(fund_code)
            
            # 优先从 LOF_FUNDS 全局变量获取基金名称
            if not fund_name:
                from config import LOF_FUNDS
                fund_name = LOF_FUNDS.get(fund_code, '')
            
            # 如果还是没有，尝试从数据获取器获取中文名称
            if not fund_name:
                try:
                    fund_name = self.data_fetcher.get_fund_chinese_name(fund_code)
                except:
                    pass
            
            # 如果还是没有，尝试从 fund_info 中获取（如果有）
            if not fund_name and fund_info:
                # fund_info 可能包含 name 字段
                fund_name = fund_info.get('name', '') or fund_info.get('fund_name', '')
            
            # 如果还是没有，使用基金代码（作为最后备选）
            if not fund_name:
                fund_name = fund_code
            if not fund_info:
                return False
            
            # 计算套利数据
            arbitrage_result = self.calculator.calculate_arbitrage(fund_info)
            if not arbitrage_result:
                # 即使没有套利机会，也保存基础数据
                arbitrage_result = {
                    'fund_code': fund_code,
                    'price': fund_info.get('price', 0),
                    'nav': fund_info.get('nav', 0),
                    'price_diff': 0,
                    'price_diff_pct': 0,
                    'profit_rate': 0,
                    'arbitrage_type': None,
                    'has_opportunity': False
                }
            
            # 构建数据库记录
            fund_data = {
                'fund_code': fund_code,
                'fund_name': fund_name,
                'price': fund_info.get('price'),
                'price_date': fund_info.get('price_date', ''),
                'change_pct': fund_info.get('change_pct', 0) * 100 if fund_info.get('change_pct') else None,  # 转换为百分比
                'nav': fund_info.get('nav'),
                'nav_date': fund_info.get('nav_date', ''),
                'price_diff': arbitrage_result.get('price_diff', 0),
                'price_diff_pct': arbitrage_result.get('price_diff_pct', 0),
                'arbitrage_type': arbitrage_result.get('arbitrage_type'),
                'profit_rate': arbitrage_result.get('profit_rate', 0),
                'purchase_limit': fund_info.get('purchase_limit', {}),
                'data_source': fund_info.get('data_source', 'unknown')
            }
            
            # 更新到数据库
            result = self.fund_data_manager.update_fund_data(fund_code, fund_data)

            # 数据更新成功后，检测套利机会并触发提醒
            if result and arbitrage_result.get('has_opportunity'):
                try:
                    self._check_and_alert(fund_code, fund_name, arbitrage_result)
                except Exception as alert_err:
                    print(f"套利提醒触发异常 {fund_code}: {alert_err}")

            # 追加一条历史折溢价率记录（仅在价格和净值都有效时记录）
            if result and fund_info.get('price', 0) > 0 and fund_info.get('nav', 0) > 0:
                try:
                    from database_models import get_db_manager, PriceHistory
                    db = get_db_manager(self.fund_data_manager.db_manager.db_path)
                    session = db.get_session()
                    try:
                        history = PriceHistory(
                            fund_code=fund_code,
                            price=fund_info.get('price'),
                            nav=fund_info.get('nav'),
                            price_diff_pct=arbitrage_result.get('price_diff_pct', 0),
                            profit_rate=arbitrage_result.get('profit_rate', 0),
                        )
                        session.add(history)
                        session.commit()
                    finally:
                        session.close()
                except Exception as hist_err:
                    print(f"写历史记录失败 {fund_code}: {hist_err}")

            return result
        except Exception as e:
            print(f"更新基金数据失败 {fund_code}: {e}")
            return False
    
    def update_all_funds(self, fund_codes: List[str] = None):
        """批量更新所有基金数据"""
        try:
            if not fund_codes:
                # 如果没有提供基金代码列表，从数据库获取
                all_funds = self.fund_data_manager.get_all_funds_data()
                fund_codes = [f['fund_code'] for f in all_funds]
            
            if not fund_codes:
                print("没有需要更新的基金")
                return
            
            print(f"开始更新 {len(fund_codes)} 只基金数据...")
            updated_count = 0
            failed_count = 0
            
            # 从全局变量获取基金名称映射
            from config import LOF_FUNDS
            
            for i, fund_code in enumerate(fund_codes, 1):
                try:
                    # 优先从 LOF_FUNDS 获取基金名称
                    fund_name = LOF_FUNDS.get(fund_code, '')
                    
                    # 如果全局变量中没有，尝试从数据库获取
                    if not fund_name:
                        fund_data = self.fund_data_manager.get_fund_data(fund_code)
                        if fund_data and fund_data.get('fund_name') and fund_data.get('fund_name') != fund_code:
                            fund_name = fund_data.get('fund_name')
                    
                    if self.update_single_fund(fund_code, fund_name):
                        updated_count += 1
                    else:
                        failed_count += 1
                    
                    # 每更新10只基金，打印一次进度
                    if i % 10 == 0:
                        print(f"  进度: {i}/{len(fund_codes)}, 成功: {updated_count}, 失败: {failed_count}")
                    
                    # 避免请求过快，稍微延迟
                    time.sleep(0.1)
                except Exception as e:
                    failed_count += 1
                    print(f"更新基金 {fund_code} 失败: {e}")
            
            self.last_update_time = datetime.now()
            print(f"更新完成: 成功 {updated_count}, 失败 {failed_count}, 总计 {len(fund_codes)}")
        except Exception as e:
            print(f"批量更新基金数据失败: {e}")
    
    def update_stale_funds(self, max_age_seconds: int = None):
        """更新过期的基金数据"""
        try:
            # 如果没有指定，使用配置中的默认值
            if max_age_seconds is None:
                max_age_seconds = self.price_nav_max_age_seconds
            
            stale_funds = self.fund_data_manager.get_stale_funds(max_age_seconds)
            if stale_funds:
                print(f"发现 {len(stale_funds)} 只过期基金，开始更新...")
                self.update_all_funds(stale_funds)
            else:
                print("没有过期的基金数据")
        except Exception as e:
            print(f"更新过期基金数据失败: {e}")
    
    def update_all_purchase_limits(self, max_age_seconds: int = None):
        """
        批量刷新所有基金的申购状态（仅更新 purchase_limit，不阻塞前端请求）
        
        Args:
            max_age_seconds: 两次全量刷新之间的最小间隔（秒），如果为None则使用配置中的值
        """
        try:
            # 如果没有指定，使用配置中的默认值
            if max_age_seconds is None:
                max_age_seconds = self.purchase_limit_update_interval

            now = datetime.now()
            if (
                self.last_purchase_limit_update_time is not None and
                (now - self.last_purchase_limit_update_time).total_seconds() < max_age_seconds
            ):
                # 距离上次刷新时间太短，本轮跳过（避免过于频繁地访问第三方数据源）
                return

            # 预热 AKShare 缓存（一次性下载全量申购数据，后续 _get_akshare_limit 只读缓存）
            self.data_fetcher._warm_akshare_cache()

            all_funds = self.fund_data_manager.get_all_funds_data()
            if not all_funds:
                print("数据库中没有基金数据，跳过申购状态刷新")
                return
            
            fund_codes = [f['fund_code'] for f in all_funds]
            if not fund_codes:
                print("基金代码列表为空，跳过申购状态刷新")
                return
            
            print(f"开始刷新 {len(fund_codes)} 只基金的申购状态（后台任务）...")
            updated_count = 0
            failed_count = 0
            
            # 延迟导入，避免循环导入问题
            from config import LOF_FUNDS
            
            for i, fund_code in enumerate(fund_codes, 1):
                try:
                    # 使用已有的限购信息获取逻辑（内部会优先使用 akshare 批量数据）
                    purchase_limit = self.data_fetcher.get_fund_purchase_limit(fund_code)
                    
                    if purchase_limit:
                        # 读取现有基金数据
                        fund_data = self.fund_data_manager.get_fund_data(fund_code)
                        if fund_data:
                            fund_data['purchase_limit'] = purchase_limit
                        else:
                            # 如果基金数据不存在，最少创建一条只包含申购状态的记录
                            fund_name = LOF_FUNDS.get(fund_code, fund_code)
                            fund_data = {
                                'fund_code': fund_code,
                                'fund_name': fund_name,
                                'purchase_limit': purchase_limit
                            }
                        self.fund_data_manager.update_fund_data(fund_code, fund_data)
                        updated_count += 1
                    
                    # 每更新10只基金，打印一次进度，方便观察
                    if i % 10 == 0:
                        print(f"  申购状态进度: {i}/{len(fund_codes)}, 成功: {updated_count}, 失败: {failed_count}")
                    
                    # 稍作延迟，避免对第三方数据源压力过大
                    time.sleep(0.1)
                except Exception as e:
                    failed_count += 1
                    print(f"刷新基金 {fund_code} 申购状态失败: {e}")
            
            self.last_purchase_limit_update_time = datetime.now()
            print(f"申购状态刷新完成：成功 {updated_count}，失败 {failed_count}，总计 {len(fund_codes)}")
        except Exception as e:
            print(f"批量刷新申购状态失败: {e}")
    
    def cleanup_old_backups(self):
        """
        清理过期的备份文件
        根据配置的保留天数，删除超过保留期的备份文件
        """
        if not self.backup_cleanup_enabled:
            return
        
        try:
            now = datetime.now()
            
            # 检查是否需要执行清理（根据清理间隔）
            if self.last_backup_cleanup_time is not None:
                hours_since_last_cleanup = (now - self.last_backup_cleanup_time).total_seconds() / 3600
                if hours_since_last_cleanup < self.backup_cleanup_interval_hours:
                    return  # 还没到清理时间
            
            import os
            from datetime import timedelta
            
            backup_dir = "json_backup"
            if not os.path.exists(backup_dir):
                return  # 备份目录不存在，无需清理
            
            # 计算过期时间点
            retention_delta = timedelta(days=self.backup_retention_days)
            cutoff_time = now - retention_delta
            
            # 获取所有备份文件
            backup_files = []
            for filename in os.listdir(backup_dir):
                if filename.endswith('.json') and '.backup_' in filename:
                    filepath = os.path.join(backup_dir, filename)
                    if os.path.isfile(filepath):
                        backup_files.append(filepath)
            
            if not backup_files:
                return  # 没有备份文件
            
            deleted_count = 0
            total_size_freed = 0
            
            for filepath in backup_files:
                try:
                    # 从文件名中提取时间戳
                    # 格式：文件名.backup_YYYYMMDD_HHMMSS
                    filename = os.path.basename(filepath)
                    if '.backup_' in filename:
                        # 提取时间戳部分
                        timestamp_str = filename.split('.backup_')[-1]
                        # 移除.json后缀（如果有）
                        if timestamp_str.endswith('.json'):
                            timestamp_str = timestamp_str[:-5]
                        
                        # 解析时间戳
                        try:
                            file_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                            
                            # 如果文件时间早于过期时间点，删除文件
                            if file_time < cutoff_time:
                                file_size = os.path.getsize(filepath)
                                os.remove(filepath)
                                deleted_count += 1
                                total_size_freed += file_size
                                print(f"已删除过期备份文件: {filename} (创建时间: {file_time.strftime('%Y-%m-%d %H:%M:%S')})")
                        except ValueError:
                            # 时间戳格式不正确，跳过
                            continue
                except Exception as e:
                    print(f"清理备份文件失败 {filepath}: {e}")
            
            if deleted_count > 0:
                size_mb = total_size_freed / (1024 * 1024)
                print(f"备份清理完成：删除 {deleted_count} 个过期备份文件，释放空间 {size_mb:.2f} MB")
                self.last_backup_cleanup_time = now
            else:
                # 即使没有删除文件，也更新最后清理时间，避免频繁检查
                self.last_backup_cleanup_time = now
        except Exception as e:
            print(f"清理备份文件失败: {e}")
    
    def _update_loop(self):
        """更新循环（后台线程）"""
        while self.running:
            try:
                # 如果数据库为空（首次安装），先进行全量初始填充
                all_funds_in_db = self.fund_data_manager.get_all_funds_data()
                if not all_funds_in_db:
                    print("[后台] 数据库为空，开始全量初始化（首次安装）...")
                    self.force_update_all()
                else:
                    # 检查 LOF_FUNDS 中是否有尚未入库的基金
                    # （auto_discover_funds 异步扩充了 LOF_FUNDS，但 force_update_all 可能已在扩充前执行）
                    try:
                        from config import LOF_FUNDS as _current_lof
                        db_codes = {f['fund_code'] for f in all_funds_in_db}
                        missing = [c for c in _current_lof if c not in db_codes]
                        if missing:
                            print(f"[后台] 发现 {len(missing)} 只基金在 LOF_FUNDS 中但尚未入库，开始补充更新...")
                            self.update_all_funds(missing)
                    except Exception as _e:
                        print(f"[后台] 检查缺失基金失败: {_e}")

                # 优先刷新申购状态（用户最关注，且不受价格更新影响）
                self.update_all_purchase_limits()

                # 再更新过期的价格/净值
                self.update_stale_funds()

                # 定期清理过期备份文件（防止磁盘堆积）
                self.cleanup_old_backups()

                # 等待指定时间
                time.sleep(self.update_interval)
            except Exception as e:
                print(f"后台更新循环出错: {e}")
                time.sleep(self.update_interval)
    
    def start(self):
        """启动后台更新任务"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._update_loop, daemon=True)
        self.thread.start()
        print(f"后台基金数据更新器已启动，更新间隔: {self.update_interval}秒")
        print(f"  价格/净值过期时间: {self.price_nav_max_age_seconds}秒 ({self.price_nav_max_age_seconds/60:.1f}分钟)")
        print(f"  申购状态过期时间: {self.purchase_limit_max_age_seconds}秒 ({self.purchase_limit_max_age_seconds/60:.1f}分钟)")
        print(f"  申购状态刷新间隔: {self.purchase_limit_update_interval}秒 ({self.purchase_limit_update_interval/60:.1f}分钟)")
        if self.backup_cleanup_enabled:
            print(f"  备份清理: 已启用，保留 {self.backup_retention_days} 天，每 {self.backup_cleanup_interval_hours} 小时检查一次")
        else:
            print(f"  备份清理: 已禁用")
    
    def stop(self):
        """停止后台更新任务"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        print("后台基金数据更新器已停止")
    
    def force_update_all(self):
        """强制更新所有基金数据（立即执行）"""
        print("强制更新所有基金数据...")
        # 从全局变量获取基金列表
        try:
            from config import LOF_FUNDS
            fund_codes = list(LOF_FUNDS.keys()) if LOF_FUNDS else []
            if fund_codes:
                self.update_all_funds(fund_codes)
            else:
                # 如果全局变量为空，尝试从数据库获取
                all_funds = self.fund_data_manager.get_all_funds_data()
                if all_funds:
                    fund_codes = [f['fund_code'] for f in all_funds]
                    self.update_all_funds(fund_codes)
                else:
                    print("基金列表为空，无法更新")
        except Exception as e:
            print(f"强制更新失败: {e}")
