# -*- coding: utf-8 -*-
"""
套利记录模块 - SQLite 数据库版本
"""

from datetime import datetime
from typing import Dict, List, Optional
from database_models import get_db_manager, ArbitrageRecord


class ArbitrageRecorderDB:
    """套利记录器（数据库版本）"""
    
    def __init__(self, db_path: str = "lof_arbitrage.db"):
        """
        初始化套利记录器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_manager = get_db_manager(db_path)
    
    def _get_session(self):
        """获取数据库会话"""
        return self.db_manager.get_session()
    
    def _record_to_dict(self, record: ArbitrageRecord) -> Dict:
        """将数据库记录转换为字典"""
        return {
            'id': record.id,
            'username': record.username,
            'fund_code': record.fund_code,
            'fund_name': record.fund_name,
            'arbitrage_type': record.arbitrage_type,
            'status': record.status,
            'initial_operation': {
                'type': record.initial_operation_type,
                'price': record.initial_price,
                'shares': record.initial_shares,
                'amount': record.initial_amount,
                'date': record.initial_date,
                'timestamp': record.initial_timestamp.isoformat() if record.initial_timestamp else None,
                'fees': record.initial_fees_info or {},
                'fee_amount': record.initial_fee_amount or 0.0
            },
            'final_operation': {
                'type': record.final_operation_type,
                'price': record.final_price,
                'shares': record.final_shares,
                'amount': record.final_amount,
                'date': record.final_date,
                'timestamp': record.final_timestamp.isoformat() if record.final_timestamp else None,
                'fees': record.final_fees_info or {},
                'fee_amount': record.final_fee_amount or 0.0
            } if record.final_operation_type else None,
            'profit': record.profit,
            'profit_rate': record.profit_rate,
            'net_profit': record.net_profit,
            'net_profit_rate': record.net_profit_rate,
            'created_at': record.created_at.isoformat() if record.created_at else None,
            'updated_at': record.updated_at.isoformat() if record.updated_at else None
        }
    
    def create_record(self, fund_code: str, fund_name: str, arbitrage_type: str, 
                     initial_price: float, initial_shares: float, initial_amount: float,
                     initial_date: str = None, username: str = None, trade_fees: Dict = None,
                     initial_operation_type: str = None) -> str:
        """创建套利记录（记录初始操作）"""
        record_id = f"{fund_code}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        if initial_date is None:
            initial_date = datetime.now().strftime('%Y-%m-%d')
        
        # 保存初始操作的费用信息
        initial_fees_info = {}
        if trade_fees:
            if arbitrage_type == 'premium':
                if initial_operation_type == 'on_exchange':
                    initial_fees_info = {
                        'fee_type': 'buy_commission',
                        'fee_rate': trade_fees.get('buy_commission', 0),
                        'operation_type': 'on_exchange'
                    }
                else:
                    initial_fees_info = {
                        'fee_type': 'subscribe_fee',
                        'fee_rate': trade_fees.get('subscribe_fee', 0),
                        'operation_type': 'off_exchange'
                    }
            else:
                initial_fees_info = {
                    'fee_type': 'buy_commission',
                    'fee_rate': trade_fees.get('buy_commission', 0),
                    'operation_type': 'on_exchange'
                }
        
        # 计算初始操作扣除的费用金额
        initial_fee_amount = 0
        if initial_fees_info.get('fee_rate'):
            initial_fee_amount = initial_amount * initial_fees_info['fee_rate']
        
        session = self._get_session()
        try:
            record = ArbitrageRecord(
                id=record_id,
                username=username,
                fund_code=fund_code,
                fund_name=fund_name,
                arbitrage_type=arbitrage_type,
                status='in_progress',
                initial_operation_type='subscribe' if arbitrage_type == 'premium' else 'buy',
                initial_price=initial_price,
                initial_shares=initial_shares,
                initial_amount=initial_amount,
                initial_date=initial_date,
                initial_timestamp=datetime.now(),
                initial_fees_info=initial_fees_info,
                initial_fee_amount=initial_fee_amount,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(record)
            session.commit()
            return record_id
        except Exception as e:
            session.rollback()
            print(f"创建套利记录失败: {e}")
            raise
        finally:
            session.close()
    
    def complete_record(self, record_id: str, final_price: float, final_shares: float = None,
                       final_amount: float = None, final_date: str = None, username: str = None,
                       trade_fees: Dict = None) -> bool:
        """完成套利记录（记录最终操作）"""
        session = self._get_session()
        try:
            query = session.query(ArbitrageRecord).filter(ArbitrageRecord.id == record_id)
            if username:
                query = query.filter(ArbitrageRecord.username == username)
            
            record = query.first()
            if not record:
                return False
            
            if record.status != 'in_progress':
                return False
            
            if final_date is None:
                final_date = datetime.now().strftime('%Y-%m-%d')
            
            if final_shares is None:
                final_shares = record.initial_shares
            
            # 计算最终金额（扣除交易费用）
            if final_amount is None:
                if record.arbitrage_type == 'premium':
                    if trade_fees:
                        sell_commission = trade_fees.get('sell_commission', 0)
                        stamp_tax = trade_fees.get('stamp_tax', 0)
                        total_cost = sell_commission + stamp_tax
                        final_amount = final_shares * final_price * (1 - total_cost)
                    else:
                        final_amount = final_shares * final_price
                else:
                    if trade_fees:
                        redeem_fee = trade_fees.get('redeem_fee', 0)
                        final_amount = final_shares * final_price * (1 - redeem_fee)
                    else:
                        final_amount = final_shares * final_price
            
            # 保存费用信息
            fees_info = {}
            final_fee_amount = 0
            if trade_fees:
                if record.arbitrage_type == 'premium':
                    sell_commission = trade_fees.get('sell_commission', 0)
                    stamp_tax = trade_fees.get('stamp_tax', 0)
                    total_cost_rate = sell_commission + stamp_tax
                    fees_info = {
                        'sell_commission': sell_commission,
                        'stamp_tax': stamp_tax,
                        'total_cost_rate': total_cost_rate
                    }
                    gross_amount = final_shares * final_price
                    final_fee_amount = gross_amount * total_cost_rate
                else:
                    redeem_fee = trade_fees.get('redeem_fee', 0)
                    fees_info = {
                        'redeem_fee': redeem_fee
                    }
                    gross_amount = final_shares * final_price
                    final_fee_amount = gross_amount * redeem_fee
            
            # 更新记录
            record.final_operation_type = 'sell' if record.arbitrage_type == 'premium' else 'redeem'
            record.final_price = final_price
            record.final_shares = final_shares
            record.final_amount = final_amount
            record.final_date = final_date
            record.final_timestamp = datetime.now()
            record.final_fees_info = fees_info
            record.final_fee_amount = final_fee_amount
            
            # 计算盈亏
            initial_amount = record.initial_amount
            initial_fee_amount = record.initial_fee_amount or 0.0
            
            record.profit = final_amount - initial_amount
            record.net_profit = final_amount - initial_amount - initial_fee_amount - final_fee_amount
            record.net_profit_rate = (record.net_profit / initial_amount * 100) if initial_amount > 0 else 0
            record.profit_rate = (record.profit / initial_amount * 100) if initial_amount > 0 else 0
            record.status = 'completed'
            record.updated_at = datetime.now()
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            print(f"完成套利记录失败: {e}")
            return False
        finally:
            session.close()
    
    def cancel_record(self, record_id: str, username: str = None) -> bool:
        """取消套利记录"""
        session = self._get_session()
        try:
            query = session.query(ArbitrageRecord).filter(ArbitrageRecord.id == record_id)
            if username:
                query = query.filter(ArbitrageRecord.username == username)
            
            record = query.first()
            if not record:
                return False
            
            if record.status == 'in_progress':
                record.status = 'cancelled'
                record.updated_at = datetime.now()
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_record(self, record_id: str, username: str = None) -> Optional[Dict]:
        """获取单条记录"""
        session = self._get_session()
        try:
            query = session.query(ArbitrageRecord).filter(ArbitrageRecord.id == record_id)
            if username:
                query = query.filter(ArbitrageRecord.username == username)
            
            record = query.first()
            if not record:
                return None
            
            return self._record_to_dict(record)
        finally:
            session.close()
    
    def get_all_records(self, fund_code: str = None, status: str = None, username: str = None) -> List[Dict]:
        """获取所有记录（优化：使用索引查询）"""
        session = self._get_session()
        try:
            query = session.query(ArbitrageRecord)
            
            # 使用索引字段进行过滤
            if username:
                query = query.filter(ArbitrageRecord.username == username)  # 使用 username 索引
            if fund_code:
                query = query.filter(ArbitrageRecord.fund_code == fund_code)  # 使用 fund_code 索引
            if status:
                query = query.filter(ArbitrageRecord.status == status)  # 使用 status 索引
            
            # 使用 created_at 索引排序
            query = query.order_by(ArbitrageRecord.created_at.desc())
            records = query.all()
            
            return [self._record_to_dict(r) for r in records]
        finally:
            session.close()
    
    def get_all_users_statistics(self) -> Dict:
        """获取所有用户的统计信息（管理员功能）"""
        session = self._get_session()
        try:
            all_records = session.query(ArbitrageRecord).all()
            
            total_records = len(all_records)
            total_completed = sum(1 for r in all_records if r.status == 'completed')
            total_in_progress = sum(1 for r in all_records if r.status == 'in_progress')
            total_cancelled = sum(1 for r in all_records if r.status == 'cancelled')
            total_pending = sum(1 for r in all_records if r.status == 'pending')
            
            total_profit = sum((r.profit or 0) for r in all_records if r.profit is not None)
            total_amount = sum((r.initial_amount or 0) for r in all_records)
            overall_profit_rate = (total_profit / total_amount * 100) if total_amount > 0 else 0
            
            # 按用户统计
            user_statistics = {}
            for record in all_records:
                username = record.username or 'unknown'
                if username not in user_statistics:
                    user_statistics[username] = {
                        'total_records': 0,
                        'completed': 0,
                        'in_progress': 0,
                        'cancelled': 0,
                        'pending': 0,
                        'total_profit': 0,
                        'total_amount': 0,
                        'profit_rate': 0
                    }
                
                user_stats = user_statistics[username]
                user_stats['total_records'] += 1
                user_stats[record.status] = user_stats.get(record.status, 0) + 1
                
                if record.profit is not None:
                    user_stats['total_profit'] += record.profit or 0
                
                user_stats['total_amount'] += record.initial_amount or 0
            
            # 计算每个用户的盈亏率
            for username, user_stats in user_statistics.items():
                if user_stats['total_amount'] > 0:
                    user_stats['profit_rate'] = (user_stats['total_profit'] / user_stats['total_amount']) * 100
            
            return {
                'total_records': total_records,
                'total_completed': total_completed,
                'total_in_progress': total_in_progress,
                'total_cancelled': total_cancelled,
                'total_pending': total_pending,
                'total_profit': total_profit,
                'total_amount': total_amount,
                'overall_profit_rate': overall_profit_rate,
                'user_statistics': user_statistics
            }
        finally:
            session.close()
    
    def get_statistics(self, username: str = None) -> Dict:
        """获取统计信息"""
        session = self._get_session()
        try:
            query = session.query(ArbitrageRecord)
            if username:
                query = query.filter(ArbitrageRecord.username == username)
            
            all_records = query.all()
            completed = [r for r in all_records if r.status == 'completed']
            
            total_count = len(completed)
            total_profit = sum(r.profit or 0 for r in completed)
            total_investment = sum(r.initial_amount or 0 for r in completed)
            total_net_profit = sum(r.net_profit or 0 for r in completed)
            
            total_fees = 0
            for r in completed:
                total_fees += (r.initial_fee_amount or 0) + (r.final_fee_amount or 0)
            
            profitable_count = len([r for r in completed if r.profit and r.profit > 0])
            loss_count = len([r for r in completed if r.profit and r.profit < 0])
            
            avg_profit_rate = 0
            if total_count > 0:
                profit_rates = [r.profit_rate or 0 for r in completed if r.profit_rate is not None]
                avg_profit_rate = sum(profit_rates) / len(profit_rates) if profit_rates else 0
            
            in_progress_count = len([r for r in all_records if r.status == 'in_progress'])
            
            return {
                'total_count': total_count,
                'total_profit': round(total_profit, 2),
                'total_investment': round(total_investment, 2),
                'total_return_rate': round((total_profit / total_investment * 100) if total_investment > 0 else 0, 2),
                'total_net_profit': round(total_net_profit, 2),
                'total_fees': round(total_fees, 2),
                'profitable_count': profitable_count,
                'loss_count': loss_count,
                'win_rate': round((profitable_count / total_count * 100) if total_count > 0 else 0, 2),
                'avg_profit_rate': round(avg_profit_rate, 2),
                'in_progress_count': in_progress_count
            }
        finally:
            session.close()
    
    def get_daily_purchase_amount(self, username: str, fund_code: str, date: str = None) -> float:
        """获取用户指定日期对指定基金的累计申购金额"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        session = self._get_session()
        try:
            records = session.query(ArbitrageRecord).filter(
                ArbitrageRecord.username == username,
                ArbitrageRecord.fund_code == fund_code,
                ArbitrageRecord.arbitrage_type == 'premium',
                ArbitrageRecord.initial_date == date,
                ArbitrageRecord.status.in_(['in_progress', 'completed'])
            ).all()
            
            total_amount = sum(r.initial_amount or 0 for r in records)
            return float(total_amount)
        finally:
            session.close()
    
    def delete_record(self, record_id: str, username: str = None) -> bool:
        """删除记录"""
        session = self._get_session()
        try:
            query = session.query(ArbitrageRecord).filter(ArbitrageRecord.id == record_id)
            if username:
                query = query.filter(ArbitrageRecord.username == username)
            
            record = query.first()
            if not record:
                return False
            
            session.delete(record)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
