# -*- coding: utf-8 -*-
"""
基金数据管理器 - SQLite 数据库版本
用于缓存和快速访问基金数据
"""

from datetime import datetime
from typing import Dict, List, Optional
from database_models import get_db_manager, FundData


class FundDataManagerDB:
    """基金数据管理器（数据库版本）"""
    
    def __init__(self, db_path: str = "lof_arbitrage.db"):
        """
        初始化基金数据管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_manager = get_db_manager(db_path)
    
    def _get_session(self):
        """获取数据库会话"""
        return self.db_manager.get_session()
    
    def _fund_data_to_dict(self, fund_data: FundData) -> Dict:
        """将数据库记录转换为字典"""
        return {
            'fund_code': fund_data.fund_code,
            'fund_name': fund_data.fund_name,
            'price': fund_data.price,
            'price_date': fund_data.price_date,
            'change_pct': fund_data.change_pct,
            'nav': fund_data.nav,
            'nav_date': fund_data.nav_date,
            'price_diff': fund_data.price_diff,
            'price_diff_pct': fund_data.price_diff_pct,
            'arbitrage_type': fund_data.arbitrage_type,
            'profit_rate': fund_data.profit_rate,
            'purchase_limit': fund_data.purchase_limit or {},
            'data_source': fund_data.data_source,
            'updated_at': fund_data.updated_at.isoformat() if fund_data.updated_at else None
        }
    
    def get_fund_data(self, fund_code: str) -> Optional[Dict]:
        """获取单个基金数据"""
        session = self._get_session()
        try:
            fund_data = session.query(FundData).filter(FundData.fund_code == fund_code).first()
            if not fund_data:
                return None
            return self._fund_data_to_dict(fund_data)
        finally:
            session.close()
    
    def get_funds_data(self, fund_codes: List[str] = None) -> List[Dict]:
        """批量获取基金数据"""
        session = self._get_session()
        try:
            query = session.query(FundData)
            if fund_codes:
                query = query.filter(FundData.fund_code.in_(fund_codes))
            
            # 按更新时间倒序排列（最新的在前）
            query = query.order_by(FundData.updated_at.desc())
            funds_data = query.all()
            
            return [self._fund_data_to_dict(f) for f in funds_data]
        finally:
            session.close()
    
    def get_all_funds_data(self) -> List[Dict]:
        """获取所有基金数据"""
        return self.get_funds_data()
    
    def update_fund_data(self, fund_code: str, fund_data: Dict) -> bool:
        """更新或创建基金数据"""
        session = self._get_session()
        try:
            # 查找现有记录
            existing = session.query(FundData).filter(FundData.fund_code == fund_code).first()
            
            if existing:
                # 更新现有记录
                existing.fund_name = fund_data.get('fund_name', existing.fund_name)
                existing.price = fund_data.get('price')
                existing.price_date = fund_data.get('price_date')
                existing.change_pct = fund_data.get('change_pct')
                existing.nav = fund_data.get('nav')
                existing.nav_date = fund_data.get('nav_date')
                existing.price_diff = fund_data.get('price_diff')
                existing.price_diff_pct = fund_data.get('price_diff_pct')
                existing.arbitrage_type = fund_data.get('arbitrage_type')
                existing.profit_rate = fund_data.get('profit_rate')
                # 只在新值非空时才更新申购状态，避免价格/净值更新时意外清空已有的申购状态
                new_purchase_limit = fund_data.get('purchase_limit')
                if new_purchase_limit:
                    existing.purchase_limit = new_purchase_limit
                existing.data_source = fund_data.get('data_source', 'unknown')
                existing.updated_at = datetime.now()
            else:
                # 创建新记录
                new_fund = FundData(
                    fund_code=fund_code,
                    fund_name=fund_data.get('fund_name', fund_code),
                    price=fund_data.get('price'),
                    price_date=fund_data.get('price_date'),
                    change_pct=fund_data.get('change_pct'),
                    nav=fund_data.get('nav'),
                    nav_date=fund_data.get('nav_date'),
                    price_diff=fund_data.get('price_diff'),
                    price_diff_pct=fund_data.get('price_diff_pct'),
                    arbitrage_type=fund_data.get('arbitrage_type'),
                    profit_rate=fund_data.get('profit_rate'),
                    purchase_limit=fund_data.get('purchase_limit', {}),
                    data_source=fund_data.get('data_source', 'unknown'),
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                session.add(new_fund)
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            print(f"更新基金数据失败 {fund_code}: {e}")
            return False
        finally:
            session.close()
    
    def batch_update_funds_data(self, funds_data_list: List[Dict]) -> int:
        """批量更新基金数据"""
        updated_count = 0
        for fund_data in funds_data_list:
            fund_code = fund_data.get('fund_code')
            if fund_code:
                if self.update_fund_data(fund_code, fund_data):
                    updated_count += 1
        return updated_count
    
    def delete_fund_data(self, fund_code: str) -> bool:
        """删除基金数据"""
        session = self._get_session()
        try:
            fund_data = session.query(FundData).filter(FundData.fund_code == fund_code).first()
            if fund_data:
                session.delete(fund_data)
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_funds_count(self) -> int:
        """获取基金数据总数"""
        session = self._get_session()
        try:
            return session.query(FundData).count()
        finally:
            session.close()
    
    def get_stale_funds(self, max_age_seconds: int = 300) -> List[str]:
        """获取过期的基金数据（超过指定时间未更新）"""
        session = self._get_session()
        try:
            cutoff_time = datetime.now().timestamp() - max_age_seconds
            cutoff_datetime = datetime.fromtimestamp(cutoff_time)
            
            stale_funds = session.query(FundData).filter(
                FundData.updated_at < cutoff_datetime
            ).all()
            
            return [f.fund_code for f in stale_funds]
        finally:
            session.close()
