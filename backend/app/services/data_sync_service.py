"""
数据同步服务 - 从Tushare同步数据到DuckDB
"""
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from app.db.session import db_manager
from app.utils.tushare_client import tushare_client


class DataSyncService:
    """数据同步服务"""

    def __init__(self):
        pass

    @property
    def db(self):
        """延迟获取数据库连接"""
        return db_manager.get_connection()

    async def sync_stock_list(self) -> Dict[str, Any]:
        """同步股票基础信息"""
        try:
            print("开始同步股票列表...")
            df = tushare_client.get_stock_list()

            if df.empty:
                return {"status": "error", "message": "获取股票列表为空"}

            # 转换日期格式
            if 'list_date' in df.columns:
                df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce').dt.date
            if 'delist_date' in df.columns:
                df['delist_date'] = pd.to_datetime(df['delist_date'], errors='coerce').dt.date

            # 使用注册临时表的方式进行UPSERT
            self.db.register('temp_stocks', df)

            # 先删除已存在的记录
            self.db.execute("""
                DELETE FROM stocks
                WHERE ts_code IN (SELECT ts_code FROM temp_stocks)
            """)

            # 插入新数据
            self.db.execute("""
                INSERT INTO stocks
                SELECT ts_code, symbol, name, area, industry, fullname,
                       enname, cnspell, market, exchange, curr_type,
                       list_status, list_date, delist_date, is_hs,
                       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM temp_stocks
            """)

            # 更新同步状态
            self._update_sync_status('stocks', len(df), 'success')

            print(f"股票列表同步完成，共 {len(df)} 只股票")

            return {
                "status": "success",
                "record_count": len(df),
                "message": f"同步完成，共 {len(df)} 只股票"
            }

        except Exception as e:
            error_msg = str(e)
            self._update_sync_status('stocks', 0, 'failed', error_msg)
            print(f"股票列表同步失败: {error_msg}")
            return {"status": "error", "message": error_msg}

    async def sync_daily_kline(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """同步日线数据"""
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')
            if start_date is None:
                # 默认同步最近30天
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

            print(f"开始同步日线数据: {start_date} 至 {end_date}")

            total_records = 0

            if ts_code:
                # 同步单只股票
                records = await self._fetch_and_save_daily(ts_code, start_date, end_date)
                total_records += records
            else:
                # 获取所有正常上市的股票
                stocks = self.db.execute(
                    "SELECT ts_code FROM stocks WHERE list_status='L'"
                ).fetchall()

                print(f"需要同步 {len(stocks)} 只股票的日线数据")

                # 分批处理，每批100只
                batch_size = 100
                for i in range(0, len(stocks), batch_size):
                    batch = stocks[i:i + batch_size]
                    batch_tasks = [
                        self._fetch_and_save_daily(code[0], start_date, end_date)
                        for code in batch
                    ]
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                    for result in results:
                        if isinstance(result, int):
                            total_records += result
                        elif isinstance(result, Exception):
                            print(f"同步失败: {result}")

                    # 每批次后暂停，避免请求过快
                    await asyncio.sleep(1)

                    progress = min(i + batch_size, len(stocks))
                    print(f"进度: {progress}/{len(stocks)}")

            self._update_sync_status('kline_daily', total_records, 'success')

            print(f"日线数据同步完成，共 {total_records} 条记录")

            return {
                "status": "success",
                "record_count": total_records,
                "date_range": f"{start_date} - {end_date}"
            }

        except Exception as e:
            error_msg = str(e)
            self._update_sync_status('kline_daily', 0, 'failed', error_msg)
            print(f"日线数据同步失败: {error_msg}")
            return {"status": "error", "message": error_msg}

    async def _fetch_and_save_daily(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> int:
        """获取并保存单只股票的日线数据"""
        try:
            df = tushare_client.get_daily_kline(ts_code, start_date, end_date)

            if df.empty:
                return 0

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            # 删除已存在的数据（避免重复）
            self.db.execute("""
                DELETE FROM kline_daily
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
            """, [ts_code, start_date, end_date])

            # 批量插入
            self.db.register('daily_data', df)
            self.db.execute("""
                INSERT INTO kline_daily
                SELECT ts_code, trade_date, open, high, low, close,
                       pre_close, change, pct_chg, vol, amount
                FROM daily_data
            """)

            return len(df)

        except Exception as e:
            print(f"同步 {ts_code} 失败: {e}")
            raise

    async def sync_daily_for_date(self, trade_date: Optional[str] = None) -> Dict[str, Any]:
        """同步特定交易日的所有股票日线数据（增量更新推荐方式）"""
        try:
            if trade_date is None:
                trade_date = tushare_client.get_latest_trade_date()

            print(f"开始同步 {trade_date} 的日线数据")

            df = tushare_client.get_daily_kline_all(trade_date=trade_date)

            if df.empty:
                return {"status": "error", "message": f"{trade_date} 无数据"}

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

            # 删除该日期的旧数据
            self.db.execute(
                "DELETE FROM kline_daily WHERE trade_date = ?",
                [pd.to_datetime(trade_date).date()]
            )

            # 插入新数据
            self.db.register('daily_data', df)
            self.db.execute("""
                INSERT INTO kline_daily
                SELECT ts_code, trade_date, open, high, low, close,
                       pre_close, change, pct_chg, vol, amount
                FROM daily_data
            """)

            count = len(df)
            self._update_sync_status('kline_daily', count, 'success')

            print(f"{trade_date} 日线数据同步完成，共 {count} 条记录")

            return {
                "status": "success",
                "record_count": count,
                "trade_date": trade_date
            }

        except Exception as e:
            error_msg = str(e)
            self._update_sync_status('kline_daily', 0, 'failed', error_msg)
            print(f"日线数据同步失败: {error_msg}")
            return {"status": "error", "message": error_msg}

    def get_sync_status(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """获取同步状态"""
        if table_name:
            result = self.db.execute(
                "SELECT * FROM data_sync_status WHERE table_name = ?",
                [table_name]
            ).fetchone()

            if result:
                return {
                    "table_name": result[0],
                    "last_sync_date": result[1],
                    "last_sync_time": result[2],
                    "record_count": result[3],
                    "status": result[4],
                    "message": result[5]
                }
            return {}

        # 获取所有表的同步状态
        results = self.db.execute("SELECT * FROM data_sync_status").fetchall()
        return {
            row[0]: {
                "last_sync_date": row[1],
                "last_sync_time": row[2],
                "record_count": row[3],
                "status": row[4],
                "message": row[5]
            }
            for row in results
        }

    def _update_sync_status(
        self,
        table: str,
        count: int,
        status: str,
        message: str = ''
    ):
        """更新同步状态"""
        self.db.execute("""
            INSERT OR REPLACE INTO data_sync_status
            (table_name, last_sync_date, last_sync_time, record_count, status, message)
            VALUES (?, CURRENT_DATE, CURRENT_TIMESTAMP, ?, ?, ?)
        """, [table, count, status, message])


# 全局服务实例 - 延迟初始化
data_sync_service = DataSyncService()
