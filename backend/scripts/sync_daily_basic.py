"""
同步每日指标数据（PE/PB/市值等）
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd
from datetime import datetime, timedelta


def sync_daily_basic_for_date(trade_date: str):
    """同步某一天的每日指标"""
    print(f"同步 {trade_date} 的每日指标...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return 0

    db = db_manager.get_connection()

    try:
        # 调用Tushare接口
        df = tushare_client.pro.daily_basic(trade_date=trade_date)

        if df.empty:
            print(f"  {trade_date} 无数据")
            return 0

        # 转换日期格式
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

        # 获取表的列
        table_info = db.execute("PRAGMA table_info(stock_daily_basic)").fetchall()
        table_cols = [col[1] for col in table_info]

        # 只保留表中存在的列
        df_cols = [c for c in df.columns if c in table_cols]
        df = df[df_cols]

        if df.empty:
            print(f"  {trade_date} 无有效列数据")
            return 0

        # 使用INSERT OR REPLACE
        db.register('temp_daily', df)
        col_str = ', '.join(df_cols)
        db.execute(f"""
            INSERT OR REPLACE INTO stock_daily_basic ({col_str})
            SELECT {col_str} FROM temp_daily
        """)

        print(f"  [OK] 同步 {len(df)} 条记录")
        return len(df)

    except Exception as e:
        print(f"  [ERROR] {e}")
        return 0


def sync_daily_basic_range(start_date: str, end_date: str):
    """同步日期范围内的每日指标"""
    print("=" * 60)
    print(f"同步每日指标: {start_date} 至 {end_date}")
    print("=" * 60)

    # 生成交易日列表（简化：只取每周五）
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')

    total = 0
    current = start
    while current <= end:
        date_str = current.strftime('%Y%m%d')
        count = sync_daily_basic_for_date(date_str)
        total += count
        current += timedelta(days=1)

    print(f"\n[OK] 总共同步 {total} 条记录")


def main():
    print("\n" + "=" * 60)
    print("同步每日指标数据")
    print("=" * 60 + "\n")

    init_db()

    if tushare_client.pro is None:
        print("[ERROR] TUSHARE_TOKEN未配置")
        return

    # 同步2026年1-2月的数据（用于回测）
    sync_daily_basic_range('20260101', '20260228')

    print("\n" + "=" * 60)
    print("同步完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
