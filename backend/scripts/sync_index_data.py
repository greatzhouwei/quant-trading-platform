"""
同步指数K线数据（沪深300等）
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


def sync_index_kline():
    """同步指数日线数据"""
    print("=" * 60)
    print("同步指数K线数据...")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return

    db = db_manager.get_connection()

    # 主要指数列表
    index_codes = [
        '000300.SH',  # 沪深300
        '000905.SH',  # 中证500
        '000001.SH',  # 上证指数
        '399001.SZ',  # 深证成指
        '399006.SZ',  # 创业板指
        '000016.SH',  # 上证50
    ]

    total = 0
    for code in index_codes:
        try:
            # 获取2024-2026年的指数日线数据
            df = tushare_client.pro.index_daily(
                ts_code=code,
                start_date='20240101',
                end_date='20261231'
            )

            if not df.empty:
                # 转换日期
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

                # 先删除该指数的数据
                db.execute("DELETE FROM kline_daily WHERE ts_code = ?", [code])

                # 插入数据
                db.register('temp_kline', df)
                db.execute("""
                    INSERT INTO kline_daily
                    SELECT ts_code, trade_date, open, high, low, close,
                           pre_close, change, pct_chg, vol, amount
                    FROM temp_kline
                """)

                total += len(df)
                print(f"  {code}: {len(df)} 条")

        except Exception as e:
            print(f"  {code} 失败: {e}")
            continue

    print(f"[OK] 指数K线同步完成: {total} 条")


def main():
    print("\n" + "=" * 60)
    print("同步指数数据")
    print("=" * 60 + "\n")

    init_db()

    if tushare_client.pro is None:
        print("[ERROR] TUSHARE_TOKEN未配置")
        return

    sync_index_kline()

    print("\n" + "=" * 60)
    print("同步完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
