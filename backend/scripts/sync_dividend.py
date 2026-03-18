"""
同步分红送股数据
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


def sync_dividend():
    """同步分红数据"""
    print("=" * 60)
    print("同步分红数据...")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return

    db = db_manager.get_connection()

    # 获取所有股票
    stocks_result = db.execute(
        "SELECT ts_code FROM stocks WHERE list_status = 'L'"
    ).fetchall()

    stocks = [s[0] for s in stocks_result]
    print(f"共有 {len(stocks)} 只股票需要同步")

    # 限制为样本股票
    sample_stocks = stocks[:200]
    print(f"本次同步前 {len(sample_stocks)} 只股票")

    total = 0
    skipped = 0

    for code in sample_stocks:
        try:
            # 获取最近3年的分红数据
            df = tushare_client.pro.dividend(
                ts_code=code,
                start_date='20220101',
                end_date='20261231'
            )

            if df.empty:
                skipped += 1
                continue

            # 转换日期列
            date_cols = ['ann_date', 'record_date', 'ex_date', 'pay_date', 'div_date', 'imp_ann_date']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

            # 获取表的列
            table_info = db.execute("PRAGMA table_info(stock_dividend)").fetchall()
            table_cols = [col[1] for col in table_info]

            # 只保留表中存在的列
            df_cols = [c for c in df.columns if c in table_cols]
            df = df[df_cols]

            if df.empty:
                skipped += 1
                continue

            # 使用INSERT OR REPLACE
            db.register('temp_div', df)
            col_str = ', '.join(df_cols)
            db.execute(f"""
                INSERT OR REPLACE INTO stock_dividend ({col_str})
                SELECT {col_str} FROM temp_div
            """)

            total += len(df)
            print(f"  {code}: {len(df)} 条分红记录")

        except Exception as e:
            print(f"  {code} 失败: {e}")
            continue

    print(f"\n[OK] 分红数据同步完成: 共 {total} 条, 跳过 {skipped} 只")


def main():
    print("\n" + "=" * 60)
    print("同步分红送股数据")
    print("=" * 60 + "\n")

    init_db()

    if tushare_client.pro is None:
        print("[ERROR] TUSHARE_TOKEN未配置")
        return

    sync_dividend()

    print("\n" + "=" * 60)
    print("同步完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
