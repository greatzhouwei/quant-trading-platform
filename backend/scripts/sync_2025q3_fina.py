"""
同步2025年第三季度财务数据
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


def sync_2025q3_fina():
    """同步2025年Q3财务指标"""
    print("=" * 60)
    print("同步2025年Q3财务指标...")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return

    db = db_manager.get_connection()

    # 获取所有上市股票
    stocks_result = db.execute(
        "SELECT ts_code FROM stocks WHERE list_status = 'L'"
    ).fetchall()

    stocks = [s[0] for s in stocks_result]
    print(f"共有 {len(stocks)} 只股票需要同步")

    # 限制为样本股票（避免API限流）
    sample_stocks = stocks[:100]
    print(f"本次同步前 {len(sample_stocks)} 只股票")

    total = 0
    q3_records = 0

    for code in sample_stocks:
        try:
            df = tushare_client.pro.fina_indicator(
                ts_code=code,
                start_date='20250101',
                end_date='20251231'
            )

            if not df.empty:
                # 转换日期
                for col in ['ann_date', 'end_date']:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

                # 筛选2025年Q3数据 (end_date = 2025-09-30)
                df_q3 = df[df['end_date'] == pd.Timestamp('2025-09-30').date()]

                if not df_q3.empty:
                    # 获取表的所有列
                    table_info = db.execute("PRAGMA table_info(stock_fina_indicator)").fetchall()
                    table_cols = [col[1] for col in table_info]

                    # 只保留表中存在的列
                    df_cols = [c for c in df_q3.columns if c in table_cols]
                    df_q3 = df_q3[df_cols]

                    # 使用INSERT OR REPLACE
                    db.register('temp_fina', df_q3)
                    col_str = ', '.join(df_cols)
                    db.execute(f"""
                        INSERT OR REPLACE INTO stock_fina_indicator ({col_str})
                        SELECT {col_str} FROM temp_fina
                    """)

                    total += len(df)
                    q3_records += len(df_q3)

        except Exception as e:
            print(f"  {code} 失败: {e}")
            continue

    print(f"[OK] 财务指标同步完成: 总记录 {total} 条, 2025Q3记录 {q3_records} 条")


def main():
    print("\n" + "=" * 60)
    print("同步2025年Q3财务数据")
    print("=" * 60 + "\n")

    init_db()

    if tushare_client.pro is None:
        print("[ERROR] TUSHARE_TOKEN未配置")
        return

    sync_2025q3_fina()

    print("\n" + "=" * 60)
    print("同步完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
