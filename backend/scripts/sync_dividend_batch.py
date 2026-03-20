"""
批量同步分红数据（不使用日期过滤，直接按股票代码获取全量数据）
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


def sync_dividend_for_stocks(stocks, label=''):
    """同步指定股票的分红数据"""
    db = db_manager.get_connection()
    total = 0
    failed = 0

    for i, code in enumerate(stocks):
        try:
            # 不带日期参数，获取全量历史分红
            df = tushare_client.pro.dividend(ts_code=code)

            if df is None or df.empty:
                continue

            # 只保留实施的分红记录（div_proc == '实施'）
            if 'div_proc' in df.columns:
                df = df[df['div_proc'] == '实施']

            if df.empty:
                continue

            # 转换日期列（YYYYMMDD字符串 -> date对象）
            date_cols = ['ann_date', 'record_date', 'ex_date', 'pay_date', 'div_listdate', 'imp_ann_date', 'end_date', 'base_date']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce').dt.date

            # 获取表的列
            table_info = db.execute("PRAGMA table_info(stock_dividend)").fetchall()
            table_cols = [col[1] for col in table_info]

            # 只保留表中存在的列
            df_cols = [c for c in df.columns if c in table_cols]
            df = df[df_cols]

            if df.empty:
                continue

            # 删除旧记录再插入
            db.execute("DELETE FROM stock_dividend WHERE ts_code = ?", [code])

            db.register('temp_div', df)
            col_str = ', '.join(df_cols)
            db.execute(f"""
                INSERT INTO stock_dividend ({col_str})
                SELECT {col_str} FROM temp_div
            """)

            total += len(df)
            if i % 50 == 0 or len(df) > 0:
                print(f"  [{i+1}/{len(stocks)}] {code}: {len(df)} 条分红记录")

            # 避免API频率限制
            if (i + 1) % 100 == 0:
                time.sleep(1)

        except Exception as e:
            failed += 1
            if failed <= 5:
                print(f"  {code} 失败: {e}")
            continue

    return total, failed


def main():
    init_db()

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return

    db = db_manager.get_connection()

    # 获取活跃的A股列表（排除科创板、北交所）
    stocks_result = db.execute("""
        SELECT ts_code FROM stocks
        WHERE list_status = 'L'
        AND ts_code NOT LIKE '688%'
        AND ts_code NOT LIKE '%.BJ'
        AND ts_code NOT LIKE '8%'
        AND ts_code NOT LIKE '4%'
        ORDER BY ts_code
    """).fetchall()

    stocks = [s[0] for s in stocks_result]
    print(f"共 {len(stocks)} 只A股（不含科创板/北交所）")
    print(f"开始同步分红数据...")

    # 分批同步
    batch_size = 500
    total_records = 0
    total_failed = 0

    for batch_start in range(0, len(stocks), batch_size):
        batch = stocks[batch_start:batch_start + batch_size]
        print(f"\n=== 批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_start+len(batch)} ===")
        records, failed = sync_dividend_for_stocks(batch)
        total_records += records
        total_failed += failed
        print(f"本批次: {records}条记录, {failed}个失败")
        time.sleep(2)

    print(f"\n[OK] 分红数据同步完成: 共 {total_records} 条记录, {total_failed} 个失败")

    # 验证
    cnt = db.execute("SELECT COUNT(*) FROM stock_dividend WHERE cash_div > 0").fetchone()[0]
    print(f"数据库中有效分红记录: {cnt}")

    recent = db.execute("""
        SELECT COUNT(DISTINCT ts_code) FROM stock_dividend
        WHERE ann_date >= '2022-01-01' AND cash_div > 0
    """).fetchone()[0]
    print(f"2022年后有分红记录的股票: {recent}")


if __name__ == "__main__":
    main()
