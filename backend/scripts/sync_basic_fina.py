"""
简化版财务数据同步 - 仅同步必要字段
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


def sync_dividend_simple():
    """同步分红数据 - 简化版"""
    print("=" * 60)
    print("同步分红数据...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return

    db = db_manager.get_connection()

    # 获取一些有分红的股票代码（大盘蓝筹股）
    sample_codes = [
        '000001.SZ', '000002.SZ', '000333.SZ', '000538.SZ', '000568.SZ',
        '000651.SZ', '000725.SZ', '000858.SZ', '000895.SZ', '002001.SZ',
        '002007.SZ', '002024.SZ', '002027.SZ', '002142.SZ', '002304.SZ',
        '002415.SZ', '002594.SZ', '002714.SZ', '300003.SZ', '300014.SZ',
        '300015.SZ', '300033.SZ', '300059.SZ', '300122.SZ', '300124.SZ',
        '300274.SZ', '300408.SZ', '300413.SZ', '300433.SZ', '300498.SZ',
        '600000.SH', '600009.SH', '600016.SH', '600028.SH', '600029.SH',
        '600030.SH', '600031.SH', '600036.SH', '600048.SH', '600050.SH',
        '600104.SH', '600196.SH', '600276.SH', '600309.SH', '600340.SH',
        '600406.SH', '600436.SH', '600438.SH', '600519.SH', '600547.SH',
        '600570.SH', '600585.SH', '600588.SH', '600690.SH', '600703.SH',
        '600741.SH', '600745.SH', '600809.SH', '600837.SH', '600887.SH',
        '600893.SH', '600900.SH', '600919.SH', '600958.SH', '600999.SH',
        '601012.SH', '601066.SH', '601088.SH', '601100.SH', '601111.SH',
        '601138.SH', '601166.SH', '601169.SH', '601186.SH', '601211.SH',
        '601216.SH', '601225.SH', '601229.SH', '601288.SH', '601318.SH',
        '601328.SH', '601336.SH', '601360.SH', '601377.SH', '601390.SH',
        '601398.SH', '601555.SH', '601577.SH', '601600.SH', '601601.SH',
        '601628.SH', '601633.SH', '601658.SH', '601668.SH', '601669.SH',
        '601688.SH', '601696.SH', '601699.SH', '601728.SH', '601766.SH',
        '601788.SH', '601800.SH', '601816.SH', '601818.SH', '601857.SH',
        '601866.SH', '601872.SH', '601877.SH', '601888.SH', '601899.SH',
        '601901.SH', '601916.SH', '601919.SH', '601933.SH', '601939.SH',
        '601985.SH', '601988.SH', '601989.SH', '601998.SH', '603019.SH',
        '603127.SH', '603160.SH', '603288.SH', '603369.SH', '603392.SH',
        '603486.SH', '603501.SH', '603658.SH', '603799.SH', '603986.SH'
    ]

    total = 0
    for code in sample_codes:
        try:
            df = tushare_client.pro.dividend(
                ts_code=code,
                start_date='20220101',
                end_date='20251231'
            )

            if not df.empty:
                # 只保留已实施的分红（排除预案）
                if 'div_proc' in df.columns:
                    df = df[df['div_proc'] == '实施']

                if df.empty:
                    continue

                # 转换日期
                for col in ['end_date', 'ann_date', 'record_date', 'ex_date', 'pay_date', 'div_listdate', 'imp_ann_date', 'base_date']:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

                # 获取表的所有列
                table_info = db.execute("PRAGMA table_info(stock_dividend)").fetchall()
                table_cols = [col[1] for col in table_info if col[1] != 'created_at']

                # 只保留表中存在的列
                df_cols = [c for c in df.columns if c in table_cols]
                df = df[df_cols]

                # 插入数据 (使用INSERT OR REPLACE处理主键冲突)
                db.register('temp_div', df)
                col_str = ', '.join(df_cols)
                db.execute(f"""
                    INSERT OR REPLACE INTO stock_dividend ({col_str})
                    SELECT {col_str} FROM temp_div
                """)
                total += len(df)

        except Exception as e:
            print(f"  {code} 失败: {e}")
            continue

    print(f"[OK] 分红数据同步完成: {total} 条")


def sync_fina_simple():
    """同步财务指标 - 简化版"""
    print("=" * 60)
    print("同步财务指标...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return

    db = db_manager.get_connection()

    # 同上，使用样本股票
    sample_codes = [
        '000001.SZ', '000002.SZ', '000333.SZ', '000538.SZ', '000568.SZ',
        '000651.SZ', '000725.SZ', '000858.SZ', '000895.SZ', '002001.SZ',
        '002007.SZ', '002304.SZ', '002415.SZ', '002594.SZ', '002714.SZ',
        '300015.SZ', '300033.SZ', '300059.SZ', '300122.SZ', '300124.SZ',
        '300274.SZ', '300408.SZ', '300433.SZ', '300498.SZ',
        '600000.SH', '600009.SH', '600016.SH', '600028.SH', '600030.SH',
        '600031.SH', '600036.SH', '600048.SH', '600050.SH', '600104.SH',
        '600196.SH', '600276.SH', '600309.SH', '600436.SH', '600438.SH',
        '600519.SH', '600547.SH', '600570.SH', '600585.SH', '600588.SH',
        '600690.SH', '600703.SH', '600741.SH', '600745.SH', '600809.SH',
        '600837.SH', '600887.SH', '600900.SH', '600919.SH', '600999.SH',
        '601012.SH', '601066.SH', '601088.SH', '601100.SH', '601111.SH',
        '601138.SH', '601166.SH', '601169.SH', '601211.SH', '601288.SH',
        '601318.SH', '601328.SH', '601336.SH', '601360.SH', '601377.SH',
        '601390.SH', '601398.SH', '601601.SH', '601628.SH', '601633.SH',
        '601668.SH', '601669.SH', '601688.SH', '601728.SH', '601766.SH',
        '601800.SH', '601816.SH', '601818.SH', '601857.SH', '601888.SH',
        '601899.SH', '601919.SH', '601933.SH', '601939.SH', '601988.SH',
        '603019.SH', '603127.SH', '603160.SH', '603288.SH', '603369.SH',
        '603392.SH', '603486.SH', '603501.SH', '603658.SH', '603799.SH', '603986.SH'
    ]

    total = 0
    for code in sample_codes:
        try:
            df = tushare_client.pro.fina_indicator(
                ts_code=code,
                start_date='20230101',
                end_date='20251231'
            )

            if not df.empty:
                # 转换日期
                for col in ['ann_date', 'end_date']:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

                # 获取表的所有列
                table_info = db.execute("PRAGMA table_info(stock_fina_indicator)").fetchall()
                table_cols = [col[1] for col in table_info]

                # 只保留表中存在的列
                df_cols = [c for c in df.columns if c in table_cols]
                df = df[df_cols]

                # 插入数据 (使用INSERT OR REPLACE处理主键冲突)
                db.register('temp_fina', df)
                col_str = ', '.join(df_cols)
                db.execute(f"""
                    INSERT OR REPLACE INTO stock_fina_indicator ({col_str})
                    SELECT {col_str} FROM temp_fina
                """)
                total += len(df)

        except Exception as e:
            print(f"  {code} 失败: {e}")
            continue

    print(f"[OK] 财务指标同步完成: {total} 条")


def main():
    print("\n" + "=" * 60)
    print("同步基础财务数据")
    print("=" * 60 + "\n")

    init_db()

    if tushare_client.pro is None:
        print("[ERROR] TUSHARE_TOKEN未配置")
        return

    sync_dividend_simple()
    sync_fina_simple()

    print("\n" + "=" * 60)
    print("同步完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
