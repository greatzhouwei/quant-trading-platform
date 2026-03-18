"""
同步2024-2025年财务数据
包括：财务指标(fina_indicator)、分红数据(dividend)、每日指标(daily_basic)
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


async def sync_fina_indicator():
    """同步财务指标数据（2024-2025年）"""
    print("=" * 60)
    print("开始同步财务指标数据(fina_indicator)...")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        db = db_manager.get_connection()

        # 获取所有股票代码
        stocks_result = db.execute(
            "SELECT ts_code FROM stocks WHERE list_status = 'L'"
        ).fetchall()

        stocks = [s[0] for s in stocks_result]
        print(f"共有 {len(stocks)} 只股票需要同步")

        # 分批处理，每批50只
        total_records = 0
        batch_size = 50

        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            codes = ','.join(batch)

            try:
                # 获取2024-2025年的财务指标
                df = tushare_client.pro.fina_indicator(
                    ts_code=codes,
                    start_date='20240101',
                    end_date='20251231'
                )

                if not df.empty:
                    # 转换日期格式
                    for col in ['ann_date', 'end_date']:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

                    # 删除已存在的数据
                    for code in batch:
                        db.execute("""
                            DELETE FROM stock_fina_indicator
                            WHERE ts_code = ?
                            AND end_date >= '2024-01-01'
                            AND end_date <= '2025-12-31'
                        """, [code])

                    # 插入新数据
                    # 注意：需要处理列名映射
                    columns = df.columns.tolist()
                    print(f"  批次 {i//batch_size + 1}: 获取到 {len(df)} 条记录, 列数: {len(columns)}")

                    # 由于列很多，我们只插入部分关键列
                    # 先检查表结构
                    db.register('temp_fina', df)

                    # 构建插入SQL（动态根据可用列）
                    available_cols = []
                    for col in columns:
                        # 检查列是否在表中
                        try:
                            db.execute(f"SELECT {col} FROM stock_fina_indicator LIMIT 0")
                            available_cols.append(col)
                        except:
                            pass

                    if available_cols:
                        col_str = ', '.join(available_cols)
                        db.execute(f"""
                            INSERT OR REPLACE INTO stock_fina_indicator ({col_str})
                            SELECT {col_str} FROM temp_fina
                        """)

                    total_records += len(df)

                # 避免请求过快
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"  [WARNING] 同步批次 {i//batch_size + 1} 失败: {e}")
                continue

        print(f"[SUCCESS] 财务指标同步完成，共 {total_records} 条记录")
        return True

    except Exception as e:
        print(f"[ERROR] 财务指标同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def sync_dividend():
    """同步分红数据（2022-2025年，用于计算3年股息率）"""
    print("\n" + "=" * 60)
    print("开始同步分红数据(dividend)...")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        db = db_manager.get_connection()

        # 获取所有股票
        stocks_result = db.execute(
            "SELECT ts_code FROM stocks WHERE list_status = 'L'"
        ).fetchall()

        stocks = [s[0] for s in stocks_result]
        print(f"共有 {len(stocks)} 只股票需要同步")

        total_records = 0
        batch_size = 50

        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]

            for code in batch:
                try:
                    df = tushare_client.pro.dividend(
                        ts_code=code,
                        start_date='20220101',
                        end_date='20251231'
                    )

                    if not df.empty:
                        # 转换日期格式
                        date_cols = ['end_date', 'ann_date', 'record_date', 'ex_date', 'pay_date', 'div_listdate', 'imp_ann_date', 'base_date']
                        for col in date_cols:
                            if col in df.columns:
                                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

                        # 删除旧数据
                        db.execute("""
                            DELETE FROM stock_dividend
                            WHERE ts_code = ?
                            AND end_date >= '2022-01-01'
                            AND end_date <= '2025-12-31'
                        """, [code])

                        # 插入新数据
                        db.register('temp_div', df)
                        db.execute("""
                            INSERT OR REPLACE INTO stock_dividend
                            SELECT ts_code, end_date, ann_date, div_proc, stk_div, stk_bo_rate,
                                   stk_co_rate, cash_div, cash_div_tax, record_date, ex_date,
                                   pay_date, div_listdate, imp_ann_date, base_date, base_share,
                                   CURRENT_TIMESTAMP
                            FROM temp_div
                        """)

                        total_records += len(df)

                    await asyncio.sleep(0.1)

                except Exception as e:
                    print(f"  [WARNING] 同步 {code} 失败: {e}")
                    continue

            if (i // batch_size) % 10 == 0:
                print(f"  进度: {min(i+batch_size, len(stocks))}/{len(stocks)}, 累计 {total_records} 条")

        print(f"[SUCCESS] 分红数据同步完成，共 {total_records} 条记录")
        return True

    except Exception as e:
        print(f"[ERROR] 分红数据同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def sync_daily_basic_2024_2025():
    """同步2024-2025年每日指标数据"""
    print("\n" + "=" * 60)
    print("开始同步每日指标数据(daily_basic)...")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        db = db_manager.get_connection()

        # 获取2024-2025年的交易日
        result = db.execute("""
            SELECT cal_date FROM trade_calendar
            WHERE is_open = 1
            AND cal_date BETWEEN '2024-01-01' AND '2025-12-31'
        """).fetchall()

        trade_dates = [r[0] for r in result]
        print(f"共有 {len(trade_dates)} 个交易日需要同步")

        total_records = 0

        for trade_date in trade_dates:
            try:
                df = tushare_client.pro.daily_basic(
                    trade_date=trade_date.strftime('%Y%m%d')
                )

                if not df.empty:
                    # 转换日期格式
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

                    # 删除该日期的旧数据
                    db.execute("""
                        DELETE FROM stock_daily_basic
                        WHERE trade_date = ?
                    """, [trade_date])

                    # 插入新数据
                    db.register('temp_basic', df)
                    db.execute("""
                        INSERT OR REPLACE INTO stock_daily_basic
                        SELECT ts_code, trade_date, close, turnover_rate, turnover_rate_f,
                               volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                               total_share, float_share, free_share, total_mv, circ_mv,
                               CURRENT_TIMESTAMP
                        FROM temp_basic
                    """)

                    total_records += len(df)

                if total_records % 50000 == 0:
                    print(f"  进度: {trade_date}, 累计 {total_records} 条")

                await asyncio.sleep(0.2)

            except Exception as e:
                print(f"  [WARNING] 同步 {trade_date} 失败: {e}")
                continue

        print(f"[SUCCESS] 每日指标同步完成，共 {total_records} 条记录")
        return True

    except Exception as e:
        print(f"[ERROR] 每日指标同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("开始同步2024-2025年财务数据")
    print("=" * 60 + "\n")

    # 初始化数据库
    print("初始化数据库...")
    init_db()
    print("数据库初始化完成\n")

    # 检查Tushare配置
    if tushare_client.pro is None:
        print("\n[ERROR] TUSHARE_TOKEN未配置或无效！")
        print("请访问 https://tushare.pro 注册并获取Token")
        print("然后将Token添加到 backend/.env 文件中")
        return

    print(f"Tushare配置成功\n")

    # 同步数据
    results = []

    # 1. 同步分红数据（用于计算股息率）
    results.append(("分红数据(2022-2025)", await sync_dividend()))

    # 2. 同步财务指标
    results.append(("财务指标(2024-2025)", await sync_fina_indicator()))

    # 3. 同步每日指标
    # results.append(("每日指标(2024-2025)", await sync_daily_basic_2024_2025()))

    # 打印汇总
    print("\n" + "=" * 60)
    print("同步完成汇总")
    print("=" * 60)
    for name, success in results:
        status = "成功" if success else "失败"
        print(f"{name}: {status}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
