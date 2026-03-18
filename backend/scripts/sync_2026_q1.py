"""
同步2026年Q1数据（1月1日-3月1日）
"""
import asyncio
import sys
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


async def sync_trade_calendar():
    """同步交易日历"""
    print("=" * 50)
    print("开始同步交易日历...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        df = tushare_client.pro.trade_cal(
            exchange='SSE',
            start_date='20260101',
            end_date='20260301'
        )

        if df.empty:
            print("[WARNING] 未获取到交易日历数据")
            return False

        db = db_manager.get_connection()

        # 创建交易日历表（如果不存在）
        db.execute("""
            CREATE TABLE IF NOT EXISTS trade_calendar (
                exchange VARCHAR,
                cal_date DATE,
                is_open INTEGER,
                pretrade_date DATE,
                PRIMARY KEY (exchange, cal_date)
            )
        """)

        # 转换日期格式
        df['cal_date'] = pd.to_datetime(df['cal_date']).dt.date
        if 'pretrade_date' in df.columns:
            df['pretrade_date'] = pd.to_datetime(df['pretrade_date']).dt.date

        # 删除旧数据
        db.execute("""
            DELETE FROM trade_calendar
            WHERE cal_date BETWEEN '2026-01-01' AND '2026-03-01'
        """)

        # 插入新数据
        db.register('temp_cal', df)
        db.execute("""
            INSERT INTO trade_calendar
            SELECT exchange, cal_date, is_open, pretrade_date
            FROM temp_cal
        """)

        print(f"[SUCCESS] 交易日历同步完成，共 {len(df)} 条记录")
        return True

    except Exception as e:
        print(f"[ERROR] 交易日历同步失败: {e}")
        return False


async def sync_stock_list():
    """同步股票列表"""
    print("=" * 50)
    print("开始同步股票列表...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        df = tushare_client.pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,'
                   'market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
        )

        if df.empty:
            print("[WARNING] 未获取到股票列表")
            return False

        db = db_manager.get_connection()

        # 清空并插入新数据
        db.execute("DELETE FROM stocks")

        # 转换日期格式
        if 'list_date' in df.columns:
            df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce').dt.date
        if 'delist_date' in df.columns:
            df['delist_date'] = pd.to_datetime(df['delist_date'], errors='coerce').dt.date

        db.register('temp_stocks', df)
        db.execute("""
            INSERT INTO stocks
            SELECT ts_code, symbol, name, area, industry, fullname,
                   enname, cnspell, market, exchange, curr_type,
                   list_status, list_date, delist_date, is_hs,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM temp_stocks
        """)

        print(f"[SUCCESS] 股票列表同步完成，共 {len(df)} 只股票")
        return True

    except Exception as e:
        print(f"[ERROR] 股票列表同步失败: {e}")
        return False


async def sync_daily_kline():
    """同步日线数据（2026-01-01 到 2026-03-01）"""
    print("=" * 50)
    print("开始同步日线数据...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        # 先获取所有正常上市的股票
        db = db_manager.get_connection()
        stocks_result = db.execute(
            "SELECT ts_code FROM stocks WHERE list_status = 'L'"
        ).fetchall()

        stocks = [s[0] for s in stocks_result]
        print(f"共有 {len(stocks)} 只股票需要同步")

        # 限制数量，先同步部分股票用于测试
        test_stocks = stocks[:100]  # 先同步前100只
        print(f"本次同步前 {len(test_stocks)} 只股票")

        total_records = 0
        batch_size = 10  # 每批处理10只股票

        for i in range(0, len(test_stocks), batch_size):
            batch = test_stocks[i:i+batch_size]
            codes = ','.join(batch)

            try:
                df = tushare_client.pro.daily(
                    ts_code=codes,
                    start_date='20260101',
                    end_date='20260301'
                )

                if not df.empty:
                    # 转换日期格式
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

                    # 删除已存在的数据
                    for code in batch:
                        db.execute("""
                            DELETE FROM kline_daily
                            WHERE ts_code = ? AND trade_date BETWEEN '2026-01-01' AND '2026-03-01'
                        """, [code])

                    # 插入新数据
                    db.register('temp_kline', df)
                    db.execute("""
                        INSERT INTO kline_daily
                        SELECT ts_code, trade_date, open, high, low, close,
                               pre_close, change, pct_chg, vol, amount
                        FROM temp_kline
                    """)

                    total_records += len(df)
                    print(f"  进度: {min(i+batch_size, len(test_stocks))}/{len(test_stocks)}, 本次新增 {len(df)} 条")

                # 避免请求过快
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"  [WARNING] 同步批次 {i//batch_size + 1} 失败: {e}")
                continue

        print(f"[SUCCESS] 日线数据同步完成，共 {total_records} 条记录")
        return True

    except Exception as e:
        print(f"[ERROR] 日线数据同步失败: {e}")
        return False


async def sync_name_change():
    """同步股票曾用名"""
    print("=" * 50)
    print("开始同步股票曾用名...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        df = tushare_client.pro.namechange(
            start_date='20260101',
            end_date='20260301'
        )

        if df.empty:
            print("[INFO] 该时间段没有股票名称变更记录")
            return True

        db = db_manager.get_connection()

        # 创建曾用名表
        db.execute("""
            CREATE TABLE IF NOT EXISTS stock_name_history (
                ts_code VARCHAR,
                name VARCHAR,
                start_date DATE,
                end_date DATE,
                ann_date DATE,
                change_reason VARCHAR,
                PRIMARY KEY (ts_code, start_date)
            )
        """)

        # 删除旧数据
        db.execute("""
            DELETE FROM stock_name_history
            WHERE start_date BETWEEN '2026-01-01' AND '2026-03-01'
        """)

        # 转换日期格式
        for col in ['start_date', 'end_date', 'ann_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        db.register('temp_name', df)
        db.execute("""
            INSERT INTO stock_name_history
            SELECT ts_code, name, start_date, end_date, ann_date, change_reason
            FROM temp_name
        """)

        print(f"[SUCCESS] 股票曾用名同步完成，共 {len(df)} 条记录")
        return True

    except Exception as e:
        print(f"[ERROR] 股票曾用名同步失败: {e}")
        return False


async def sync_daily_basic():
    """同步每日指标数据"""
    print("=" * 50)
    print("开始同步每日指标数据...")

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置，无法同步数据")
        return False

    try:
        # 获取交易日历中的交易日
        db = db_manager.get_connection()
        trade_dates = db.execute("""
            SELECT cal_date FROM trade_calendar
            WHERE is_open = 1
            AND cal_date BETWEEN '2026-01-01' AND '2026-03-01'
        """).fetchall()

        if not trade_dates:
            print("[WARNING] 未找到交易日，跳过每日指标同步")
            return False

        total_records = 0

        for (trade_date,) in trade_dates:
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
                        INSERT INTO stock_daily_basic
                        SELECT ts_code, trade_date, close, turnover_rate, turnover_rate_f,
                               volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                               total_share, float_share, free_share, total_mv, circ_mv
                        FROM temp_basic
                    """)

                    total_records += len(df)
                    print(f"  {trade_date}: {len(df)} 条")

                await asyncio.sleep(0.3)

            except Exception as e:
                print(f"  [WARNING] 同步 {trade_date} 失败: {e}")
                continue

        print(f"[SUCCESS] 每日指标同步完成，共 {total_records} 条记录")
        return True

    except Exception as e:
        print(f"[ERROR] 每日指标同步失败: {e}")
        return False


async def main():
    """主函数"""
    print("\n" + "=" * 50)
    print("开始同步2026年Q1数据（2026-01-01 至 2026-03-01）")
    print("=" * 50 + "\n")

    # 初始化数据库
    print("初始化数据库...")
    init_db()

    # 检查Tushare配置
    if tushare_client.pro is None:
        print("\n[ERROR] TUSHARE_TOKEN未配置或无效！")
        print("请访问 https://tushare.pro 注册并获取Token")
        print("然后将Token添加到 backend/.env 文件中")
        return

    # 同步数据
    results = []

    results.append(("交易日历", await sync_trade_calendar()))
    results.append(("股票列表", await sync_stock_list()))
    results.append(("日线数据", await sync_daily_kline()))
    results.append(("股票曾用名", await sync_name_change()))
    results.append(("每日指标", await sync_daily_basic()))

    # 打印汇总
    print("\n" + "=" * 50)
    print("同步完成汇总")
    print("=" * 50)
    for name, success in results:
        status = "✓ 成功" if success else "✗ 失败"
        print(f"{name}: {status}")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
