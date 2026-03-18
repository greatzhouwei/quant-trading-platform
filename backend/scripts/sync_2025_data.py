"""
同步2025年全年数据
包括：财务指标(fina_indicator)、日线数据(daily)、每日指标(daily_basic)、分红数据(dividend)
时间范围：2025-01-01 至 2026-01-01

API调用频率控制：
- 普通用户限制：≤3次/秒，约50-120次/分钟
- 每个请求之间至少间隔0.35秒（约3次/秒）
- 每批次后额外休息，确保不超限

超时重试机制：
- 每次请求超时：30秒
- 失败重试：最多3次
- 重试间隔：5秒、10秒、15秒（指数退避）

用法: python scripts/sync_2025_data.py
"""
import asyncio
import sys
import time
import subprocess
import os
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import db_manager, init_db
from app.utils.tushare_client import tushare_client
import pandas as pd


# 时间范围配置
START_DATE = '20250101'
END_DATE = '20260101'

# API调用频率控制配置
API_CALL_INTERVAL = 0.35  # 每次API调用间隔0.35秒（约3次/秒，符合普通用户限制）
BATCH_REST_TIME = 2.0     # 每批次处理后休息时间（秒）
DAILY_API_INTERVAL = 0.5  # daily和daily_basic接口间隔（这些接口较严格）

# 重试配置
MAX_RETRIES = 3           # 最大重试次数
RETRY_DELAY_BASE = 5      # 基础重试延迟（秒）
REQUEST_TIMEOUT = 30      # 请求超时时间（秒）

# 记录上次API调用时间
_last_api_call_time = 0


def kill_duckdb_processes():
    """强制关闭占用DuckDB数据库的Python进程"""
    print("[INFO] 检查并关闭占用DuckDB的进程...")
    try:
        # Windows: 使用tasklist和taskkill查找并关闭占用quant_trading.duckdb的Python进程
        result = subprocess.run(
            ['tasklist', '/FI', 'IMAGENAME eq python.exe', '/FO', 'CSV'],
            capture_output=True, text=True
        )

        killed = False
        for line in result.stdout.split('\n'):
            if 'python.exe' in line and 'PID' not in line:
                try:
                    pid = line.split(',')[1].strip().strip('"')
                    # 尝试关闭进程（除了当前进程）
                    current_pid = os.getpid()
                    if int(pid) != current_pid:
                        print(f"[INFO] 正在关闭Python进程 PID {pid}...")
                        subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True)
                        killed = True
                except:
                    pass

        if killed:
            print("[INFO] 等待3秒让资源释放...")
            time.sleep(3)
        else:
            print("[INFO] 未发现占用进程")

        return killed
    except Exception as e:
        print(f"[WARNING] 关闭进程时出错: {e}")
        return False


def api_rate_limit():
    """API调用频率限制 - 确保两次调用之间至少间隔指定时间"""
    global _last_api_call_time
    current_time = time.time()
    elapsed = current_time - _last_api_call_time
    if elapsed < API_CALL_INTERVAL:
        sleep_time = API_CALL_INTERVAL - elapsed
        time.sleep(sleep_time)
    _last_api_call_time = time.time()


def api_rate_limit_daily():
    """日线相关接口更严格的频率限制"""
    global _last_api_call_time
    current_time = time.time()
    elapsed = current_time - _last_api_call_time
    if elapsed < DAILY_API_INTERVAL:
        sleep_time = DAILY_API_INTERVAL - elapsed
        time.sleep(sleep_time)
    _last_api_call_time = time.time()


def api_call_with_retry(api_func, *args, **kwargs):
    """带重试机制的API调用

    Args:
        api_func: Tushare API函数
        *args, **kwargs: 传递给API函数的参数

    Returns:
        DataFrame 或 None（如果全部重试都失败）
    """
    for attempt in range(MAX_RETRIES):
        try:
            # 使用超时参数
            start_time = time.time()
            result = api_func(*args, **kwargs, timeout=REQUEST_TIMEOUT)
            elapsed = time.time() - start_time

            # 如果请求过快，补充延迟
            min_interval = kwargs.pop('_min_interval', API_CALL_INTERVAL)
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

            return result

        except Exception as e:
            error_msg = str(e).lower()

            # 判断是否可重试的错误
            retryable_errors = [
                'timeout', 'timed out', 'connection', 'network',
                'temporarily', 'rate limit', 'too many requests',
                'remote end closed', 'broken pipe'
            ]

            is_retryable = any(err in error_msg for err in retryable_errors)

            if not is_retryable and attempt < MAX_RETRIES - 1:
                print(f"  [WARNING] 非重试错误: {e}")
                return None

            if attempt < MAX_RETRIES - 1:
                # 指数退避：5秒、10秒、15秒
                delay = RETRY_DELAY_BASE * (attempt + 1)
                print(f"  [RETRY] 请求失败 ({e})，{delay}秒后重试 ({attempt + 1}/{MAX_RETRIES})...")
                time.sleep(delay)
            else:
                print(f"  [ERROR] 请求失败，已达到最大重试次数: {e}")
                return None

    return None


async def sync_fina_indicator():
    """同步财务指标数据 (fina_indicator)"""
    print("\n" + "=" * 60)
    print("[1/4] 开始同步财务指标数据(fina_indicator)...")
    print(f"时间范围: {START_DATE} - {END_DATE}")
    print("注意: fina_indicator接口每次最多返回60条记录，每次请求间隔0.35秒")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return False

    try:
        db = db_manager.get_connection()

        # 获取所有股票代码
        stocks_result = db.execute(
            "SELECT ts_code FROM stocks WHERE list_status = 'L'"
        ).fetchall()

        stocks = [s[0] for s in stocks_result]
        print(f"共有 {len(stocks)} 只股票需要同步")

        # 分批处理，每批30只（保守策略，避免超限）
        total_records = 0
        batch_size = 30
        failed_batches = 0

        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            codes = ','.join(batch)

            try:
                # API频率限制
                api_rate_limit()

                # 获取财务指标数据（带重试）
                df = api_call_with_retry(
                    tushare_client.pro.fina_indicator,
                    ts_code=codes,
                    start_date=START_DATE,
                    end_date=END_DATE
                )

                if df is None:
                    failed_batches += 1
                    continue

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
                            AND end_date >= '2025-01-01'
                            AND end_date <= '2026-01-01'
                        """, [code])

                    # 插入新数据
                    columns = df.columns.tolist()
                    db.register('temp_fina', df)

                    # 构建插入SQL（动态根据可用列）
                    available_cols = []
                    for col in columns:
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

                # 每10批次报告进度
                if (i // batch_size) % 10 == 0:
                    print(f"  进度: {min(i+batch_size, len(stocks))}/{len(stocks)}, 累计 {total_records} 条")

                # 每批次后额外休息，确保不超限
                if (i // batch_size) % 5 == 0:
                    await asyncio.sleep(BATCH_REST_TIME)

            except Exception as e:
                print(f"  [WARNING] 同步批次 {i//batch_size + 1} 失败: {e}")
                failed_batches += 1
                # 失败后多休息一会儿
                await asyncio.sleep(2)
                continue

        print(f"[SUCCESS] 财务指标同步完成，共 {total_records} 条记录, 失败批次: {failed_batches}")
        return True

    except Exception as e:
        print(f"[ERROR] 财务指标同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def sync_daily_kline():
    """同步日线数据 (daily)"""
    print("\n" + "=" * 60)
    print("[2/4] 开始同步日线数据(daily)...")
    print(f"时间范围: {START_DATE} - {END_DATE}")
    print("注意: daily接口限制较严格，每次请求间隔0.5秒")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return False

    try:
        db = db_manager.get_connection()

        # 获取2025年的交易日
        result = db.execute("""
            SELECT cal_date FROM trade_calendar
            WHERE is_open = 1
            AND cal_date >= '2025-01-01' AND cal_date <= '2026-01-01'
            ORDER BY cal_date
        """).fetchall()

        trade_dates = [r[0] for r in result]
        print(f"共有 {len(trade_dates)} 个交易日需要同步")

        total_records = 0
        failed_dates = 0

        for idx, trade_date in enumerate(trade_dates):
            try:
                date_str = trade_date.strftime('%Y%m%d')

                # API频率限制（日线接口更严格）
                api_rate_limit_daily()

                # 获取该交易日的所有股票数据（带重试）
                df = api_call_with_retry(
                    tushare_client.pro.daily,
                    trade_date=date_str,
                    _min_interval=DAILY_API_INTERVAL
                )

                if df is None:
                    failed_dates += 1
                    continue

                if not df.empty:
                    # 转换日期格式
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

                    # 删除该日期的旧数据
                    db.execute("""
                        DELETE FROM kline_daily
                        WHERE trade_date = ?
                    """, [trade_date])

                    # 插入新数据
                    db.register('temp_daily', df)
                    db.execute("""
                        INSERT INTO kline_daily
                        SELECT ts_code, trade_date, open, high, low, close,
                               pre_close, change, pct_chg, vol, amount
                        FROM temp_daily
                    """)

                    total_records += len(df)

                # 每10个交易日报告进度
                if idx % 10 == 0:
                    print(f"  进度: {idx+1}/{len(trade_dates)} ({date_str}), 累计 {total_records} 条")

                # 每50个交易日后多休息
                if idx % 50 == 0 and idx > 0:
                    print(f"  已完成50个交易日，休息5秒...")
                    await asyncio.sleep(5)

            except Exception as e:
                print(f"  [WARNING] 同步 {trade_date} 失败: {e}")
                failed_dates += 1
                # 失败后多休息
                await asyncio.sleep(2)
                continue

        print(f"[SUCCESS] 日线数据同步完成，共 {total_records} 条记录, 失败日期: {failed_dates}")
        return True

    except Exception as e:
        print(f"[ERROR] 日线数据同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def sync_daily_basic():
    """同步每日指标数据 (daily_basic)"""
    print("\n" + "=" * 60)
    print("[3/4] 开始同步每日指标数据(daily_basic)...")
    print(f"时间范围: {START_DATE} - {END_DATE}")
    print("注意: daily_basic接口限制较严格，每次请求间隔0.5秒")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
        return False

    try:
        db = db_manager.get_connection()

        # 获取2025年的交易日
        result = db.execute("""
            SELECT cal_date FROM trade_calendar
            WHERE is_open = 1
            AND cal_date >= '2025-01-01' AND cal_date <= '2026-01-01'
            ORDER BY cal_date
        """).fetchall()

        trade_dates = [r[0] for r in result]
        print(f"共有 {len(trade_dates)} 个交易日需要同步")

        total_records = 0
        failed_dates = 0

        for idx, trade_date in enumerate(trade_dates):
            try:
                date_str = trade_date.strftime('%Y%m%d')

                # API频率限制（日线接口更严格）
                api_rate_limit_daily()

                # 获取该交易日的每日指标（带重试）
                df = api_call_with_retry(
                    tushare_client.pro.daily_basic,
                    trade_date=date_str,
                    _min_interval=DAILY_API_INTERVAL
                )

                if df is None:
                    failed_dates += 1
                    continue

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

                # 每10个交易日报告进度
                if idx % 10 == 0:
                    print(f"  进度: {idx+1}/{len(trade_dates)} ({date_str}), 累计 {total_records} 条")

                # 每50个交易日后多休息
                if idx % 50 == 0 and idx > 0:
                    print(f"  已完成50个交易日，休息5秒...")
                    await asyncio.sleep(5)

            except Exception as e:
                print(f"  [WARNING] 同步 {trade_date} 失败: {e}")
                failed_dates += 1
                # 失败后多休息
                await asyncio.sleep(2)
                continue

        print(f"[SUCCESS] 每日指标同步完成，共 {total_records} 条记录, 失败日期: {failed_dates}")
        return True

    except Exception as e:
        print(f"[ERROR] 每日指标同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def sync_dividend():
    """同步分红数据 (dividend)"""
    print("\n" + "=" * 60)
    print("[4/4] 开始同步分红数据(dividend)...")
    print(f"时间范围: {START_DATE} - {END_DATE}")
    print("注意: dividend接口限制，每只股票的请求间隔0.35秒")
    print("=" * 60)

    if tushare_client.pro is None:
        print("[ERROR] Tushare未配置")
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
        failed_stocks = 0
        batch_size = 30

        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]

            for code in batch:
                try:
                    # API频率限制
                    api_rate_limit()

                    # 获取分红数据（带重试）
                    df = api_call_with_retry(
                        tushare_client.pro.dividend,
                        ts_code=code,
                        start_date=START_DATE,
                        end_date=END_DATE
                    )

                    if df is None:
                        failed_stocks += 1
                        continue

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
                            AND end_date >= '2025-01-01'
                            AND end_date <= '2026-01-01'
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

                except Exception as e:
                    print(f"  [WARNING] 同步 {code} 失败: {e}")
                    failed_stocks += 1
                    # 失败后多休息
                    await asyncio.sleep(1)
                    continue

            # 每批次后报告进度
            if (i // batch_size) % 10 == 0:
                print(f"  进度: {min(i+batch_size, len(stocks))}/{len(stocks)}, 累计 {total_records} 条")

            # 每5批次后额外休息
            if (i // batch_size) % 5 == 0:
                await asyncio.sleep(BATCH_REST_TIME)

        print(f"[SUCCESS] 分红数据同步完成，共 {total_records} 条记录, 失败股票: {failed_stocks}")
        return True

    except Exception as e:
        print(f"[ERROR] 分红数据同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def init_db_with_retry():
    """初始化数据库，如果失败则尝试关闭占用进程后重试"""
    max_db_retries = 3

    for attempt in range(max_db_retries):
        try:
            print("初始化数据库...")
            init_db()
            print("数据库初始化完成\n")
            return True
        except Exception as e:
            error_msg = str(e).lower()
            if 'already open' in error_msg or 'cannot open file' in error_msg or '另一程序正在使用' in error_msg:
                if attempt < max_db_retries - 1:
                    print(f"[WARNING] 数据库被占用，尝试关闭占用进程... (尝试 {attempt + 1}/{max_db_retries})")
                    if kill_duckdb_processes():
                        time.sleep(3)
                    else:
                        print("[ERROR] 无法关闭占用进程，请手动停止后端服务")
                        return False
                else:
                    print("[ERROR] 数据库初始化失败，已达到最大重试次数")
                    print(f"错误: {e}")
                    return False
            else:
                print(f"[ERROR] 数据库初始化失败: {e}")
                import traceback
                traceback.print_exc()
                return False

    return False


async def main():
    """主函数 - 顺序执行所有同步任务"""
    start_time = datetime.now()

    print("\n" + "=" * 60)
    print("开始同步2025年全年数据")
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"时间范围: {START_DATE} - {END_DATE}")
    print("=" * 60)
    print("\nAPI频率控制策略:")
    print(f"  - 财务指标/分红接口: 每 {API_CALL_INTERVAL} 秒 1 次（约 {int(60/API_CALL_INTERVAL)} 次/分钟）")
    print(f"  - 日线/每日指标接口: 每 {DAILY_API_INTERVAL} 秒 1 次（约 {int(60/DAILY_API_INTERVAL)} 次/分钟）")
    print(f"  - 每批次后休息: {BATCH_REST_TIME} 秒")
    print("\n重试机制:")
    print(f"  - 最大重试次数: {MAX_RETRIES}")
    print(f"  - 重试延迟: {RETRY_DELAY_BASE}秒、{RETRY_DELAY_BASE*2}秒、{RETRY_DELAY_BASE*3}秒")
    print(f"  - 请求超时: {REQUEST_TIMEOUT} 秒")
    print("=" * 60 + "\n")

    # 初始化数据库（带重试）
    if not init_db_with_retry():
        return

    # 检查Tushare配置
    if tushare_client.pro is None:
        print("\n[ERROR] TUSHARE_TOKEN未配置或无效！")
        print("请访问 https://tushare.pro 注册并获取Token")
        print("然后将Token添加到 backend/.env 文件中")
        return

    print(f"Tushare配置成功\n")

    # 顺序执行同步任务
    results = []

    # 1. 财务指标
    results.append(("财务指标(fina_indicator)", await sync_fina_indicator()))

    # 2. 日线数据
    results.append(("日线数据(daily)", await sync_daily_kline()))

    # 3. 每日指标
    results.append(("每日指标(daily_basic)", await sync_daily_basic()))

    # 4. 分红数据
    results.append(("分红数据(dividend)", await sync_dividend()))

    # 计算总耗时
    end_time = datetime.now()
    duration = end_time - start_time

    # 打印汇总
    print("\n" + "=" * 60)
    print("同步完成汇总")
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {duration}")
    print("=" * 60)
    for name, success in results:
        status = "成功" if success else "失败"
        print(f"{name}: {status}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
