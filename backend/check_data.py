"""
检查数据库数据
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.db.session import db_manager

db = db_manager.get_connection()

print("=" * 60)
print("数据库数据检查")
print("=" * 60)

# 1. 检查K线数据
print("\n1. K线数据（前10只股票）:")
result = db.execute("""
    SELECT ts_code, COUNT(*) as cnt, MIN(trade_date), MAX(trade_date)
    FROM kline_daily
    GROUP BY ts_code
    ORDER BY cnt DESC
    LIMIT 10
""").fetchall()
for row in result:
    print(f"  {row[0]}: {row[1]}条, {row[2]} 至 {row[3]}")

# 2. 检查分红数据
print("\n2. 分红数据:")
count = db.execute("SELECT COUNT(*) FROM stock_dividend").fetchone()[0]
print(f"  总记录数: {count}")
if count > 0:
    result = db.execute("""
        SELECT ts_code, base_date, cash_div
        FROM stock_dividend
        ORDER BY base_date DESC
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"  {row[0]}: {row[1]}, 分红{row[2]}元")

# 3. 检查每日指标（PE/PB/市值）
print("\n3. 每日指标数据:")
count = db.execute("SELECT COUNT(*) FROM stock_daily_basic").fetchone()[0]
print(f"  总记录数: {count}")
if count > 0:
    result = db.execute("""
        SELECT ts_code, trade_date, pe, pb, total_mv
        FROM stock_daily_basic
        ORDER BY trade_date DESC
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"  {row[0]}: {row[1]}, PE={row[2]}, PB={row[3]}, 市值={row[4]}")

# 4. 检查财务指标（ROE/增长率）
print("\n4. 财务指标数据:")
count = db.execute("SELECT COUNT(*) FROM stock_fina_indicator").fetchone()[0]
print(f"  总记录数: {count}")
if count > 0:
    result = db.execute("""
        SELECT ts_code, end_date, roe_dt, revenue_yoy, profit_yoy
        FROM stock_fina_indicator
        ORDER BY end_date DESC
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"  {row[0]}: {row[1]}, ROE={row[2]}, 营收增长={row[3]}, 利润增长={row[4]}")

# 5. 检查是否有2026年1-3月的数据
print("\n5. 2026年1-3月数据:")
result = db.execute("""
    SELECT ts_code, COUNT(*)
    FROM kline_daily
    WHERE trade_date BETWEEN '2026-01-01' AND '2026-03-01'
    GROUP BY ts_code
    ORDER BY COUNT(*) DESC
    LIMIT 10
""").fetchall()
for row in result:
    print(f"  {row[0]}: {row[1]}条")

print("\n" + "=" * 60)
