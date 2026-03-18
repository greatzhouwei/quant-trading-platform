"""
调试股息策略
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import datetime
from app.db.session import db_manager

print("=" * 60)
print("调试股息策略依赖的函数")
print("=" * 60)

# 初始化适配器
db = db_manager.get_connection()

# 测试1: get_all_securities
print("\n1. 测试 get_all_securities")
from app.engine.jqdata_adapter import get_all_securities
stocks = get_all_securities('stock')
print(f"   返回 {len(stocks)} 只股票")
print(f"   前5只: {list(stocks.index[:5])}")

# 测试2: 检查这些股票是否有K线数据
print("\n2. 检查K线数据覆盖")
sample_stocks = list(stocks.index[:10])
for code in sample_stocks:
    count = db.execute(
        "SELECT COUNT(*) FROM kline_daily WHERE ts_code = ? AND trade_date BETWEEN '2026-01-01' AND '2026-02-28'",
        [code]
    ).fetchone()[0]
    print(f"   {code}: {count}条K线")

# 测试3: 检查每日指标数据
print("\n3. 检查每日指标数据")
result = db.execute("""
    SELECT ts_code, trade_date, pe, pb, total_mv
    FROM stock_daily_basic
    WHERE trade_date = '2026-01-05'
    LIMIT 5
""").fetchall()
for row in result:
    print(f"   {row[0]}: PE={row[2]}, PB={row[3]}, 市值={row[4]}")

# 测试4: 检查分红数据
print("\n4. 检查分红数据")
count = db.execute("SELECT COUNT(*) FROM stock_dividend").fetchone()[0]
print(f"   总记录数: {count}")
if count > 0:
    result = db.execute("SELECT * FROM stock_dividend LIMIT 3").fetchall()
    for row in result:
        print(f"   {row}")

# 测试5: 检查财务指标
print("\n5. 检查财务指标")
count = db.execute("SELECT COUNT(*) FROM stock_fina_indicator").fetchone()[0]
print(f"   总记录数: {count}")
result = db.execute("""
    SELECT ts_code, end_date, roe_dt
    FROM stock_fina_indicator
    ORDER BY end_date DESC
    LIMIT 5
""").fetchall()
for row in result:
    print(f"   {row[0]}: {row[1]}, ROE={row[2]}")

# 测试6: 模拟选股逻辑
print("\n6. 模拟选股逻辑")
# 获取有数据的股票
df_stocks = db.execute("""
    SELECT DISTINCT k.ts_code
    FROM kline_daily k
    JOIN stock_daily_basic d ON k.ts_code = d.ts_code AND k.trade_date = d.trade_date
    WHERE k.trade_date = '2026-01-05'
    LIMIT 100
""").fetchdf()
print(f"   有K线和每日指标数据的股票: {len(df_stocks)}只")

print("\n" + "=" * 60)
