"""
测试股息策略回测
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.db.session import db_manager
from app.engine.backtrader_wrapper import BacktraderEngine
from app.engine.jqdata_strategy_converter import JQStrategyConverter

# 获取策略代码
db = db_manager.get_connection()
strategy = db.execute(
    "SELECT code FROM strategies WHERE id = 'e739776a-41ad-4ba2-89ed-c01b6f57cdd0'"
).fetchone()

if not strategy:
    print("策略不存在")
    sys.exit(1)

strategy_code = strategy[0]
print("=" * 60)
print("策略代码前1000字符:")
print(strategy_code[:1000])
print("=" * 60)

# 检测策略类型
is_jq = JQStrategyConverter.is_jq_strategy(strategy_code)
print(f"\n是否为聚宽策略: {is_jq}")

if is_jq:
    print("\n正在转换策略...")
    try:
        converted_code = JQStrategyConverter.convert_to_backtrader(strategy_code)
        print("策略转换成功")
        print("\n转换后的代码前2000字符:")
        print(converted_code[:2000])
    except Exception as e:
        print(f"策略转换失败: {e}")
        import traceback
        traceback.print_exc()

# 尝试加载策略
print("\n" + "=" * 60)
print("尝试加载策略...")
engine = BacktraderEngine()

try:
    strategy_class = engine.load_strategy(strategy_code, strategy_type='jqdata')
    print(f"策略加载成功: {strategy_class}")
except Exception as e:
    print(f"策略加载失败: {e}")
    import traceback
    traceback.print_exc()

# 检查数据
print("\n" + "=" * 60)
print("检查数据库数据...")

# 检查沪深300数据
count = db.execute("SELECT COUNT(*) FROM kline_daily WHERE ts_code = '000300.SH'").fetchone()[0]
print(f"000300.SH K线数据条数: {count}")

# 检查分红数据
count = db.execute("SELECT COUNT(*) FROM stock_dividend").fetchone()[0]
print(f"分红数据条数: {count}")

# 检查财务指标数据
count = db.execute("SELECT COUNT(*) FROM stock_fina_indicator").fetchone()[0]
print(f"财务指标数据条数: {count}")

# 检查每日指标数据
count = db.execute("SELECT COUNT(*) FROM stock_daily_basic").fetchone()[0]
print(f"每日指标数据条数: {count}")

print("\n" + "=" * 60)
print("检查完成")
