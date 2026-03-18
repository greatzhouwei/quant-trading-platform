"""
直接测试股息策略回测
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import datetime
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
print("开始测试股息策略回测")
print("=" * 60)

# 检查必要的数据
print("\n检查数据...")
count = db.execute("SELECT COUNT(*) FROM kline_daily WHERE ts_code = '000300.SH' AND trade_date BETWEEN '2026-01-01' AND '2026-03-01'").fetchone()[0]
print(f"000300.SH 2026年1-3月K线数据条数: {count}")

# 尝试转换策略
print("\n转换策略...")
try:
    converted_code = JQStrategyConverter.convert_to_backtrader(strategy_code)
    print(f"策略转换成功，代码长度: {len(converted_code)} 字符")
except Exception as e:
    print(f"策略转换失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 尝试加载策略
print("\n加载策略...")
engine = BacktraderEngine()

try:
    strategy_class = engine.load_strategy(strategy_code, strategy_type='jqdata')
    print(f"策略加载成功: {strategy_class.__name__}")
except Exception as e:
    print(f"策略加载失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 加载数据
print("\n加载数据...")
try:
    data = engine.load_data(
        symbol='000300.SH',
        start_date='2026-01-01',
        end_date='2026-03-01',
        timeframe='1d'
    )
    print(f"数据加载成功: {len(data)} 条")
except Exception as e:
    print(f"数据加载失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 执行回测
print("\n执行回测...")
try:
    result = engine.run_backtest(
        strategy_class=strategy_class,
        data=data,
        config={
            'initial_cash': 1000000.0,
            'commission': 0.00025,
            'slippage': 0.001,
            'parameters': {
                'print_log': True
            }
        }
    )
    print("\n" + "=" * 60)
    print("回测结果:")
    print("=" * 60)
    print(f"总收益率: {result['metrics']['total_return']:.2f}%")
    print(f"年化收益率: {result['metrics']['annual_return']:.2f}%")
    print(f"最大回撤: {result['metrics']['max_drawdown']:.2f}%")
    print(f"夏普比率: {result['metrics']['sharpe_ratio']:.2f}")
    print(f"胜率: {result['metrics']['win_rate']:.2f}%")
    print(f"总交易次数: {result['metrics']['total_trades']}")
    print(f"权益曲线点数: {len(result['equity_curve'])}")

except Exception as e:
    print(f"回测执行失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)
