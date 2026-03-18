"""
简单测试策略 - 验证回测流程
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.db.session import db_manager

# 创建简单策略
test_strategy = '''
def initialize(context):
    set_benchmark('000300.XSHG')
    log.info('策略初始化成功')
    g.stock_num = 5
    g.initialized = True

def handle_data(context, data):
    # 简单的买入持有策略
    if not hasattr(g, 'bought') or not g.bought:
        stocks = get_all_securities('stock').index.tolist()[:10]
        log.info(f'选中股票: {stocks}')

        # 每只买入10%
        for stock in stocks[:g.stock_num]:
            order_value(stock, context.portfolio.total_value * 0.1)

        g.bought = True
'''

# 保存策略
db = db_manager.get_connection()
import uuid
strategy_id = str(uuid.uuid4())

db.execute("""
    INSERT INTO strategies (id, name, code, description, strategy_type, parameters, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, now(), now())
    ON CONFLICT (id) DO UPDATE SET
        code = excluded.code,
        updated_at = now()
""", [
    'test-simple-strategy',
    '简单测试策略',
    test_strategy,
    '用于测试回测流程的简单策略',
    'jqdata',
    '{}'
])

print("策略已保存")

# 运行回测
print("\n运行回测...")
from app.engine.backtrader_wrapper import BacktraderEngine
from app.engine.jqdata_strategy_converter import JQStrategyConverter

engine = BacktraderEngine()

# 加载策略
strategy_class = engine.load_strategy(test_strategy, strategy_type='jqdata')
print(f"策略加载成功: {strategy_class.__name__}")

# 加载数据
data = engine.load_data(
    symbol='000300.SH',
    start_date='2026-01-01',
    end_date='2026-03-01'
)
print(f"数据加载成功: {len(data)} 条")

# 执行回测
result = engine.run_backtest(
    strategy_class=strategy_class,
    data=data,
    config={
        'initial_cash': 1000000.0,
        'commission': 0.00025,
        'slippage': 0.001,
        'parameters': {}
    }
)

print("\n" + "=" * 60)
print("回测结果:")
print("=" * 60)
print(f"总收益率: {result['metrics']['total_return']:.2f}%")
print(f"年化收益率: {result['metrics']['annual_return']:.2f}%")
print(f"最大回撤: {result['metrics']['max_drawdown']:.2f}%")
print(f"夏普比率: {result['metrics']['sharpe_ratio']:.2f}")
print(f"总交易次数: {result['metrics']['total_trades']}")
print(f"权益曲线点数: {len(result['equity_curve'])}")

if result['trades']:
    print(f"\n交易记录 ({len(result['trades'])} 条):")
    for trade in result['trades'][:5]:
        print(f"  {trade}")
else:
    print("\n没有交易记录")
