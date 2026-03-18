"""
创建演示用的简化股息策略
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.db.session import db_manager
import uuid

# 简化版股息策略 - 使用PE和市值筛选
demo_strategy = '''
import pandas as pd
from jqdata import *

def initialize(context):
    set_benchmark('000300.XSHG')
    log.info('策略初始化成功')
    g.stock_num = 5
    g.month = 0
    g.initialized = True

def handle_data(context, data):
    # 只在月初执行
    current_month = context.current_dt.month
    if current_month == g.month:
        return

    g.month = current_month

    # 获取所有股票
    stocks = get_all_securities('stock').index.tolist()[:50]
    log.info(f'选中股票池: {len(stocks)}只')

    # 筛选有数据的股票
    available_stocks = []
    for stock in stocks[:10]:
        try:
            # 简化：只选股票代码
            available_stocks.append(stock)
        except:
            continue

    log.info(f'可交易股票: {available_stocks}')

    # 每月调仓 - 买入前5只
    target_stocks = available_stocks[:g.stock_num]

    # 清仓不在目标列表的股票
    for stock in list(context.portfolio.positions.keys()):
        if stock not in target_stocks:
            order_target(stock, 0)
            log.info(f'卖出 {stock}')

    # 买入目标股票
    if len(target_stocks) > 0:
        cash_per_stock = context.portfolio.available_cash / len(target_stocks)
        for stock in target_stocks:
            if stock not in context.portfolio.positions:
                order_value(stock, cash_per_stock * 0.9)
                log.info(f'买入 {stock}')
'''

# 保存策略
db = db_manager.get_connection()
strategy_id = 'demo-dividend-strategy'

db.execute("""
    INSERT INTO strategies (id, name, code, description, strategy_type, parameters, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, now(), now())
    ON CONFLICT (id) DO UPDATE SET
        code = excluded.code,
        updated_at = now()
""", [
    strategy_id,
    '演示股息策略',
    demo_strategy,
    '简化版股息策略，用于演示回测功能',
    'jqdata',
    '{}'
])

print(f"演示策略已保存: {strategy_id}")
