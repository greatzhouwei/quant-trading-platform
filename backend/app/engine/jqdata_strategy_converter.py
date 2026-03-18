"""
聚宽策略转换器

将聚宽策略代码转换为backtrader可执行策略
"""
import re
import ast
from typing import Optional, List, Dict, Any


class JQStrategyConverter:
    """
    聚宽策略转换器

    功能：
    1. 检测代码是否为聚宽策略
    2. 将聚宽代码转换为backtrader策略类
    """

    # 聚宽策略特征模式
    JQ_PATTERNS = [
        r'def\s+initialize\s*\(\s*context\s*\)',
        r'def\s+handle_data\s*\(\s*context\s*,\s*data\s*\)',
        r'def\s+before_trading_start\s*\(\s*context\s*\)',
        r'def\s+after_trading_end\s*\(\s*context\s*\)',
        r'from\s+jqdata\s+import',
        r'set_benchmark\s*\(',
        r'set_option\s*\(',
        r'set_slippage\s*\(',
        r'get_all_securities\s*\(',
        r'get_fundamentals\s*\(',
        r'get_current_data\s*\(',
        r'order\s*\(',
        r'order_value\s*\(',
        r'order_target\s*\(',
        r'order_target_value\s*\(',
        r'get_price\s*\(',
        r'history\s*\(',
        r'attribute_history\s*\(',
        r'record\s*\(',
        r'run_query\s*\(',
    ]

    @classmethod
    def is_jq_strategy(cls, code: str) -> bool:
        """
        检测代码是否为聚宽策略

        通过检测聚宽特有的函数调用来判断

        Args:
            code: 策略代码字符串

        Returns:
            bool: 是否为聚宽策略
        """
        for pattern in cls.JQ_PATTERNS:
            if re.search(pattern, code):
                return True
        return False

    @classmethod
    def convert_to_backtrader(cls, code: str) -> str:
        """
        将聚宽代码转换为backtrader策略类

        转换逻辑：
        1. 保留用户所有函数定义和变量
        2. 创建backtrader策略类包装器
        3. 在__init__中调用initialize
        4. 在prenext中调用before_trading_start
        5. 在next中调用handle_data
        6. 提供Context对象和全局函数

        Args:
            code: 聚宽策略代码

        Returns:
            str: 转换后的backtrader策略代码
        """
        # 解析用户代码，提取函数定义
        functions = cls._extract_functions(code)

        # 构建转换后的代码 - 使用format而不是f-string避免大括号转义问题
        parts = []

        # 头部导入
        parts.append('''import backtrader as bt
import pandas as pd
import datetime
from types import SimpleNamespace
from typing import Dict, Any

# ============================================================================
# 聚宽API适配器导入
# ============================================================================
from app.engine.jqdata_adapter import (
    JQContext, Portfolio, Position, CurrentData, RunParams,
    FixedSlippage, log, set_benchmark, set_option, set_slippage,
    get_all_securities, get_price, get_current_data, get_fundamentals, get_security_info,
    history, attribute_history, record, run_query, query, valuation, indicator,
    order, order_value, order_target, order_target_value,
    calculate_limit_price, get_records
)

# finance模块导入
from app.engine.jqdata_adapter import finance

# ============================================================================
# 全局变量初始化
# ============================================================================
context = None  # 将由JQStrategyWrapper初始化
g = SimpleNamespace()  # 聚宽全局变量对象

# ============================================================================
# 用户原始代码（保留所有函数定义，jqdata导入已处理）
# ============================================================================
''')

        # 用户代码 - 处理jqdata导入
        processed_code = cls._process_user_code(code)
        parts.append(processed_code)

        # 包装类模板
        parts.append('''

# ============================================================================
# 自动生成的Backtrader策略包装类
# ============================================================================

class JQStrategyWrapper(bt.Strategy):
    """
    聚宽策略的Backtrader包装器

    将聚宽的事件驱动模型映射到Backtrader的时间序列模型
    """

    params = (
        ('initial_cash', 100000.0),
        ('commission', 0.00025),
        ('print_log', True),
    )

    def __init__(self):
        """初始化策略"""
        # 初始化聚宽Context
        self._init_context()

        # 存储记录数据
        self._records = []

        # 交易记录列表（注意：避免与backtrader内部的_trades冲突）
        self._jq_trades = []

        # 初始化标志
        self._initialized = False

        # 当前日期（用于检测新的一天）
        self._current_date = None

        # 数据源映射
        self._data_map = {}

        # 构建数据源映射
        for i, data in enumerate(self.datas):
            # 尝试从数据源获取代码
            code = getattr(data, '_name', None) or 'stock_' + str(i)
            self._data_map[code] = data
            # 反向引用
            data._jq_code = code

    def _init_context(self):
        """初始化聚宽Context"""
        global context

        # 创建新的Context
        context = JQContext()
        self.context = context

        # 同时更新jqdata_adapter模块的context
        import sys
        if 'app.engine.jqdata_adapter' in sys.modules:
            sys.modules['app.engine.jqdata_adapter'].context = context

        # 设置初始资金
        context.portfolio.starting_cash = self.p.initial_cash
        context.portfolio.available_cash = self.p.initial_cash
        context.portfolio.total_value = self.p.initial_cash

        # 设置运行参数
        context.run_params.start_date = None
        context.run_params.end_date = None
        context.run_params.type = 'backtest'

        # 绑定backtrader对象
        context._broker = self.broker
        context._datas = self.datas
        context._strategy = self

        # 辅助方法
        context._get_data = self._get_data_by_code

    def _get_data_by_code(self, code: str):
        """根据股票代码获取数据源"""
        return self._data_map.get(code)

    def log(self, txt, dt=None):
        """日志输出"""
        if self.p.print_log:
            dt = dt or self.data.datetime.date(0)
            print(str(dt.isoformat()) + ', ' + str(txt))

    def start(self):
        """策略开始时调用（早于__init__之后的任何操作）"""
        # 获取回测日期范围
        if len(self.data) > 0:
            # 获取起始日期
            context.run_params.start_date = self.data.datetime.date(0)

    def prenext(self):
        """
        每个bar之前调用

        在聚宽中对应 before_trading_start
        """
        dt = self.data.datetime.datetime(0)
        current_date = dt.date()

        # 检测是否是新的一天
        is_new_day = (self._current_date != current_date)

        if is_new_day:
            self._current_date = current_date

            # 更新Context时间
            context.current_dt = dt
            context.previous_date = self._get_previous_date()

            # 更新Portfolio
            self._update_portfolio()

            # 调用用户的before_trading_start
            try:
                if 'before_trading_start' in globals():
                    before_trading_start(context)
            except Exception as e:
                log.error('before_trading_start错误: ' + str(e))

    def next(self):
        """
        每个bar调用

        在聚宽中对应 handle_data
        """
        dt = self.data.datetime.datetime(0)
        current_date = dt.date()

        # 更新Context时间（在调用initialize之前）
        context.current_dt = dt
        context.previous_date = self._get_previous_date()

        # 首次运行时调用initialize
        if not self._initialized:
            self._initialized = True
            try:
                if 'initialize' in globals():
                    initialize(context)
            except Exception as e:
                log.error('initialize错误: ' + str(e))
                return

        # 更新Portfolio
        self._update_portfolio()

        # 更新Portfolio
        self._update_portfolio()

        # 构建data对象（简化版）
        data_obj = SimpleNamespace()
        data_obj.dt = dt

        # 调用用户的handle_data
        try:
            if 'handle_data' in globals():
                handle_data(context, data_obj)
        except Exception as e:
            log.error('handle_data错误: ' + str(e))

        # 检测交易结束（简化：假设日线数据，每天收盘后）
        try:
            if 'after_trading_end' in globals():
                after_trading_end(context)
        except Exception as e:
            log.error('after_trading_end错误: ' + str(e))

    def _update_portfolio(self):
        """更新Portfolio信息"""
        # 获取当前资金
        available_cash = self.broker.getcash()
        total_value = self.broker.getvalue()

        context.portfolio.available_cash = available_cash
        context.portfolio.total_value = total_value

        # 更新持仓
        for code, data in self._data_map.items():
            pos = self.broker.getposition(data)

            if pos.size != 0:
                if code not in context.portfolio.positions:
                    context.portfolio.positions[code] = Position(code)

                position = context.portfolio.positions[code]
                position.total_amount = int(pos.size)
                position.available_amount = int(pos.size)  # 简化：假设都可卖
                position.avg_cost = pos.price if pos.price else 0.0
                position.price = data.close[0]
                position.value = pos.size * data.close[0]

                # 计算盈亏
                if position.avg_cost > 0:
                    position.pnl = (data.close[0] - position.avg_cost) * pos.size
            else:
                # 清仓的从positions中移除
                if code in context.portfolio.positions:
                    del context.portfolio.positions[code]

        # 计算收益率
        if context.portfolio.starting_cash > 0:
            context.portfolio.returns = (
                (total_value - context.portfolio.starting_cash)
                / context.portfolio.starting_cash
            )

    def _get_previous_date(self):
        """获取上一个交易日"""
        if len(self.data) > 1:
            return self.data.datetime.date(-1)
        return None

    def notify_order(self, order):
        """订单状态通知"""
        # 调试：打印所有订单状态
        status_names = ['Created', 'Submitted', 'Accepted', 'Partial', 'Completed',
                        'Canceled', 'Expired', 'Margin', 'Rejected']
        status_name = status_names[order.status] if order.status < len(status_names) else str(order.status)

        if order.status in [order.Completed]:
            trade_type = '买入' if order.isbuy() else '卖出'
            self.log('%s执行, 价格: %.2f, 数量: %d' % (trade_type, order.executed.price, order.executed.size))

            # 记录交易到列表 - 使用当前bar的日期
            dt = self.data.datetime.datetime()
            if hasattr(dt, 'isoformat'):
                dt_str = dt.isoformat()
            else:
                dt_str = str(dt)

            trade_record = {
                'datetime': dt_str,
                'type': trade_type,
                'price': order.executed.price,
                'size': int(order.executed.size),
                'value': order.executed.value,
                'commission': order.executed.comm,
                'pnl': None,  # 单笔订单没有pnl，在notify_trade中更新
            }
            self._jq_trades.append(trade_record)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单取消/拒绝/保证金不足: %s' % status_name)

    def notify_trade(self, trade):
        """交易完成通知"""
        if trade.isclosed:
            # 更新最后一笔交易的pnl
            if self._jq_trades:
                self._jq_trades[-1]['pnl'] = trade.pnl

    def get_trades(self):
        """获取交易记录列表"""
        return self._jq_trades


# ============================================================================
# 导出策略类（Backtrader将使用这个类）
# ============================================================================
JQStrategy = JQStrategyWrapper
''')

        return ''.join(parts)

    @staticmethod
    def _process_user_code(code: str) -> str:
        """
        处理用户代码，注释掉jqdata导入等

        Args:
            code: 用户原始代码

        Returns:
            处理后的代码
        """
        import re

        lines = code.split('\n')
        processed_lines = []

        for line in lines:
            stripped = line.strip()

            # 注释掉jqdata导入
            if re.match(r'^from\s+jqdata\s+import', stripped):
                processed_lines.append('# [已注释] ' + line)
                continue

            if re.match(r'^import\s+jqdata', stripped):
                processed_lines.append('# [已注释] ' + line)
                continue

            processed_lines.append(line)

        return '\n'.join(processed_lines)

    @staticmethod
    def _extract_functions(code: str) -> Dict[str, str]:
        """
        从代码中提取函数定义

        Args:
            code: Python代码

        Returns:
            Dict[函数名, 函数代码]
        """
        functions = {}

        try:
            tree = ast.parse(code)

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    functions[node.name] = ast.unparse(node)

        except SyntaxError:
            # 语法错误时返回空字典
            pass

        return functions

    @staticmethod
    def extract_strategy_info(code: str) -> Dict[str, Any]:
        """
        提取策略信息

        Args:
            code: 策略代码

        Returns:
            策略信息字典
        """
        info = {
            'is_jq_strategy': False,
            'has_initialize': False,
            'has_handle_data': False,
            'has_before_trading_start': False,
            'has_after_trading_end': False,
            'detected_patterns': [],
        }

        # 检测是否为聚宽策略
        info['is_jq_strategy'] = JQStrategyConverter.is_jq_strategy(code)

        # 检测特定函数
        if re.search(r'def\s+initialize\s*\(', code):
            info['has_initialize'] = True
        if re.search(r'def\s+handle_data\s*\(', code):
            info['has_handle_data'] = True
        if re.search(r'def\s+before_trading_start\s*\(', code):
            info['has_before_trading_start'] = True
        if re.search(r'def\s+after_trading_end\s*\(', code):
            info['has_after_trading_end'] = True

        # 记录检测到的模式
        for pattern in JQStrategyConverter.JQ_PATTERNS:
            if re.search(pattern, code):
                info['detected_patterns'].append(pattern)

        return info


def convert_jq_strategy(code: str) -> str:
    """
    转换聚宽策略的便捷函数

    Args:
        code: 聚宽策略代码

    Returns:
        转换后的backtrader策略代码
    """
    converter = JQStrategyConverter()
    return converter.convert_to_backtrader(code)


def detect_strategy_type(code: str) -> str:
    """
    检测策略类型

    Args:
        code: 策略代码

    Returns:
        'jqdata' 或 'backtrader'
    """
    if JQStrategyConverter.is_jq_strategy(code):
        return 'jqdata'

    # 检测是否为backtrader策略（包含继承bt.Strategy的类）
    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    if isinstance(base, ast.Attribute) and base.attr == 'Strategy':
                        return 'backtrader'
                    elif isinstance(base, ast.Name) and base.id == 'Strategy':
                        return 'backtrader'
    except SyntaxError:
        pass

    # 默认返回jqdata（更宽松的检测）
    return 'jqdata'
