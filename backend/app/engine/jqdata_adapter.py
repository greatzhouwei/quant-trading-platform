"""
聚宽(JoinQuant) API 兼容层

提供与聚宽API兼容的函数和类，让用户可以直接粘贴聚宽策略代码运行。
"""
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field

from app.db.session import db_manager

# 兼容旧版 pandas DataFrame.append（新版已移除）
if not hasattr(pd.DataFrame, 'append'):
    def _df_append(self, other, ignore_index=False, verify_integrity=False, sort=False):
        # 保存原始列的dtype（避免date object被转成datetime64）
        dtypes = self.dtypes.to_dict()
        result = pd.concat([self, other], ignore_index=ignore_index, sort=sort)
        # 恢复object类型的列（避免date被转换成datetime64）
        for col, dtype in dtypes.items():
            if dtype == object and col in result.columns:
                try:
                    result[col] = result[col].astype(object)
                except Exception:
                    pass
        return result
    pd.DataFrame.append = _df_append


# ============================================================================
# 股票代码格式转换工具
# ============================================================================

def normalize_code(code: str) -> str:
    """
    将聚宽股票代码格式转换为Tushare格式

    聚宽格式:
        - 上海证券交易所: 600519.XSHG
        - 深圳证券交易所: 000001.XSHE
    Tushare格式:
        - 上海证券交易所: 600519.SH
        - 深圳证券交易所: 000001.SZ

    Args:
        code: 股票代码（支持聚宽或Tushare格式）

    Returns:
        Tushare格式的股票代码
    """
    if not code:
        return code

    # 已经是Tushare格式
    if code.endswith('.SH') or code.endswith('.SZ'):
        return code

    # 聚宽格式转换
    if code.endswith('.XSHG'):
        return code.replace('.XSHG', '.SH')
    elif code.endswith('.XSHE'):
        return code.replace('.XSHE', '.SZ')

    # 无后缀格式，根据数字判断
    # 6开头为上海，0/3开头为深圳
    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('0') or code.startswith('3'):
        return f"{code}.SZ"

    # 无法识别，原样返回
    return code


def normalize_code_list(codes: List[str]) -> List[str]:
    """
    批量转换股票代码格式

    Args:
        codes: 股票代码列表

    Returns:
        转换后的代码列表
    """
    return [normalize_code(c) for c in codes]


def to_jq_code(code: str) -> str:
    """
    将Tushare格式转换为聚宽格式

    Args:
        code: Tushare格式代码

    Returns:
        聚宽格式代码
    """
    if not code:
        return code

    if code.endswith('.SH'):
        return code.replace('.SH', '.XSHG')
    elif code.endswith('.SZ'):
        return code.replace('.SZ', '.XSHE')

    return code


# ============================================================================
# 全局 Context 对象
# ============================================================================

class JQContext:
    """
    聚宽 context 对象模拟

    包含：
    - portfolio: 账户资金和持仓信息
    - current_dt: 当前时间
    - previous_date: 上一个交易日
    - run_params: 运行参数
    - g: 全局变量存储（用户自定义）
    """
    def __init__(self):
        self.portfolio: 'Portfolio' = Portfolio()
        self.current_dt: Optional[datetime] = None
        self.previous_date: Optional[datetime] = None
        self.run_params: 'RunParams' = RunParams()
        self.g: SimpleNamespace = SimpleNamespace()

        # 内部设置存储
        self._settings: Dict[str, Any] = {
            'benchmark': None,
            'use_real_price': False,
            'avoid_future_data': False,
            'slippage': None,
        }

        # 内部数据缓存
        self._cache: Dict[str, Any] = {}

        # backtrader 对象引用（由包装器设置）
        self._broker = None
        self._datas = None
        self._strategy = None


class RunParams:
    """运行参数对象"""
    def __init__(self):
        self.type = 'simulation'  # 模拟交易
        self.start_date = None
        self.end_date = None
        self.frequency = 'daily'


# ============================================================================
# Portfolio 和 Position 类
# ============================================================================

@dataclass
class Position:
    """
    持仓对象

    模拟聚宽的 Position 对象
    """
    security: str
    total_amount: int = 0
    available_amount: int = 0
    avg_cost: float = 0.0
    price: float = 0.0
    value: float = 0.0
    pnl: float = 0.0

    def __repr__(self):
        return f"Position({self.security}, amount={self.total_amount}, cost={self.avg_cost:.2f})"


class Portfolio:
    """
    账户组合对象

    模拟聚宽的 Portfolio 对象
    """
    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.available_cash: float = 0.0
        self.total_value: float = 0.0
        self.returns: float = 0.0
        self.starting_cash: float = 0.0

    def __repr__(self):
        return f"Portfolio(cash={self.available_cash:.2f}, value={self.total_value:.2f}, positions={len(self.positions)})"


# ============================================================================
# 当前数据对象
# ============================================================================

@dataclass
class CurrentData:
    """
    当前时刻数据对象

    模拟聚宽的 current_data 对象
    """
    day_open: float = 0.0      # 开盘价
    high: float = 0.0          # 最高价
    low: float = 0.0           # 最低价
    close: float = 0.0         # 收盘价（最新价）
    last_price: float = 0.0    # 最新价
    high_limit: float = 0.0    # 涨停价
    low_limit: float = 0.0     # 跌停价
    volume: float = 0.0        # 成交量
    money: float = 0.0         # 成交额
    paused: bool = False       # 是否停牌
    is_st: bool = False        # 是否ST
    name: str = ''             # 股票名称

    @property
    def price(self):
        """兼容属性"""
        return self.last_price


# ============================================================================
# 滑点类
# ============================================================================

class FixedSlippage:
    """固定滑点"""
    def __init__(self, value: float):
        """
        Args:
            value: 滑点值（元）
        """
        self.value = value

    def __repr__(self):
        return f"FixedSlippage({self.value})"


# ============================================================================
# 日志对象
# ============================================================================

class JQLog:
    """聚宽日志对象"""

    _level = 'info'
    _logs: List[str] = []

    @classmethod
    def set_level(cls, *levels):
        """设置日志级别"""
        if levels:
            cls._level = levels[0]

    @classmethod
    def info(cls, msg, *args):
        """信息日志"""
        if cls._level in ['info', 'debug']:
            message = msg % args if args else msg
            log_entry = f"[INFO] {message}"
            print(log_entry)
            cls._logs.append(log_entry)

    @classmethod
    def warn(cls, msg, *args):
        """警告日志"""
        if cls._level in ['info', 'debug', 'warning']:
            message = msg % args if args else msg
            log_entry = f"[WARN] {message}"
            print(log_entry)
            cls._logs.append(log_entry)

    @classmethod
    def error(cls, msg, *args):
        """错误日志"""
        message = msg % args if args else msg
        log_entry = f"[ERROR] {message}"
        print(log_entry)
        cls._logs.append(log_entry)

    @classmethod
    def debug(cls, msg, *args):
        """调试日志"""
        if cls._level == 'debug':
            message = msg % args if args else msg
            log_entry = f"[DEBUG] {message}"
            print(log_entry)
            cls._logs.append(log_entry)

    @classmethod
    def get_logs(cls) -> List[str]:
        """获取所有日志"""
        return cls._logs.copy()

    @classmethod
    def clear_logs(cls):
        """清空日志"""
        cls._logs.clear()


# 全局日志对象
log = JQLog()


# ============================================================================
# 初始化函数
# ============================================================================

def set_benchmark(benchmark: str):
    """
    设置基准

    Args:
        benchmark: 基准代码，如 '000300.XSHG' (沪深300)
    """
    global context
    if 'context' in globals():
        context._settings['benchmark'] = benchmark


def set_option(key: str, value):
    """
    设置选项

    Args:
        key: 选项名称
            - use_real_price: 是否使用真实价格
            - avoid_future_data: 是否避免未来数据
        value: 选项值
    """
    global context
    if 'context' in globals() and key in ['use_real_price', 'avoid_future_data']:
        context._settings[key] = value


def set_slippage(slippage):
    """
    设置滑点

    Args:
        slippage: 滑点对象，如 FixedSlippage(0.02)
    """
    global context
    if 'context' in globals():
        context._settings['slippage'] = slippage


# ============================================================================
# 数据获取函数
# ============================================================================

def get_all_securities(types=None, date=None) -> pd.DataFrame:
    """
    获取所有股票列表

    Args:
        types: 证券类型，如 ['stock'] 或 'stock'
        date: 查询日期

    Returns:
        DataFrame，index为ts_code
    """
    if types is None:
        types = ['stock']

    if isinstance(types, str):
        types = [types]

    db = db_manager.get_connection()

    query = """
        SELECT ts_code, symbol, name, area, industry, fullname,
               market, exchange, list_status, list_date, delist_date, is_hs
        FROM stocks
        WHERE list_status = 'L'
          AND ts_code NOT LIKE '%.BJ'
          AND ts_code NOT LIKE '688%'
    """

    df = db.execute(query).fetchdf()

    if not df.empty:
        df.set_index('ts_code', inplace=True)

    return df


def get_current_data() -> Dict[str, CurrentData]:
    """
    获取当前时刻所有股票的数据

    Returns:
        Dict[stock_code, CurrentData]
    """
    global context

    if 'context' not in globals() or context.current_dt is None:
        return {}

    trade_date = context.current_dt.strftime('%Y-%m-%d')

    db = db_manager.get_connection()

    # 获取当日K线数据
    query = """
        SELECT k.ts_code, k.open, k.high, k.low, k.close, k.vol, k.amount,
               s.name, s.symbol
        FROM kline_daily k
        JOIN stocks s ON k.ts_code = s.ts_code
        WHERE k.trade_date = ?
          AND s.list_status = 'L'
    """

    df = db.execute(query, [trade_date]).fetchdf()

    result = {}

    for _, row in df.iterrows():
        ts_code = row['ts_code']

        # 计算涨跌停价格（简化版：基于前收盘价）
        # 实际应该从数据库获取pre_close
        pre_close = row['close'] * 0.98  # 简化估算

        # 判断是否为ST（简化：从名称判断）
        is_st = 'ST' in str(row.get('name', ''))

        # 涨跌停幅度
        limit_pct = 0.05 if is_st else 0.10

        high_limit = round(pre_close * (1 + limit_pct), 2)
        low_limit = round(pre_close * (1 - limit_pct), 2)

        data = CurrentData(
            day_open=row['open'],
            high=row['high'],
            low=row['low'],
            close=row['close'],
            last_price=row['close'],
            high_limit=high_limit,
            low_limit=low_limit,
            volume=row['vol'],
            money=row['amount'],
            paused=False,  # 简化处理
            is_st=is_st,
            name=row.get('name', '')
        )

        result[ts_code] = data

    # 返回带默认值的字典，访问不存在的股票时返回停牌的CurrentData
    class _DefaultCurrentData(dict):
        def __missing__(self, key):
            return CurrentData(paused=True, is_st=False, name='')

    return _DefaultCurrentData(result)


def get_price(security, start_date=None, end_date=None,
              frequency='daily', fields=None, count=None) -> pd.DataFrame:
    """
    获取历史价格数据

    Args:
        security: 股票代码或列表（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）
        start_date: 开始日期
        end_date: 结束日期
        frequency: 频率，如 'daily', 'minute'
        fields: 字段列表，如 ['open', 'close', 'high', 'low', 'volume']
        count: 获取最近N条数据（与start_date互斥）

    Returns:
        DataFrame
    """
    db = db_manager.get_connection()

    # 转换股票代码格式
    if isinstance(security, list):
        security = normalize_code_list(security)
    else:
        security = normalize_code(security)

    # 处理日期
    if count is not None and end_date is not None:
        # 根据count和end_date计算start_date
        # 简化：直接获取count条
        pass

    # 默认字段
    if fields is None:
        fields = ['open', 'close', 'high', 'low', 'volume']

    # 构建查询
    field_map = {
        'open': 'open',
        'close': 'close',
        'high': 'high',
        'low': 'low',
        'volume': 'vol',
        'money': 'amount',
        'pre_close': 'pre_close',
        'high_limit': 'high_limit',
        'low_limit': 'low_limit'
    }

    sql_fields = ['trade_date', 'ts_code']
    for f in fields:
        if f in field_map:
            sql_fields.append(field_map[f])

    # 处理多股票
    if isinstance(security, list):
        if not security:
            return pd.DataFrame()
        placeholders = ','.join(['?' for _ in security])
        stock_filter = f"ts_code IN ({placeholders})"
        params = security.copy()
    else:
        stock_filter = "ts_code = ?"
        params = [security]

    query = f"""
        SELECT {', '.join(sql_fields)}
        FROM kline_daily
        WHERE {stock_filter}
    """

    if start_date:
        query += " AND trade_date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND trade_date <= ?"
        params.append(end_date)

    query += " ORDER BY trade_date"

    if count:
        query += f" LIMIT {count}"

    df = db.execute(query, params).fetchdf()

    # 设置索引
    if not df.empty:
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df.set_index('trade_date', inplace=True)

    return df


class _JQSeries(pd.Series):
    """兼容聚宽的Series，支持负数位置索引（如 series[-1]）"""
    def __getitem__(self, key):
        if isinstance(key, int) and key < 0:
            return self.iloc[key]
        return super().__getitem__(key)


class _JQDataFrame(pd.DataFrame):
    """兼容聚宽的DataFrame，列访问返回_JQSeries"""
    def __getitem__(self, key):
        result = super().__getitem__(key)
        if isinstance(result, pd.Series):
            return _JQSeries(result)
        return result


def history(count, unit='1d', field='close', security_list=None,
            skip_paused=True, frequency='daily') -> pd.DataFrame:
    """
    获取历史数据

    Args:
        count: 获取条数
        unit: 时间单位，如 '1d', '1m'
        field: 字段，如 'close', 'open', 'high', 'low', 'volume', 'money'
        security_list: 股票代码列表（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）
        skip_paused: 是否跳过停牌
        frequency: 频率

    Returns:
        DataFrame，列为股票代码，行为日期
    """
    global context

    if 'context' not in globals() or context.current_dt is None:
        return pd.DataFrame()

    end_date = context.current_dt.strftime('%Y-%m-%d')

    if security_list is None:
        # 获取所有股票
        securities = get_all_securities()
        security_list = securities.index.tolist()[:50]  # 限制数量
    else:
        # 转换代码格式
        security_list = normalize_code_list(security_list)

    # 获取数据
    data = get_price(
        security=security_list,
        end_date=end_date,
        frequency=frequency,
        fields=[field],
        count=count
    )

    # 转换为宽格式
    if not data.empty and 'ts_code' in data.columns:
        result = data.pivot(columns='ts_code', values=field)
        # 确保所有请求的股票都有列（缺失的填NaN）
        for s in security_list:
            if s not in result.columns:
                result[s] = float('nan')
        return _JQDataFrame(result)

    # 返回空DataFrame但包含所有请求列
    return _JQDataFrame(pd.DataFrame(columns=security_list))


def attribute_history(security, count, unit='1d',
                     fields=['open', 'close', 'high', 'low', 'volume'],
                     skip_paused=True, df=True):
    """
    获取单只股票历史数据

    Args:
        security: 股票代码（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）
        count: 条数
        unit: 时间单位
        fields: 字段列表
        skip_paused: 跳过停牌
        df: 返回DataFrame

    Returns:
        DataFrame
    """
    # 转换代码格式
    security = normalize_code(security)

    return get_price(
        security=security,
        count=count,
        frequency='daily' if unit == '1d' else 'minute',
        fields=fields
    )


# ============================================================================
# 交易函数
# ============================================================================

def order(security, amount, style=None, side='long', pindex=0):
    """
    下单

    Args:
        security: 股票代码（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）
        amount: 数量（正买负卖）
        style: 下单风格
        side: 多空方向
        pindex: 子账户索引

    Returns:
        Order对象（简化）
    """
    global context

    if 'context' not in globals() or context._broker is None:
        log.error("未在回测环境中")
        return None

    # 转换代码格式
    security = normalize_code(security)

    try:
        # 通过backtrader下单
        # 注意：这里需要访问策略对象，由包装器提供
        if context._strategy:
            # 获取数据源
            data = context._get_data(security)
            if data is None:
                log.error(f"未找到 {security} 的数据源，请确保该股票已添加到回测")
                return None

            if amount > 0:
                order_obj = context._strategy.buy(data=data, size=amount)
                log.info(f"买入 {security} {amount}股")
            elif amount < 0:
                order_obj = context._strategy.sell(data=data, size=abs(amount))
                log.info(f"卖出 {security} {abs(amount)}股")
            else:
                return None
            return order_obj
    except Exception as e:
        log.error(f"下单失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def order_value(security, value, style=None, side='long', pindex=0):
    """
    按金额下单

    Args:
        security: 股票代码（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）
        value: 金额（正买负卖）
        style: 下单风格
        side: 多空方向
        pindex: 子账户索引
    """
    global context

    if 'context' not in globals():
        return None

    # 转换代码格式
    security = normalize_code(security)

    # 获取当前价格
    current_data = get_current_data()
    if security not in current_data:
        log.error(f"无法获取 {security} 价格")
        return None

    price = current_data[security].last_price
    if price <= 0:
        return None

    # 计算股数（100股整数）
    amount = int(value / price / 100) * 100
    if value < 0:
        amount = -amount

    return order(security, amount, style, side, pindex)


def order_target(security, amount, style=None, side='long', pindex=0):
    """
    目标持仓下单

    Args:
        security: 股票代码（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）
        amount: 目标持仓数量
        style: 下单风格
        side: 多空方向
        pindex: 子账户索引
    """
    global context

    if 'context' not in globals():
        return None

    # 转换代码格式
    security = normalize_code(security)

    # 获取当前持仓
    current_amount = 0
    if security in context.portfolio.positions:
        current_amount = context.portfolio.positions[security].total_amount

    # 计算需要调整的数量
    delta = amount - current_amount

    if delta != 0:
        return order(security, delta, style, side, pindex)
    return None


def order_target_value(security, value, style=None, side='long', pindex=0):
    """
    目标市值下单

    Args:
        security: 股票代码（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）
        value: 目标市值
        style: 下单风格
        side: 多空方向
        pindex: 子账户索引
    """
    global context

    if 'context' not in globals():
        return None

    # 转换代码格式
    security = normalize_code(security)

    # 获取当前价格
    current_data = get_current_data()
    if security not in current_data:
        return None

    price = current_data[security].last_price
    if price <= 0:
        return None

    # 计算目标股数
    target_amount = int(value / price / 100) * 100

    return order_target(security, target_amount, style, side, pindex)


# 删除旧的函数定义（下方重复的代码）
    """
    按金额下单

    Args:
        security: 股票代码
        value: 金额（正买负卖）
        style: 下单风格
        side: 多空方向
        pindex: 子账户索引
    """
    global context

    if 'context' not in globals():
        return None

    # 获取当前价格
    current_data = get_current_data()
    if security not in current_data:
        log.error(f"无法获取 {security} 价格")
        return None

    price = current_data[security].last_price
    if price <= 0:
        return None

    # 计算股数（100股整数）
    amount = int(value / price / 100) * 100
    if value < 0:
        amount = -amount

    return order(security, amount, style, side, pindex)


def order_target(security, amount, style=None, side='long', pindex=0):
    """
    目标持仓下单

    Args:
        security: 股票代码
        amount: 目标持仓数量
        style: 下单风格
        side: 多空方向
        pindex: 子账户索引
    """
    global context

    if 'context' not in globals():
        return None

    # 获取当前持仓
    current_amount = 0
    if security in context.portfolio.positions:
        current_amount = context.portfolio.positions[security].total_amount

    # 计算需要调整的数量
    delta = amount - current_amount

    if delta != 0:
        return order(security, delta, style, side, pindex)
    return None


def order_target_value(security, value, style=None, side='long', pindex=0):
    """
    目标市值下单

    Args:
        security: 股票代码
        value: 目标市值
        style: 下单风格
        side: 多空方向
        pindex: 子账户索引
    """
    global context

    if 'context' not in globals():
        return None

    # 获取当前价格
    current_data = get_current_data()
    if security not in current_data:
        return None

    price = current_data[security].last_price
    if price <= 0:
        return None

    # 计算目标股数
    target_amount = int(value / price / 100) * 100

    return order_target(security, target_amount, style, side, pindex)


def _eval_expr_on_df(expr, df):
    """在DataFrame上对QueryField/QueryExpr求值"""
    if isinstance(expr, QueryField):
        return df[expr.name] if expr.name in df.columns else None
    elif isinstance(expr, QueryExpr):
        left = _eval_expr_on_df(expr.left, df)
        right = _eval_expr_on_df(expr.right, df)
        if left is None or right is None:
            return None
        if expr.op == '/':
            return left / right
        elif expr.op == '*':
            return left * right
        elif expr.op == '+':
            return left + right
        elif expr.op == '-':
            return left - right
    elif isinstance(expr, (int, float)):
        return expr
    return None


def _apply_filter_conditions_on_df(df, filter_conditions):
    """在DataFrame上应用过滤条件列表（每项为(left, op, right)元组或QueryCondition）"""
    mask = pd.Series([True] * len(df), index=df.index)
    for cond in filter_conditions:
        if isinstance(cond, tuple):
            col_name, op, val = cond
            if col_name not in df.columns:
                continue
            series = df[col_name]
        else:
            # QueryCondition对象
            series = _eval_expr_on_df(cond.left, df)
            if series is None:
                continue
            op = cond.op
            val = cond.right

        try:
            if op == 'gt':
                mask &= series > val
            elif op == 'ge':
                mask &= series >= val
            elif op == 'lt':
                mask &= series < val
            elif op == 'le':
                mask &= series <= val
            elif op == 'eq':
                mask &= series == val
            elif op == 'between':
                mask &= (series >= val[0]) & (series <= val[1])
            elif op == 'in':
                mask &= series.isin(val)
        except Exception:
            pass
    return df[mask].reset_index(drop=True)


def get_fundamentals(query_obj, date=None, statDate=None):
    """
    查询财务数据

    Args:
        query_obj: Query对象或filter条件
        date: 查询日期
        statDate: 统计日期（如 '2025q3'）

    Returns:
        DataFrame
    """
    db = db_manager.get_connection()

    # 早期返回：空stocks列表
    if hasattr(query_obj, '_filter'):
        for f in query_obj._filter:
            if hasattr(f, 'left') and hasattr(f, 'right') and hasattr(f, 'op'):
                left = f.left
                if hasattr(left, 'name') and left.name == 'code' and f.op == 'in':
                    right = f.right
                    codes_check = right.value if hasattr(right, 'value') else right
                    if isinstance(codes_check, list) and len(codes_check) == 0:
                        return pd.DataFrame()

    # 从query_obj提取信息
    codes = None
    entities = []
    filter_conditions = []  # 存储QueryCondition对象
    simple_filter_conditions = []  # 存储(col, op, val)简单元组

    if hasattr(query_obj, '_entities'):
        entities = query_obj._entities

    if hasattr(query_obj, '_filter'):
        for f in query_obj._filter:
            if not hasattr(f, 'left') or not hasattr(f, 'right'):
                continue
            left = f.left
            # code.in_() 条件
            if hasattr(left, 'name') and left.name == 'code' and f.op == 'in':
                right = f.right
                if hasattr(right, 'value'):
                    codes = right.value
                elif isinstance(right, list):
                    codes = right
            # 简单字段条件（非表达式）
            elif isinstance(left, QueryField):
                simple_filter_conditions.append((left.name, f.op, f.right))
            # 表达式条件（如 pe_ratio / inc_net_profit_year_on_year > 0.08）
            elif isinstance(left, QueryExpr):
                filter_conditions.append(f)

    # 转换代码格式
    if codes:
        if isinstance(codes, str):
            codes = [codes]
        codes = normalize_code_list(codes)

    # 判断查询类型：看是否包含 indicator 实体
    need_valuation = False
    need_indicator = False
    for entity in entities:
        if isinstance(entity, IndicatorTable):
            need_indicator = True
        elif isinstance(entity, ValuationTable):
            need_valuation = True
        elif hasattr(entity, 'table'):
            if entity.table == 'indicator':
                need_indicator = True
            elif entity.table == 'valuation':
                need_valuation = True

    # 将简单过滤条件分给对应的表
    val_filter = [(col, op, val) for col, op, val in simple_filter_conditions
                  if col in ('pe_ratio', 'pb_ratio', 'ps_ratio', 'market_cap', 'circulating_market_cap')]
    ind_filter = [(col, op, val) for col, op, val in simple_filter_conditions
                  if col not in ('pe_ratio', 'pb_ratio', 'ps_ratio', 'market_cap',
                                 'circulating_market_cap', 'code')]

    # 获取两个表的数据并合并
    if need_indicator and need_valuation:
        # 联合查询：先获取valuation，再merge indicator
        val_df = _get_valuation_fundamentals(db, codes, date)
        if val_df.empty:
            return val_df
        ind_df = _get_indicator_fundamentals(db, list(val_df['code']), date, [])
        if not ind_df.empty:
            df = val_df.merge(
                ind_df[['code'] + [c for c in ind_df.columns if c not in val_df.columns]],
                on='code', how='left'
            )
        else:
            df = val_df
    elif need_indicator:
        df = _get_indicator_fundamentals(db, codes, date, [])
    else:
        df = _get_valuation_fundamentals(db, codes, date)

    if df.empty:
        return df

    # 应用所有简单过滤条件
    all_simple = val_filter + ind_filter
    if all_simple:
        df = _apply_filter_conditions_on_df(df, all_simple)

    # 应用表达式过滤条件
    if filter_conditions and not df.empty:
        df = _apply_filter_conditions_on_df(df, filter_conditions)

    return df


def _get_valuation_fundamentals(db, codes, date):
    """获取估值数据"""
    if codes:
        placeholders = ','.join(['?' for _ in codes])
        where_clause = f"WHERE ts_code IN ({placeholders})"
        params = codes.copy()
    else:
        where_clause = "WHERE 1=1"
        params = []

    if date:
        where_clause += " AND trade_date <= ?"
        params.append(date)

    # 获取最新数据，按ts_code分组取最新
    query = f"""
        SELECT DISTINCT ON (ts_code)
            ts_code as code,
            pe as pe_ratio,
            pb as pb_ratio,
            ps as ps_ratio,
            total_mv as market_cap,
            circ_mv as circulating_market_cap
        FROM stock_daily_basic
        {where_clause}
        ORDER BY ts_code, trade_date DESC
    """

    try:
        df = db.execute(query, params).fetchdf()
        return df
    except Exception as e:
        log.error(f"get_fundamentals valuation查询失败: {e}")
        return pd.DataFrame()


def _get_indicator_fundamentals(db, codes, date, filter_conditions):
    """
    获取财务指标数据，包含同比增长率计算

    注意：由于Tushare的fina_indicator表没有直接的同比增长率字段，
    这里需要通过计算得到
    """
    if codes:
        placeholders = ','.join(['?' for _ in codes])
        where_clause = f"WHERE ts_code IN ({placeholders})"
        params = codes.copy()
    else:
        where_clause = "WHERE 1=1"
        params = []

    if date:
        # 转换为日期对象
        if isinstance(date, str):
            query_date = datetime.strptime(date, '%Y-%m-%d').date()
        else:
            query_date = date
        # 允许最多往前查6个月的财务数据（在回测中财务数据通常有延迟披露）
        # 同时向后宽松180天：财务数据按季度披露，允许使用最近已公布的季报
        # 这样在2025-01-02查询时，可以找到2025-03-31的季报数据（已同步到数据库）
        date_upper = (query_date + timedelta(days=180)).strftime('%Y-%m-%d')
        where_clause += " AND end_date <= ?"
        params.append(date_upper)
    else:
        date_upper = None

    try:
        # 获取最新一期的财务数据
        query_current = f"""
            SELECT
                ts_code as code,
                end_date,
                roe,
                roe_yearly,
                bps,
                eps,
                dt_eps,
                netprofit_margin,
                grossprofit_margin,
                profit_dedt,
                gross_margin,
                op_income
            FROM stock_fina_indicator
            {where_clause}
            ORDER BY ts_code, end_date DESC
        """

        df_current = db.execute(query_current, params).fetchdf()

        if df_current.empty:
            return pd.DataFrame()

        # 去重：每个股票只保留最新一期数据
        df_current = df_current.drop_duplicates(subset=['code'], keep='first')

        # 获取去年同期数据用于计算同比增长率
        # 计算一年前的日期范围
        if date:
            if isinstance(date, str):
                query_date = datetime.strptime(date, '%Y-%m-%d').date()
            else:
                query_date = date
            year_ago_start = (query_date - timedelta(days=400)).strftime('%Y-%m-%d')
            year_ago_end = (query_date - timedelta(days=300) + timedelta(days=180)).strftime('%Y-%m-%d')
        else:
            year_ago_start = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
            year_ago_end = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')

        # 获取去年同期数据
        query_last_year = f"""
            SELECT
                ts_code as code,
                end_date,
                profit_dedt,
                gross_margin
            FROM stock_fina_indicator
            WHERE ts_code IN ({placeholders if codes else "SELECT ts_code FROM stock_fina_indicator"})
            AND end_date BETWEEN ? AND ?
            ORDER BY ts_code, end_date DESC
        """

        if codes:
            params_last_year = codes + [year_ago_start, year_ago_end]
        else:
            params_last_year = [year_ago_start, year_ago_end]

        df_last_year = db.execute(query_last_year, params_last_year).fetchdf()

        # 计算同比增长率
        result = df_current.copy()

        # 对每个股票计算同比增长率
        if not df_last_year.empty:
            # 按code分组，取每组第一条（最新）
            df_last_year_grouped = df_last_year.groupby('code').first().reset_index()

            # 合并数据
            result = result.merge(
                df_last_year_grouped[['code', 'profit_dedt', 'gross_margin']],
                on='code',
                how='left',
                suffixes=('', '_last_year')
            )

            # 计算净利润同比增长率
            result['inc_net_profit_year_on_year'] = np.where(
                (result['profit_dedt_last_year'] != 0) & (~result['profit_dedt_last_year'].isna()),
                ((result['profit_dedt'] - result['profit_dedt_last_year']) / abs(result['profit_dedt_last_year'])) * 100,
                np.nan
            )

            # 计算营业总收入同比增长率（使用gross_margin作为近似）
            result['inc_total_revenue_year_on_year'] = np.where(
                (result['gross_margin_last_year'] != 0) & (~result['gross_margin_last_year'].isna()),
                ((result['gross_margin'] - result['gross_margin_last_year']) / abs(result['gross_margin_last_year'])) * 100,
                np.nan
            )
        else:
            # 没有去年同期数据，设为NaN
            result['inc_net_profit_year_on_year'] = np.nan
            result['inc_total_revenue_year_on_year'] = np.nan

        # 映射字段名
        result = result.rename(columns={
            'roe': 'inc_return',
            'roe_yearly': 'roe_yearly'
        })

        # 确保所有需要的字段都存在
        required_fields = ['code', 'inc_return', 'inc_total_revenue_year_on_year',
                          'inc_net_profit_year_on_year']
        for field in required_fields:
            if field not in result.columns:
                result[field] = np.nan

        return result

    except Exception as e:
        log.error(f"get_fundamentals indicator查询失败: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


# 缓存股票信息，避免重复查询导致DuckDB并发问题
_security_info_cache = {}


def _load_security_info_cache():
    """预加载所有股票信息到缓存"""
    global _security_info_cache
    if _security_info_cache:
        return
    try:
        db = db_manager.get_connection()
        rows = db.execute(
            "SELECT ts_code, name, symbol, list_date, industry, market FROM stocks"
        ).fetchall()
        from datetime import date as _date
        from types import SimpleNamespace
        for row in rows:
            ts_code = row[0]
            start_date = _date(2000, 1, 1)
            if row[3]:
                try:
                    if isinstance(row[3], datetime):
                        start_date = row[3].date() if hasattr(row[3], 'date') else row[3]
                    elif isinstance(row[3], str):
                        start_date = datetime.strptime(row[3], '%Y%m%d').date()
                    else:
                        start_date = row[3]
                except:
                    pass
            _security_info_cache[ts_code] = SimpleNamespace(
                code=ts_code,
                display_name=row[1],
                name=row[1],
                symbol=row[2],
                start_date=start_date,
                industry=row[4],
                market=row[5]
            )
    except Exception as e:
        log.error(f"预加载股票信息失败: {e}")


def get_security_info(code):
    """
    获取证券信息

    Args:
        code: 股票代码（支持聚宽格式 .XSHG/.XSHE 或 Tushare格式 .SH/.SZ）

    Returns:
        SimpleNamespace对象，包含display_name, start_date等属性
    """
    from datetime import date as _date
    from types import SimpleNamespace

    # 转换代码格式
    ts_code = normalize_code(code)

    # 从缓存中查找（避免DuckDB并发问题）
    _load_security_info_cache()
    if ts_code in _security_info_cache:
        return _security_info_cache[ts_code]

    # 返回默认值
    return SimpleNamespace(
        code=ts_code,
        display_name=ts_code,
        name=ts_code,
        start_date=_date(2000, 1, 1),
        industry='',
        market=''
    )


def run_query(query_obj):
    """
    执行财务数据查询

    Args:
        query_obj: Query对象

    Returns:
        DataFrame
    """
    db = db_manager.get_connection()

    # 解析Query对象
    table = 'stock_dividend'
    field_columns = []  # QueryField对象列表
    codes = None
    date_start = None
    date_end = None

    # a_registration_date = 股权登记日 = record_date (聚宽字段名 -> Tushare字段名)
    col_map = {
        'code': 'ts_code',
        'a_registration_date': 'record_date',
        'bonus_amount_rmb': 'cash_div'
    }

    # 提取查询字段
    if hasattr(query_obj, '_entities'):
        for entity in query_obj._entities:
            if hasattr(entity, 'key'):
                # 是单个QueryField对象（如finance.STK_XR_XD.code）
                field_columns.append(entity)

    # 解析filter
    if hasattr(query_obj, '_filter'):
        for f in query_obj._filter:
            # 解析code.in_条件
            if hasattr(f, 'left') and hasattr(f.left, 'key'):
                if f.left.key == 'code' and hasattr(f, 'right'):
                    codes = f.right
                    # 转换代码格式
                    if isinstance(codes, list):
                        codes = normalize_code_list(codes)
                    elif isinstance(codes, str):
                        codes = normalize_code(codes)
            # 解析日期范围
            if hasattr(f, 'left') and hasattr(f.left, 'key'):
                if 'date' in f.left.key.lower():
                    if hasattr(f, 'right') and hasattr(f, 'op'):
                        if 'ge' in str(f.op):
                            date_start = f.right
                        elif 'le' in str(f.op):
                            date_end = f.right

    # 构建SELECT列
    # record_date (登记日) 大量为NULL，使用 COALESCE(record_date, ann_date) 作为有效日期
    col_map_sql = {
        'code': 'ts_code',
        'a_registration_date': 'COALESCE(record_date, ann_date)',
        'bonus_amount_rmb': 'cash_div'
    }

    if field_columns:
        # 使用指定的QueryField字段
        sql_cols = []
        for field in field_columns:
            db_col = col_map_sql.get(field.key, col_map.get(field.key, field.key))
            sql_cols.append(f"{db_col} as {field.key}")
    else:
        # 默认列（包含所有有用字段）
        sql_cols = ['ts_code as code', 'COALESCE(record_date, ann_date) as a_registration_date', 'cash_div as bonus_amount_rmb']

    query_sql = f"SELECT {', '.join(sql_cols)} FROM stock_dividend WHERE 1=1"
    # 只查有分红金额的记录
    query_sql += " AND cash_div > 0"
    params = []

    if codes:
        if len(codes) == 0:
            return pd.DataFrame()
        placeholders = ','.join(['?' for _ in codes])
        query_sql += f" AND ts_code IN ({placeholders})"
        params.extend(codes)

    if date_start:
        query_sql += " AND COALESCE(record_date, ann_date) >= ?"
        params.append(date_start)

    if date_end:
        query_sql += " AND COALESCE(record_date, ann_date) <= ?"
        params.append(date_end)

    try:
        df = db.execute(query_sql, params).fetchdf()
        # 将日期列转为字符串（避免 groupby.sum() 报 datetime64 错误）
        for col in df.columns:
            if 'date' in col.lower() and hasattr(df[col], 'dt'):
                df[col] = df[col].dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        log.error(f"run_query执行失败: {e}")
        return pd.DataFrame()


# ============================================================================
# 记录函数
# ============================================================================

def record(**kwargs):
    """
    记录指标（用于绘图）

    Args:
        **kwargs: 指标名称和值
    """
    global context

    if 'context' not in globals():
        return

    # 存储到context的record中
    if not hasattr(context, '_records'):
        context._records = []

    record_data = {
        'datetime': context.current_dt,
        **kwargs
    }
    context._records.append(record_data)


def get_records() -> List[Dict]:
    """获取所有记录"""
    global context

    if 'context' not in globals() or not hasattr(context, '_records'):
        return []

    return context._records


# ============================================================================
# 工具函数
# ============================================================================

def calculate_limit_price(close_price: float, is_st: bool = False,
                         is_kcb: bool = False, is_cyb: bool = False) -> tuple:
    """
    计算涨跌停价格

    Args:
        close_price: 前收盘价
        is_st: 是否ST
        is_kcb: 是否科创板
        is_cyb: 是否创业板

    Returns:
        (跌停价, 涨停价)
    """
    if is_kcb or is_cyb:
        limit_pct = 0.20
    elif is_st:
        limit_pct = 0.05
    else:
        limit_pct = 0.10

    # A股价格档位为0.01元
    high_limit = round(close_price * (1 + limit_pct), 2)
    low_limit = round(close_price * (1 - limit_pct), 2)

    return low_limit, high_limit


# ============================================================================
# Query构建器 - 模拟聚宽query语法
# ============================================================================

class QueryExpr:
    """查询表达式（字段间的算术运算结果）"""
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right
        # 用于过滤条件识别
        self.name = f"({_expr_name(left)} {op} {_expr_name(right)})"
        self.key = self.name
        self.table = None

    def __ge__(self, other):
        return QueryCondition(self, 'ge', other)

    def __le__(self, other):
        return QueryCondition(self, 'le', other)

    def __gt__(self, other):
        return QueryCondition(self, 'gt', other)

    def __lt__(self, other):
        return QueryCondition(self, 'lt', other)

    def __eq__(self, other):
        return QueryCondition(self, 'eq', other)

    def between(self, low, high):
        return QueryCondition(self, 'between', (low, high))


def _expr_name(e):
    if isinstance(e, QueryField):
        return e.name
    elif isinstance(e, QueryExpr):
        return e.name
    return str(e)


class QueryField:
    """查询字段"""
    def __init__(self, name, table=None):
        self.key = name
        self.name = name
        self.table = table

    def __eq__(self, other):
        return QueryCondition(self, 'eq', other)

    def __ge__(self, other):
        return QueryCondition(self, 'ge', other)

    def __le__(self, other):
        return QueryCondition(self, 'le', other)

    def __gt__(self, other):
        return QueryCondition(self, 'gt', other)

    def __lt__(self, other):
        return QueryCondition(self, 'lt', other)

    def __truediv__(self, other):
        return QueryExpr(self, '/', other)

    def __mul__(self, other):
        return QueryExpr(self, '*', other)

    def __add__(self, other):
        return QueryExpr(self, '+', other)

    def __sub__(self, other):
        return QueryExpr(self, '-', other)

    def __rtruediv__(self, other):
        return QueryExpr(other, '/', self)

    def __rmul__(self, other):
        return QueryExpr(other, '*', self)

    def __radd__(self, other):
        return QueryExpr(other, '+', self)

    def __rsub__(self, other):
        return QueryExpr(other, '-', self)

    def between(self, low, high):
        return QueryCondition(self, 'between', (low, high))

    def in_(self, values):
        return QueryCondition(self, 'in', values)


class QueryCondition:
    """查询条件"""
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right


class Query:
    """聚宽query对象"""
    def __init__(self, *entities):
        self._entities = entities
        self._filter = []
        self._order_by = None
        self._limit = None

    def filter(self, *conditions):
        """添加过滤条件"""
        self._filter.extend(conditions)
        return self

    def order_by(self, field):
        """排序"""
        self._order_by = field
        return self

    def limit(self, n):
        """限制数量"""
        self._limit = n
        return self


def query(*entities):
    """创建查询"""
    return Query(*entities)


# ============================================================================
# 财务数据表对象
# ============================================================================

class ValuationTable:
    """valuation表 - 估值数据"""
    def __init__(self):
        self.code = QueryField('code', 'valuation')
        self.market_cap = QueryField('market_cap', 'valuation')
        self.circulating_market_cap = QueryField('circulating_market_cap', 'valuation')
        self.pe_ratio = QueryField('pe_ratio', 'valuation')
        self.pb_ratio = QueryField('pb_ratio', 'valuation')
        self.ps_ratio = QueryField('ps_ratio', 'valuation')
        self.pcf_ratio = QueryField('pcf_ratio', 'valuation')
        self.pe_ratio_lyr = QueryField('pe_ratio_lyr', 'valuation')


class IndicatorTable:
    """indicator表 - 财务指标"""
    def __init__(self):
        self.code = QueryField('code', 'indicator')
        self.inc_return = QueryField('inc_return', 'indicator')  # ROE
        self.inc_total_revenue_year_on_year = QueryField('inc_total_revenue_year_on_year', 'indicator')
        self.inc_net_profit_year_on_year = QueryField('inc_net_profit_year_on_year', 'indicator')
        self.inc_net_profit_to_shareholders_year_on_year = QueryField('inc_net_profit_to_shareholders_year_on_year', 'indicator')


# 全局表实例
valuation = ValuationTable()
indicator = IndicatorTable()


# ============================================================================
# finance模块 - 分红送股数据
# ============================================================================

class FinanceSTK_XR_XD:
    """分红送股表"""
    def __init__(self):
        self.code = QueryField('code', 'STK_XR_XD')
        self.a_registration_date = QueryField('a_registration_date', 'STK_XR_XD')
        self.bonus_amount_rmb = QueryField('bonus_amount_rmb', 'STK_XR_XD')
        self.bonus_type = QueryField('bonus_type', 'STK_XR_XD')


class FinanceModule:
    """finance模块，包含分红送股数据"""
    def __init__(self):
        self.STK_XR_XD = FinanceSTK_XR_XD()

    def run_query(self, query_obj):
        """执行查询"""
        return run_query(query_obj)


finance = FinanceModule()


# ============================================================================
# 全局Context实例
# ============================================================================

# 由策略转换器在运行时创建
context = None

# 从jqdata导入的标记
jqdata_imported = True
