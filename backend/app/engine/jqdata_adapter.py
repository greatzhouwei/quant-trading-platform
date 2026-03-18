"""
聚宽(JoinQuant) API 兼容层

提供与聚宽API兼容的函数和类，让用户可以直接粘贴聚宽策略代码运行。
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field

from app.db.session import db_manager


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

    return result


def get_price(security, start_date=None, end_date=None,
              frequency='daily', fields=None, count=None) -> pd.DataFrame:
    """
    获取历史价格数据

    Args:
        security: 股票代码或列表
        start_date: 开始日期
        end_date: 结束日期
        frequency: 频率，如 'daily', 'minute'
        fields: 字段列表，如 ['open', 'close', 'high', 'low', 'volume']
        count: 获取最近N条数据（与start_date互斥）

    Returns:
        DataFrame
    """
    db = db_manager.get_connection()

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
        'pre_close': 'pre_close'
    }

    sql_fields = ['trade_date', 'ts_code']
    for f in fields:
        if f in field_map:
            sql_fields.append(field_map[f])

    # 处理多股票
    if isinstance(security, list):
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


def history(count, unit='1d', field='close', security_list=None,
            skip_paused=True, frequency='daily') -> pd.DataFrame:
    """
    获取历史数据

    Args:
        count: 获取条数
        unit: 时间单位，如 '1d', '1m'
        field: 字段，如 'close', 'open', 'high', 'low', 'volume', 'money'
        security_list: 股票代码列表
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
        return result

    return data


def attribute_history(security, count, unit='1d',
                     fields=['open', 'close', 'high', 'low', 'volume'],
                     skip_paused=True, df=True):
    """
    获取单只股票历史数据

    Args:
        security: 股票代码
        count: 条数
        unit: 时间单位
        fields: 字段列表
        skip_paused: 跳过停牌
        df: 返回DataFrame

    Returns:
        DataFrame
    """
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
        security: 股票代码
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

    # 从query_obj提取信息
    codes = None
    filters = {}

    if hasattr(query_obj, '_entities'):
        # 提取查询的实体（valuation, indicator等）
        entities = query_obj._entities
    else:
        entities = ['valuation', 'indicator']

    if hasattr(query_obj, '_filter'):
        # 简化处理：解析filter中的条件
        for f in query_obj._filter:
            # 尝试提取code.in_条件
            if hasattr(f, 'left') and hasattr(f, 'right'):
                if hasattr(f.left, 'name') and f.left.name == 'code':
                    if hasattr(f.right, 'value'):
                        codes = f.right.value

    # 构建查询
    # 从stock_daily_basic获取估值数据
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

    # 获取最新数据
    query = f"""
        SELECT
            ts_code as code,
            pe as pe_ratio,
            pb as pb_ratio,
            ps as ps_ratio,
            total_mv as market_cap,
            circ_mv as circulating_market_cap
        FROM stock_daily_basic
        {where_clause}
        ORDER BY trade_date DESC
    """

    try:
        df = db.execute(query, params).fetchdf()
        return df
    except Exception as e:
        log.error(f"get_fundamentals查询失败: {e}")
        return pd.DataFrame()


def get_security_info(code):
    """
    获取证券信息

    Args:
        code: 股票代码

    Returns:
        SimpleNamespace对象，包含display_name, start_date等属性
    """
    db = db_manager.get_connection()

    try:
        row = db.execute(
            """SELECT ts_code, name, symbol, list_date, industry, market
               FROM stocks WHERE ts_code = ?""",
            [code]
        ).fetchone()

        if row:
            from types import SimpleNamespace
            # 解析list_date
            start_date = None
            if row[3]:
                try:
                    start_date = datetime.strptime(str(row[3]), '%Y%m%d').date()
                except:
                    pass

            return SimpleNamespace(
                code=row[0],
                display_name=row[1],
                name=row[1],
                symbol=row[2],
                start_date=start_date,
                industry=row[4],
                market=row[5]
            )
    except Exception as e:
        log.error(f"get_security_info失败: {e}")

    # 返回默认值
    return SimpleNamespace(
        code=code,
        display_name=code,
        name=code,
        start_date=None,
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
    table = None
    columns = []
    codes = None
    date_start = None
    date_end = None

    # 提取表和列
    if hasattr(query_obj, '_entities'):
        for entity in query_obj._entities:
            if hasattr(entity, 'code'):
                table = 'stock_dividend'
                columns = ['ts_code', 'div_date', 'cash_div']

    # 解析filter
    if hasattr(query_obj, '_filter'):
        for f in query_obj._filter:
            # 解析code.in_条件
            if hasattr(f, 'left') and hasattr(f.left, 'key'):
                if f.left.key == 'code' and hasattr(f, 'right'):
                    codes = f.right
            # 解析日期范围
            if hasattr(f, 'left') and hasattr(f.left, 'key'):
                if 'date' in f.left.key.lower():
                    if hasattr(f, 'right') and hasattr(f, 'op'):
                        if 'ge' in str(f.op):
                            date_start = f.right
                        elif 'le' in str(f.op):
                            date_end = f.right

    # 如果没有解析到，使用默认查询
    if not columns:
        columns = ['ts_code', 'div_date', 'cash_div']

    # 构建查询
    col_map = {
        'code': 'ts_code',
        'a_registration_date': 'base_date',
        'bonus_amount_rmb': 'cash_div'
    }

    sql_cols = []
    for col in columns:
        if hasattr(col, 'key'):
            sql_col = col_map.get(col.key, col.key)
            sql_cols.append(f"{sql_col} as {col.key}")
        else:
            sql_cols.append(str(col))

    if not sql_cols:
        sql_cols = ['ts_code as code', 'base_date as a_registration_date', 'cash_div as bonus_amount_rmb']

    query_sql = f"SELECT {', '.join(sql_cols)} FROM stock_dividend WHERE 1=1"
    params = []

    if codes:
        placeholders = ','.join(['?' for _ in codes])
        query_sql += f" AND ts_code IN ({placeholders})"
        params.extend(codes)

    if date_start:
        query_sql += " AND base_date >= ?"
        params.append(date_start)

    if date_end:
        query_sql += " AND base_date <= ?"
        params.append(date_end)

    try:
        df = db.execute(query_sql, params).fetchdf()
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
