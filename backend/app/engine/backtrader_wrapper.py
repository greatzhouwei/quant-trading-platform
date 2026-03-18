"""
backtrader回测引擎封装
"""
import backtrader as bt
import pandas as pd
from typing import Type, Dict, Any, Optional, List
from datetime import datetime

from app.db.session import db_manager
from app.engine.jqdata_strategy_converter import JQStrategyConverter


class TradeRecorder(bt.Analyzer):
    """自定义交易记录分析器"""

    def __init__(self):
        super().__init__()
        self.trades = []

    def notify_order(self, order):
        """记录订单成交"""
        if order.status in [order.Completed]:
            trade_type = '买入' if order.isbuy() else '卖出'
            self.trades.append({
                'datetime': self.data.datetime.datetime().isoformat(),
                'type': trade_type,
                'price': order.executed.price,
                'size': order.executed.size,
                'value': order.executed.value,
                'commission': order.executed.comm,
                'pnl': None,  # 单笔订单没有pnl，在notify_trade中更新
            })

    def notify_trade(self, trade):
        if trade.isclosed:
            # 更新最后一笔交易的pnl
            if self.trades:
                self.trades[-1]['pnl'] = trade.pnl

    def get_analysis(self):
        return {'trades': self.trades}


class BacktraderEngine:
    """backtrader回测引擎封装"""

    def __init__(self):
        self.cerebro = None
        self.results = None

    def create_cerebro(self, config: Dict[str, Any]) -> bt.Cerebro:
        """创建并配置Cerebro引擎"""
        cerebro = bt.Cerebro()

        # 设置初始资金
        initial_cash = config.get('initial_cash', 100000.0)
        cerebro.broker.setcash(initial_cash)

        # 设置手续费
        commission = config.get('commission', 0.00025)
        cerebro.broker.setcommission(commission=commission)

        # 设置滑点
        slippage = config.get('slippage', 0.001)
        cerebro.broker.set_slippage_perc(slippage)

        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn', timeframe=bt.TimeFrame.Days)
        cerebro.addanalyzer(TradeRecorder, _name='trade_recorder')

        return cerebro

    def load_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        timeframe: str = '1d'
    ) -> bt.feeds.PandasData:
        """从DuckDB加载数据"""

        # 查询数据
        db = db_manager.get_connection()
        query = """
            SELECT trade_date as datetime, open, high, low, close, vol as volume
            FROM kline_daily
            WHERE ts_code = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date
        """
        df = db.execute(query, [symbol, start_date, end_date]).fetchdf()

        if df.empty:
            raise ValueError(f"未找到 {symbol} 在 {start_date} 至 {end_date} 期间的数据")

        # 设置索引
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)

        # 映射timeframe
        timeframe_map = {
            '1d': bt.TimeFrame.Days,
            '1w': bt.TimeFrame.Weeks,
            '1m': bt.TimeFrame.Months,
            '5m': bt.TimeFrame.Minutes,
            '15m': bt.TimeFrame.Minutes,
            '30m': bt.TimeFrame.Minutes,
            '60m': bt.TimeFrame.Minutes,
        }
        compression_map = {
            '1d': 1, '1w': 1, '1m': 1,
            '5m': 5, '15m': 15, '30m': 30, '60m': 60
        }

        data = bt.feeds.PandasData(
            dataname=df,
            timeframe=timeframe_map.get(timeframe, bt.TimeFrame.Days),
            compression=compression_map.get(timeframe, 1)
        )

        # 设置数据源名称，用于策略中的代码识别
        data._name = symbol

        return data

    def load_strategy(self, code: str, strategy_type: str = 'auto') -> Type[bt.Strategy]:
        """
        从代码字符串加载策略类

        Args:
            code: 策略代码字符串
            strategy_type: 策略类型 ('auto', 'backtrader', 'jqdata')
                - 'auto': 自动检测
                - 'backtrader': 原生backtrader策略
                - 'jqdata': 聚宽策略

        Returns:
            bt.Strategy的子类
        """
        import ast
        import types
        import sys

        # 解析代码
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            raise ValueError(f"策略代码语法错误: {e}")

        # 自动检测策略类型
        if strategy_type == 'auto':
            if JQStrategyConverter.is_jq_strategy(code):
                print("检测到聚宽策略，正在转换...")
                strategy_type = 'jqdata'
            else:
                strategy_type = 'backtrader'

        # 如果是聚宽策略，先转换
        if strategy_type == 'jqdata':
            code = JQStrategyConverter.convert_to_backtrader(code)

        # 查找策略类
        strategy_class = None
        tree = ast.parse(code)  # 重新解析（可能已转换）
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # 检查是否继承自bt.Strategy
                for base in node.bases:
                    if isinstance(base, ast.Attribute) and base.attr == 'Strategy':
                        strategy_class = node.name
                        break
                    elif isinstance(base, ast.Name) and base.id == 'Strategy':
                        strategy_class = node.name
                        break

        if not strategy_class:
            raise ValueError("策略代码中未找到继承自bt.Strategy的类")

        # 创建模块并注册到sys.modules（backtrader需要）
        module_name = 'dynamic_strategy'
        module = types.ModuleType(module_name)
        module.__dict__['bt'] = bt
        module.__dict__['backtrader'] = bt
        sys.modules[module_name] = module

        # 执行代码
        exec(code, module.__dict__)

        # 获取策略类
        strategy_cls = getattr(module, strategy_class)

        return strategy_cls

    def run_backtest(
        self,
        strategy_class: Type[bt.Strategy],
        data: bt.feeds.PandasData,
        config: Dict[str, Any],
        extra_data: Optional[List[bt.feeds.PandasData]] = None
    ) -> Dict[str, Any]:
        """执行回测

        Args:
            strategy_class: 策略类
            data: 主数据源（基准）
            config: 回测配置
            extra_data: 额外的数据源列表（用于聚宽策略交易多只股票）
        """

        # 保存基准数据的价格历史（用于绘制基准曲线）
        self._benchmark_data = self._extract_price_history(data)
        print(f"[DEBUG] 基准数据点数量: {len(self._benchmark_data)}")

        self.cerebro = self.create_cerebro(config)
        self.cerebro.adddata(data)

        # 添加额外数据源（如果有）
        if extra_data:
            for d in extra_data:
                self.cerebro.adddata(d)

        # 添加策略（带参数）
        parameters = config.get('parameters', {})
        self.cerebro.addstrategy(strategy_class, **parameters)

        # 运行回测
        self.results = self.cerebro.run()

        # 提取结果
        return self._extract_results()

    def _extract_price_history(self, data: bt.feeds.PandasData) -> List[Dict[str, Any]]:
        """提取数据源的价格历史"""
        price_history = []
        # 获取原始DataFrame
        df = data._dataname
        if df is not None and not df.empty:
            for idx, row in df.iterrows():
                price_history.append({
                    'date': idx.isoformat() if hasattr(idx, 'isoformat') else str(idx),
                    'close': float(row['close'])
                })
        return price_history

    def _extract_results(self) -> Dict[str, Any]:
        """提取回测结果"""
        if not self.results:
            return {}

        strat = self.results[0]

        # 获取分析器结果
        sharpe = strat.analyzers.sharpe.get_analysis()
        drawdown = strat.analyzers.drawdown.get_analysis()
        trades = strat.analyzers.trades.get_analysis()
        returns = strat.analyzers.returns.get_analysis()
        timereturn = strat.analyzers.timereturn.get_analysis()
        trade_recorder = strat.analyzers.trade_recorder.get_analysis()

        # 构建权益曲线
        equity_curve = []
        for date, value in timereturn.items():
            equity_curve.append({
                'date': date.isoformat() if hasattr(date, 'isoformat') else str(date),
                'return': value
            })

        # 计算累计收益
        cumulative_returns = []
        cumulative = 1.0
        for point in equity_curve:
            cumulative *= (1 + point['return'])
            cumulative_returns.append({
                'date': point['date'],
                'value': cumulative
            })

        # 提取交易记录
        # 优先从策略的get_trades方法获取（JQStrategyWrapper）
        trades_list = []
        if hasattr(strat, 'get_trades'):
            trades_list = strat.get_trades()
        # 如果没有，则从TradeRecorder分析器获取
        if not trades_list:
            trades_list = trade_recorder.get('trades', [])

        # 构建回撤曲线
        drawdown_curve = []
        # backtrader的drawdown分析器没有直接提供时序数据
        # 这里使用最大回撤信息
        max_dd = drawdown.get('max', {})

        # 构建结果
        total_trades = trades.get('total', {}).get('total', 0)
        winning_trades = trades.get('won', {}).get('total', 0)
        losing_trades = trades.get('lost', {}).get('total', 0)

        win_rate = winning_trades / total_trades if total_trades > 0 else 0

        # 计算盈亏比
        avg_win = trades.get('won', {}).get('pnl', {}).get('average', 0)
        avg_loss = abs(trades.get('lost', {}).get('pnl', {}).get('average', 1))
        profit_factor = avg_win / avg_loss if avg_loss > 0 else 0

        # 构建基准曲线（标准化为从1.0开始）
        benchmark_curve = []
        if hasattr(self, '_benchmark_data') and self._benchmark_data:
            first_price = self._benchmark_data[0]['close'] if self._benchmark_data else 1.0
            for point in self._benchmark_data:
                benchmark_curve.append({
                    'date': point['date'],
                    'value': point['close'] / first_price if first_price > 0 else 1.0
                })

        # 计算更多指标
        metrics = self._calculate_advanced_metrics(
            cumulative_returns, benchmark_curve, equity_curve,
            returns, max_dd, sharpe, total_trades, winning_trades,
            losing_trades, win_rate, profit_factor
        )

        result = {
            'metrics': metrics,
            'equity_curve': cumulative_returns,
            'benchmark_curve': benchmark_curve,
            'trades': trades_list,
            'drawdown': {
                'max_drawdown': max_dd.get('drawdown', 0) * 100,
                'max_drawdown_len': max_dd.get('len', 0),
                'max_drawdown_start': str(max_dd.get('start', '')),
                'max_drawdown_end': str(max_dd.get('end', '')),
            }
        }

        return result

    def _calculate_advanced_metrics(self, equity_curve, benchmark_curve, equity_returns,
                                    returns, max_dd, sharpe, total_trades, winning_trades,
                                    losing_trades, win_rate, profit_factor) -> Dict[str, Any]:
        """计算高级指标"""
        import math
        import statistics

        # 基础指标
        total_return = (returns.get('rtot', 0)) * 100
        annual_return = returns.get('rnorm100', 0)
        # 最大回撤 - backtrader返回的drawdown可能是小数形式(0.0713)或百分比形式(7.13)
        raw_dd = max_dd.get('drawdown', 0)
        if raw_dd > 1:  # 如果大于1，说明已经是百分比形式
            max_drawdown = raw_dd
        else:
            max_drawdown = raw_dd * 100
        sharpe_ratio = sharpe.get('sharperatio', 0) or 0

        # 基准收益
        benchmark_return = 0.0
        if benchmark_curve and len(benchmark_curve) >= 2:
            benchmark_return = (benchmark_curve[-1]['value'] - 1) * 100

        # 超额收益 (Alpha)
        excess_return = total_return - benchmark_return

        # 计算收益率序列（日收益率）
        strategy_returns = [point['return'] for point in equity_returns] if equity_returns else []

        # 计算基准收益率序列
        benchmark_returns = []
        if benchmark_curve and len(benchmark_curve) > 1:
            for i in range(1, len(benchmark_curve)):
                ret = (benchmark_curve[i]['value'] - benchmark_curve[i-1]['value']) / benchmark_curve[i-1]['value']
                benchmark_returns.append(ret)

        # 波动率（年化）
        strategy_volatility = 0.0
        if len(strategy_returns) > 1:
            try:
                vol = statistics.stdev(strategy_returns) * math.sqrt(252) * 100
                strategy_volatility = vol
            except:
                strategy_volatility = 0.0

        benchmark_volatility = 0.0
        if len(benchmark_returns) > 1:
            try:
                vol = statistics.stdev(benchmark_returns) * math.sqrt(252) * 100
                benchmark_volatility = vol
            except:
                benchmark_volatility = 0.0

        # Beta系数
        beta = 0.0
        if len(strategy_returns) > 1 and len(benchmark_returns) > 1:
            try:
                # 确保长度相同
                min_len = min(len(strategy_returns), len(benchmark_returns))
                if min_len > 1:
                    s_returns = strategy_returns[:min_len]
                    b_returns = benchmark_returns[:min_len]

                    # 计算协方差和基准方差
                    b_mean = statistics.mean(b_returns)
                    s_mean = statistics.mean(s_returns)

                    covariance = sum((s - s_mean) * (b - b_mean) for s, b in zip(s_returns, b_returns)) / (min_len - 1)
                    benchmark_variance = sum((b - b_mean) ** 2 for b in b_returns) / (min_len - 1)

                    if benchmark_variance > 0:
                        beta = covariance / benchmark_variance
            except:
                beta = 0.0

        # Alpha系数 (Jensen's Alpha)
        alpha = 0.0
        if len(strategy_returns) > 0 and len(benchmark_returns) > 0:
            try:
                risk_free_rate = 0.02 / 252  # 假设无风险利率2%，日利率
                avg_strategy_return = statistics.mean(strategy_returns)
                avg_benchmark_return = statistics.mean(benchmark_returns)
                alpha = (avg_strategy_return - risk_free_rate - beta * (avg_benchmark_return - risk_free_rate)) * 252 * 100
            except:
                alpha = 0.0

        # 索提诺比率 (Sortino Ratio)
        sortino_ratio = 0.0
        if len(strategy_returns) > 0:
            try:
                risk_free_daily = 0.02 / 252
                excess_returns = [r - risk_free_daily for r in strategy_returns]
                avg_excess = statistics.mean(excess_returns)

                # 下行标准差（只考虑负收益）
                negative_returns = [r for r in excess_returns if r < 0]
                if negative_returns:
                    downside_std = math.sqrt(sum(r ** 2 for r in negative_returns) / len(negative_returns))
                    if downside_std > 0:
                        sortino_ratio = (avg_excess * 252) / (downside_std * math.sqrt(252))
            except:
                sortino_ratio = 0.0

        # 信息比率 (Information Ratio)
        information_ratio = 0.0
        if len(strategy_returns) > 0 and len(benchmark_returns) > 0:
            try:
                min_len = min(len(strategy_returns), len(benchmark_returns))
                if min_len > 1:
                    excess_returns = [strategy_returns[i] - benchmark_returns[i] for i in range(min_len)]
                    avg_excess_return = statistics.mean(excess_returns)
                    tracking_error = statistics.stdev(excess_returns) if len(excess_returns) > 1 else 0
                    if tracking_error > 0:
                        information_ratio = (avg_excess_return * 252) / (tracking_error * math.sqrt(252))
            except:
                information_ratio = 0.0

        # 日胜率（日收益为正的比例）
        daily_win_rate = 0.0
        if strategy_returns:
            positive_days = sum(1 for r in strategy_returns if r > 0)
            daily_win_rate = (positive_days / len(strategy_returns)) * 100

        # 日均超额收益
        daily_excess_return = excess_return / len(strategy_returns) * 252 if strategy_returns else 0.0

        return {
            # 第一行指标
            'total_return': total_return,
            'annual_return': annual_return,
            'excess_return': excess_return,
            'benchmark_return': benchmark_return,
            'alpha': alpha,
            'beta': beta,
            'sharpe_ratio': sharpe_ratio,
            'win_rate': win_rate * 100,
            'profit_factor': profit_factor,
            'max_drawdown': max_drawdown,
            'sortino_ratio': sortino_ratio,
            # 第二行指标
            'daily_excess_return': daily_excess_return,
            'excess_drawdown': 0.0,  # 需要额外计算
            'excess_sharpe': information_ratio,  # 近似
            'daily_win_rate': daily_win_rate,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'information_ratio': information_ratio,
            'volatility': strategy_volatility,
            'benchmark_volatility': benchmark_volatility,
            'max_drawdown_duration': max_dd.get('len', 0),
            'total_trades': total_trades,
        }

    def plot(self, filename: Optional[str] = None):
        """绘制回测图表（需要matplotlib）"""
        if self.cerebro:
            self.cerebro.plot(style='candlestick', barup='red', bardown='green')
