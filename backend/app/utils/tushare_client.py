"""
Tushare客户端封装
"""
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from app.core.config import settings


class TushareClient:
    """Tushare数据客户端"""

    def __init__(self):
        self.pro = None
        self._init_client()

    def _init_client(self):
        """初始化Tushare客户端"""
        if settings.TUSHARE_TOKEN and settings.TUSHARE_TOKEN != 'your_token_here':
            ts.set_token(settings.TUSHARE_TOKEN)
            self.pro = ts.pro_api()
        else:
            print("[WARNING] TUSHARE_TOKEN未配置，请在.env文件中设置有效的Token")
            print("          访问 https://tushare.pro 注册获取Token")
            self.pro = None

    def _check_client(self):
        """检查客户端是否初始化"""
        if self.pro is None:
            raise ValueError("TUSHARE_TOKEN未配置，无法获取数据。请在.env文件中设置有效的Token")

    def get_stock_list(self) -> pd.DataFrame:
        """获取A股股票列表"""
        self._check_client()
        df = self.pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,'
                   'market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
        )
        return df

    def get_daily_kline(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取日线数据

        Args:
            ts_code: 股票代码，如 '000001.SZ'
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'
            trade_date: 特定交易日（与start_date/end_date互斥）
        """
        self._check_client()
        if trade_date:
            df = self.pro.daily(ts_code=ts_code, trade_date=trade_date)
        else:
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
        return df

    def get_daily_kline_all(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取所有股票的日线数据（适合批量更新）"""
        self._check_client()
        if trade_date:
            # 单日获取所有股票数据（一次API调用）
            df = self.pro.daily(trade_date=trade_date)
        elif start_date and end_date:
            # 分日期获取，每天一次API调用
            all_data = []
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')

            for date in date_range:
                date_str = date.strftime('%Y%m%d')
                try:
                    df = self.pro.daily(trade_date=date_str)
                    if not df.empty:
                        all_data.append(df)
                        print(f"  获取 {date_str}: {len(df)} 条记录")
                except Exception as e:
                    print(f"  获取 {date_str} 失败: {e}")

            if all_data:
                df = pd.concat(all_data, ignore_index=True)
            else:
                df = pd.DataFrame()
        else:
            raise ValueError("必须提供 trade_date 或 start_date+end_date")

        return df

    def get_minute_kline(
        self,
        ts_code: str,
        freq: str = '1min',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取分钟线数据

        Args:
            ts_code: 股票代码
            freq: 分钟频率，支持 1min/5min/15min/30min/60min
            start_date: 开始日期 'YYYY-MM-DD HH:MM:SS'
            end_date: 结束日期 'YYYY-MM-DD HH:MM:SS'
        """
        self._check_client()
        df = ts.pro_bar(
            ts_code=ts_code,
            freq=freq,
            start_date=start_date,
            end_date=end_date
        )
        return df

    def get_trade_calendar(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: str = 'SSE'
    ) -> pd.DataFrame:
        """获取交易日历"""
        self._check_client()
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        df = self.pro.trade_cal(
            exchange=exchange,
            start_date=start_date,
            end_date=end_date
        )
        return df

    def get_latest_trade_date(self) -> str:
        """获取最近一个交易日"""
        self._check_client()
        today = datetime.now().strftime('%Y%m%d')
        df = self.pro.trade_cal(
            exchange='SSE',
            start_date=today,
            end_date=today,
            is_open='1'
        )

        if not df.empty:
            return df.iloc[0]['cal_date']

        # 今天不是交易日，找最近一个
        df = self.pro.trade_cal(
            exchange='SSE',
            end_date=today,
            is_open='1'
        )

        if not df.empty:
            return df.iloc[0]['cal_date']

        return today

    def get_dividend(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取分红送股数据

        Args:
            ts_code: 股票代码
            start_date: 公告开始日期 YYYYMMDD
            end_date: 公告结束日期 YYYYMMDD

        Returns:
            DataFrame with columns:
            - ts_code: 股票代码
            - end_date: 分红年度
            - ann_date: 公告日
            - div_proc: 实施进度
            - stk_div: 每股送转
            - stk_bo_rate: 每股送股比例
            - stk_co_rate: 每股转增比例
            - cash_div: 每股分红
            - cash_div_tax: 每股分红扣税
            - record_date: 股权登记日
            - ex_date: 除权除息日
            - pay_date: 派息日
            - div_listdate: 红股上市日
            - imp_ann_date: 实施公告日
            - base_date: 基准日
            - base_share: 基准股本
        """
        self._check_client()
        df = self.pro.dividend(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        return df

    def get_daily_basic(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取每日基本面指标

        Args:
            ts_code: 股票代码
            trade_date: 交易日期 YYYYMMDD
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with columns:
            - ts_code: 股票代码
            - trade_date: 交易日期
            - close: 收盘价
            - turnover_rate: 换手率
            - turnover_rate_f: 换手率(自由流通股)
            - volume_ratio: 量比
            - pe: 市盈率
            - pe_ttm: 市盈率TTM
            - pb: 市净率
            - ps: 市销率
            - ps_ttm: 市销率TTM
            - dv_ratio: 股息率
            - dv_ttm: 股息率TTM
            - total_share: 总股本
            - float_share: 流通股本
            - free_share: 自由流通股本
            - total_mv: 总市值
            - circ_mv: 流通市值
        """
        self._check_client()
        if trade_date:
            df = self.pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
        else:
            df = self.pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
        return df

    def get_fina_indicator(
        self,
        ts_code: Optional[str] = None,
        period: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取财务指标数据（季度）

        Args:
            ts_code: 股票代码
            period: 报告期 YYYYMMDD
            start_date: 公告开始日期
            end_date: 公告结束日期

        Returns:
            DataFrame with financial indicators including:
            - ts_code: 股票代码
            - ann_date: 公告日期
            - end_date: 报告期
            - eps: 每股收益
            - dt_eps: 稀释每股收益
            - roe: 净资产收益率
            - roe_waa: 加权平均净资产收益率
            - roe_dt: 净资产收益率(扣非)
            - roa: 总资产报酬率
            - roic: 投入资本回报率
            - grossprofit_margin: 销售毛利率
            - netprofit_margin: 销售净利率
            - revenue_yoy: 营业收入同比增长率
            - profit_yoy: 净利润同比增长率
            - ebit_to_interest: 已获利息倍数
            - debt_to_assets: 资产负债率
            - current_ratio: 流动比率
            - quick_ratio: 速动比率
            - and many more...
        """
        self._check_client()
        if period:
            df = self.pro.fina_indicator(ts_code=ts_code, period=period)
        else:
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
        return df

    def get_fina_indicator_all(
        self,
        trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取所有股票的最新财务指标

        Args:
            trade_date: 交易日，获取该日期可用的最新财务数据

        Returns:
            DataFrame
        """
        self._check_client()
        if trade_date:
            # 获取特定日期之前最新的财务数据
            # 按报告期倒序，取第一个
            df = self.pro.fina_indicator(end_date=trade_date)
        else:
            df = self.pro.fina_indicator()

        return df


# 全局客户端实例
tushare_client = TushareClient()
