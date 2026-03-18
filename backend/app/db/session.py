"""
DuckDB数据库连接管理
"""
import duckdb
from contextlib import contextmanager
from pathlib import Path
from app.core.config import settings


class DuckDBManager:
    """DuckDB连接管理器"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.DUCKDB_PATH
        # 使用线程本地存储，每个线程有自己的连接
        self._local = {}

    def _ensure_data_dir(self):
        """确保数据目录存在"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def get_connection(self):
        """获取数据库连接（每个线程独立）"""
        import threading
        thread_id = threading.current_thread().ident

        if thread_id not in self._local:
            self._ensure_data_dir()
            self._local[thread_id] = duckdb.connect(self.db_path)
            # 启用并行查询
            self._local[thread_id].execute("PRAGMA threads=4")

        return self._local[thread_id]

    def close(self):
        """关闭所有连接"""
        for conn in self._local.values():
            try:
                conn.close()
            except:
                pass
        self._local.clear()

    @contextmanager
    def session(self):
        """上下文管理器，用于数据库操作"""
        conn = self.get_connection()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise

    def init_tables(self):
        """初始化数据库表"""
        conn = self.get_connection()

        # 股票基础信息表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                ts_code VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                area VARCHAR,
                industry VARCHAR,
                fullname VARCHAR,
                enname VARCHAR,
                cnspell VARCHAR,
                market VARCHAR,
                exchange VARCHAR,
                curr_type VARCHAR,
                list_status VARCHAR,
                list_date DATE,
                delist_date DATE,
                is_hs VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 日线数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kline_daily (
                ts_code VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                pre_close DOUBLE,
                change DOUBLE,
                pct_chg DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 分钟线数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kline_minute (
                ts_code VARCHAR NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                PRIMARY KEY (ts_code, trade_time)
            )
        """)

        # 策略表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategies (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                description TEXT,
                strategy_type VARCHAR DEFAULT 'custom',
                code TEXT NOT NULL,
                parameters JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_backtest_at TIMESTAMP,
                backtest_count INTEGER DEFAULT 0,
                is_deleted BOOLEAN DEFAULT FALSE
            )
        """)

        # 回测记录表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_records (
                id VARCHAR PRIMARY KEY,
                strategy_id VARCHAR NOT NULL,
                config JSON NOT NULL,
                status VARCHAR DEFAULT 'pending',
                total_return DOUBLE,
                annual_return DOUBLE,
                max_drawdown DOUBLE,
                sharpe_ratio DOUBLE,
                result_path VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                execution_time DOUBLE,
                error_message TEXT,
                FOREIGN KEY (strategy_id) REFERENCES strategies(id)
            )
        """)

        # 数据同步状态表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_sync_status (
                table_name VARCHAR PRIMARY KEY,
                last_sync_date DATE,
                last_sync_time TIMESTAMP,
                record_count INTEGER,
                status VARCHAR,
                message TEXT
            )
        """)

        # 分红数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_dividend (
                ts_code VARCHAR NOT NULL,
                end_date DATE NOT NULL,
                ann_date DATE,
                div_proc VARCHAR,
                stk_div DOUBLE,
                stk_bo_rate DOUBLE,
                stk_co_rate DOUBLE,
                cash_div DOUBLE,
                cash_div_tax DOUBLE,
                record_date DATE,
                ex_date DATE,
                pay_date DATE,
                div_listdate DATE,
                imp_ann_date DATE,
                base_date DATE,
                base_share DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, end_date)
            )
        """)

        # 每日基本面指标表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_daily_basic (
                ts_code VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                close DOUBLE,
                turnover_rate DOUBLE,
                turnover_rate_f DOUBLE,
                volume_ratio DOUBLE,
                pe DOUBLE,
                pe_ttm DOUBLE,
                pb DOUBLE,
                ps DOUBLE,
                ps_ttm DOUBLE,
                dv_ratio DOUBLE,
                dv_ttm DOUBLE,
                total_share DOUBLE,
                float_share DOUBLE,
                free_share DOUBLE,
                total_mv DOUBLE,
                circ_mv DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 财务指标表（季度）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_fina_indicator (
                ts_code VARCHAR NOT NULL,
                ann_date DATE,
                end_date DATE NOT NULL,
                eps DOUBLE,
                dt_eps DOUBLE,
                total_revenue_ps DOUBLE,
                revenue_ps DOUBLE,
                capital_rese_ps DOUBLE,
                surplus_rese_ps DOUBLE,
                undist_profit_ps DOUBLE,
                extra_item DOUBLE,
                profit_dedt DOUBLE,
                gross_margin DOUBLE,
                current_ratio DOUBLE,
                quick_ratio DOUBLE,
                cash_ratio DOUBLE,
                invturn_days DOUBLE,
                arturn_days DOUBLE,
                inv_turn DOUBLE,
                ar_turn DOUBLE,
                ca_turn DOUBLE,
                fa_turn DOUBLE,
                assets_turn DOUBLE,
                op_income DOUBLE,
                valuechange_income DOUBLE,
                interst_income DOUBLE,
                daa DOUBLE,
                ebit DOUBLE,
                ebitda DOUBLE,
                fcff DOUBLE,
                fcfe DOUBLE,
                current_exint DOUBLE,
                noncurrent_exint DOUBLE,
                interestdebt DOUBLE,
                netdebt DOUBLE,
                tangible_asset DOUBLE,
                working_capital DOUBLE,
                networking_capital DOUBLE,
                invest_capital DOUBLE,
                retained_earnings DOUBLE,
                diluted2_eps DOUBLE,
                bps DOUBLE,
                ocfps DOUBLE,
                retainedps DOUBLE,
                cfps DOUBLE,
                ebit_ps DOUBLE,
                fcff_ps DOUBLE,
                fcfe_ps DOUBLE,
                netprofit_margin DOUBLE,
                grossprofit_margin DOUBLE,
                cogs_of_sales DOUBLE,
                expense_of_sales DOUBLE,
                profit_to_gr DOUBLE,
                saleexp_to_gr DOUBLE,
                adminexp_of_gr DOUBLE,
                finaexp_of_gr DOUBLE,
                impai_ttm DOUBLE,
                gc_of_gr DOUBLE,
                op_of_gr DOUBLE,
                ebit_of_gr DOUBLE,
                roe DOUBLE,
                roe_waa DOUBLE,
                roe_dt DOUBLE,
                roa DOUBLE,
                npta DOUBLE,
                roic DOUBLE,
                roe_yearly DOUBLE,
                roa2_yearly DOUBLE,
                roe_avg DOUBLE,
                opincome_of_ebt DOUBLE,
                investincome_of_ebt DOUBLE,
                n_op_profit_of_ebt DOUBLE,
                tax_to_ebt DOUBLE,
                dtprofit_to_profit DOUBLE,
                salescash_to_or DOUBLE,
                ocf_to_or DOUBLE,
                ocf_to_opincome DOUBLE,
                capitalized_to_da DOUBLE,
                debt_to_assets DOUBLE,
                assets_to_eqt DOUBLE,
                dp_assets_to_eqt DOUBLE,
                ca_to_assets DOUBLE,
                nca_to_assets DOUBLE,
                tbassets_to_totalassets DOUBLE,
                int_to_talcap DOUBLE,
                eqt_to_talcapital DOUBLE,
                currentdebt_to_debt DOUBLE,
                longdeb_to_debt DOUBLE,
                ocf_to_shortdebt DOUBLE,
                debt_to_eqt DOUBLE,
                eqt_to_debt DOUBLE,
                eqt_to_interestdebt DOUBLE,
                tangibleasset_to_debt DOUBLE,
                tangasset_to_intdebt DOUBLE,
                tangibleasset_to_netdebt DOUBLE,
                ocf_to_debt DOUBLE,
                ocf_to_interestdebt DOUBLE,
                ocf_to_netdebt DOUBLE,
                ebit_to_interest DOUBLE,
                longdebt_to_workingcapital DOUBLE,
                ebitda_to_debt DOUBLE,
                turn_days DOUBLE,
                roa_ytd DOUBLE,
                roe_ytd DOUBLE,
                gross_margin_ytd DOUBLE,
                roa_ytd_yearly DOUBLE,
                profit_to_op_ytd DOUBLE,
                profit_to_op_ytd_yearly DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, end_date)
            )
        """)

        # 创建索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_daily_code_date ON kline_daily(ts_code, trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_daily_date ON kline_daily(trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_strategy ON backtest_records(strategy_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_created ON backtest_records(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dividend_code ON stock_dividend(ts_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_basic_code_date ON stock_daily_basic(ts_code, trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_basic_date ON stock_daily_basic(trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fina_indicator_code ON stock_fina_indicator(ts_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fina_indicator_end_date ON stock_fina_indicator(end_date)")

        print("数据库表初始化完成")


# 全局数据库管理器实例
db_manager = DuckDBManager()


def init_db():
    """初始化数据库"""
    db_manager.init_tables()
