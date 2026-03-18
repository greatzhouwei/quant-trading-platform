"""
回测API
"""
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from app.db.session import db_manager
from app.engine.backtrader_wrapper import BacktraderEngine

router = APIRouter()


class BacktestConfig(BaseModel):
    strategy_id: str
    strategy_type: str = Field(default="auto", description="策略类型: auto/backtrader/jqdata")
    symbol: Optional[str] = Field(default=None, description="股票代码，如 000001.SZ。聚宽策略可选，用于基准对比")
    start_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    timeframe: str = Field(default="1d", pattern=r"^(1d|1w|1m|5m|15m|30m|60m)$")
    initial_cash: float = Field(default=100000.0, gt=0)
    commission: float = Field(default=0.00025, ge=0, le=0.01)
    slippage: float = Field(default=0.001, ge=0)
    parameters: dict = Field(default_factory=dict)


class BacktestMetrics(BaseModel):
    total_return: float
    annual_return: float
    max_drawdown: float
    max_drawdown_duration: int
    sharpe_ratio: float
    sortino_ratio: Optional[float] = None
    calmar_ratio: Optional[float] = None
    volatility: Optional[float] = None
    win_rate: float
    profit_factor: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_trade_return: Optional[float] = None
    avg_winning_trade: Optional[float] = None
    avg_losing_trade: Optional[float] = None


class TradeRecord(BaseModel):
    datetime: str
    type: str
    price: float
    size: int
    value: float
    commission: float
    pnl: Optional[float] = None


class BacktestResult(BaseModel):
    id: str
    config: BacktestConfig
    status: str
    metrics: Optional[BacktestMetrics] = None
    equity_curve: List[dict] = []
    benchmark_curve: List[dict] = []
    trades: List[TradeRecord] = []
    drawdown_curve: List[dict] = []
    monthly_returns: List[dict] = []
    logs: List[str] = []
    error_message: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    execution_time: Optional[float] = None


@router.get("/history", response_model=List[dict])
async def get_backtest_history(
    strategy_id: Optional[str] = None,
    limit: int = 50
):
    """获取回测历史"""
    db = db_manager.get_connection()

    query = """
        SELECT b.id, b.strategy_id, s.name as strategy_name, b.config,
               b.status, b.total_return, b.annual_return, b.max_drawdown,
               b.sharpe_ratio, b.created_at, b.completed_at
        FROM backtest_records b
        JOIN strategies s ON b.strategy_id = s.id
        WHERE 1=1
    """
    params = []

    if strategy_id:
        query += " AND b.strategy_id = ?"
        params.append(strategy_id)

    query += " ORDER BY b.created_at DESC LIMIT ?"
    params.append(limit)

    results = db.execute(query, params).fetchall()

    return [
        {
            "id": row[0],
            "strategy_id": row[1],
            "strategy_name": row[2],
            "config": row[3],
            "status": row[4],
            "total_return": row[5],
            "annual_return": row[6],
            "max_drawdown": row[7],
            "sharpe_ratio": row[8],
            "created_at": row[9],
            "completed_at": row[10]
        }
        for row in results
    ]


@router.post("/run")
async def run_backtest(
    config: BacktestConfig,
    background_tasks: BackgroundTasks
):
    """
    执行回测（同步执行，适合快速回测）
    对于长时间回测，建议使用异步接口
    """
    db = db_manager.get_connection()

    # 检查策略是否存在
    strategy = db.execute(
        "SELECT code FROM strategies WHERE id = ? AND is_deleted = FALSE",
        [config.strategy_id]
    ).fetchone()

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    strategy_code = strategy[0]

    # 判断是否为聚宽策略
    is_jq_strategy = (
        config.strategy_type == 'jqdata' or
        'def initialize(context)' in strategy_code or
        'from jqdata import' in strategy_code
    )

    # 如果是聚宽策略且没有指定symbol，使用默认基准
    symbol = config.symbol
    if is_jq_strategy and not symbol:
        symbol = '000300.SH'  # 默认使用沪深300作为基准
        print(f"[INFO] 聚宽策略未指定基准，使用默认基准: {symbol}")

    # 如果非聚宽策略且没有symbol，报错
    if not is_jq_strategy and not symbol:
        raise HTTPException(status_code=400, detail="请选择股票代码")

    # 检查数据库里是否有数据
    data_check = db.execute("""
        SELECT COUNT(*) FROM kline_daily WHERE ts_code = ?
    """, [symbol]).fetchone()

    if data_check[0] == 0:
        raise HTTPException(
            status_code=400,
            detail=f"数据库中没有 {symbol} 的数据。请先前往'数据中心'页面同步股票数据，或选择其他有数据的股票。"
        )

    backtest_id = str(uuid.uuid4())

    # 创建回测记录
    db.execute("""
        INSERT INTO backtest_records (id, strategy_id, config, status)
        VALUES (?, ?, ?, 'running')
    """, [backtest_id, config.strategy_id, config.model_dump_json()])

    try:
        import json
        import os
        from datetime import datetime

        # 执行回测
        engine = BacktraderEngine()

        # 加载主数据（基准）
        data = engine.load_data(
            symbol=symbol,
            start_date=config.start_date,
            end_date=config.end_date,
            timeframe=config.timeframe
        )

        # 如果是聚宽策略，额外加载一些常用股票数据供交易
        extra_data = []
        if is_jq_strategy:
            # 加载一些常用的A股供策略交易（如平安银行等）
            common_stocks = ['000001.SZ', '000002.SZ', '600519.SH']
            for stock in common_stocks:
                try:
                    stock_data = engine.load_data(
                        symbol=stock,
                        start_date=config.start_date,
                        end_date=config.end_date,
                        timeframe=config.timeframe
                    )
                    extra_data.append(stock_data)
                    print(f"[INFO] 已加载股票数据: {stock}")
                except ValueError as e:
                    print(f"[WARN] 无法加载 {stock}: {e}")

        # 加载策略
        strategy_type = config.strategy_type if config.strategy_type != 'auto' else 'auto'
        strategy_class = engine.load_strategy(strategy_code, strategy_type=strategy_type)

        # 运行回测
        result = engine.run_backtest(
            strategy_class=strategy_class,
            data=data,
            config=config.model_dump(),
            extra_data=extra_data if extra_data else None
        )

        # 保存完整结果到文件
        result_dir = Path(__file__).parent.parent.parent.parent.parent / 'data' / 'backtest_results'
        result_dir.mkdir(parents=True, exist_ok=True)
        result_path = result_dir / f'{backtest_id}.json'

        # 添加时间戳和配置信息
        result_with_meta = {
            'backtest_id': backtest_id,
            'config': config.model_dump(),
            'created_at': datetime.now().isoformat(),
            'metrics': result.get('metrics', {}),
            'equity_curve': result.get('equity_curve', []),
            'benchmark_curve': result.get('benchmark_curve', []),
            'trades': result.get('trades', []),
            'drawdown': result.get('drawdown', {}),
            'logs': result.get('logs', []),
        }

        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(result_with_meta, f, ensure_ascii=False, indent=2)

        # 更新回测记录
        db.execute("""
            UPDATE backtest_records
            SET status = 'completed',
                total_return = ?,
                annual_return = ?,
                max_drawdown = ?,
                sharpe_ratio = ?,
                completed_at = now(),
                result_path = ?
            WHERE id = ?
        """, [
            result['metrics'].get('total_return'),
            result['metrics'].get('annual_return'),
            result['metrics'].get('max_drawdown'),
            result['metrics'].get('sharpe_ratio'),
            str(result_path),
            backtest_id
        ])

        # 更新策略回测次数
        db.execute("""
            UPDATE strategies
            SET backtest_count = backtest_count + 1,
                last_backtest_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, [config.strategy_id])

        # 构建完整的回测结果（包含config和id）
        full_result = {
            "id": backtest_id,
            "config": config.model_dump(),
            "status": "completed",
            "metrics": result.get('metrics', {}),
            "equity_curve": result.get('equity_curve', []),
            "benchmark_curve": result.get('benchmark_curve', []),
            "trades": result.get('trades', []),
            "drawdown_curve": result.get('drawdown', {}).get('curve', []),
            "monthly_returns": result.get('monthly_returns', []),
            "logs": result.get('logs', []),
            "created_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
        }

        return {
            "id": backtest_id,
            "status": "completed",
            "result": full_result
        }

    except Exception as e:
        import traceback
        error_detail = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[ERROR] 回测执行失败: {error_detail}")

        # 更新失败状态
        db.execute("""
            UPDATE backtest_records
            SET status = 'failed',
                error_message = ?,
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, [error_detail[:500], backtest_id])

        raise HTTPException(status_code=500, detail=f"回测执行失败: {str(e)}")


@router.get("/{backtest_id}", response_model=BacktestResult)
async def get_backtest_result(backtest_id: str):
    """获取回测结果"""
    db = db_manager.get_connection()

    # 获取所有列
    row = db.execute(
        "SELECT * FROM backtest_records WHERE id = ?",
        [backtest_id]
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="回测记录不存在")

    # 解析列（根据表结构）
    # columns: id, strategy_id, config, status, total_return, annual_return, max_drawdown,
    #          sharpe_ratio, result_path, created_at, completed_at, execution_time, error_message
    import json
    config = json.loads(row[2]) if row[2] else {}

    # 从文件加载详细结果
    result_path = row[8] if len(row) > 8 else None  # result_path column (index 8)
    details = {}

    if result_path and os.path.exists(result_path):
        try:
            with open(result_path, 'r', encoding='utf-8') as f:
                details = json.load(f)
        except Exception as e:
            print(f"读取结果文件失败: {e}")

    # 构建完整响应
    # 确保所有metrics字段都有默认值
    metrics_from_db = {
        "total_return": row[4] or 0,
        "annual_return": row[5] or 0,
        "max_drawdown": row[6] or 0,
        "max_drawdown_duration": 0,
        "sharpe_ratio": row[7] or 0,
        "sortino_ratio": None,
        "calmar_ratio": None,
        "volatility": None,
        "win_rate": 0,
        "profit_factor": 0,
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "avg_trade_return": None,
        "avg_winning_trade": None,
        "avg_losing_trade": None,
    }

    # 优先使用文件中的详细指标
    if 'metrics' in details:
        metrics = {**metrics_from_db, **details['metrics']}
    else:
        metrics = metrics_from_db

    # 处理时间字段
    created_at = row[9]  # index 9: created_at
    if hasattr(created_at, 'isoformat'):
        created_at = created_at.isoformat()

    completed_at = row[10]  # index 10: completed_at
    if hasattr(completed_at, 'isoformat'):
        completed_at = completed_at.isoformat()

    execution_time = row[11]  # index 11: execution_time

    return {
        "id": row[0],
        "config": config,
        "status": row[3],
        "metrics": metrics,
        "equity_curve": details.get('equity_curve', []),
        "benchmark_curve": details.get('benchmark_curve', []),
        "trades": details.get('trades', []),
        "drawdown_curve": details.get('drawdown', {}).get('curve', []),
        "monthly_returns": details.get('monthly_returns', []),
        "logs": details.get('logs', []),
        "error_message": row[12],  # index 12: error_message
        "created_at": created_at,
        "completed_at": completed_at,
        "execution_time": execution_time
    }


@router.delete("/{backtest_id}")
async def delete_backtest(backtest_id: str):
    """删除回测记录"""
    db = db_manager.get_connection()

    result = db.execute(
        "DELETE FROM backtest_records WHERE id = ?",
        [backtest_id]
    )

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="回测记录不存在")

    return {"message": "回测记录已删除"}
