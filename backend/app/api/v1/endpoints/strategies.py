"""
策略管理API
"""
import json
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.db.session import db_manager

router = APIRouter()


class StrategyBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    strategy_type: str = Field(default="custom")
    parameters: dict = Field(default_factory=dict)


class StrategyCreate(StrategyBase):
    code: str = Field(..., description="策略Python代码")


class StrategyUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    code: Optional[str] = None
    parameters: Optional[dict] = None


class StrategyResponse(StrategyBase):
    id: str
    code: str
    created_at: datetime
    updated_at: datetime
    last_backtest_at: Optional[datetime] = None
    backtest_count: int = 0

    class Config:
        from_attributes = True


@router.get("", response_model=List[StrategyResponse])
async def list_strategies(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    strategy_type: Optional[str] = None
):
    """获取策略列表"""
    db = db_manager.get_connection()

    query = "SELECT * FROM strategies WHERE is_deleted = FALSE"
    params = []

    if strategy_type:
        query += " AND strategy_type = ?"
        params.append(strategy_type)

    query += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, skip])

    results = db.execute(query, params).fetchall()

    return [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "strategy_type": row[3],
            "code": row[4],
            "parameters": json.loads(row[5]) if row[5] else {},
            "created_at": row[6],
            "updated_at": row[7],
            "last_backtest_at": row[8],
            "backtest_count": row[9]
        }
        for row in results
    ]


@router.get("/templates")
async def get_strategy_templates():
    """获取策略模板"""
    templates = [
        {
            "name": "双均线策略",
            "type": "cta",
            "description": "基于短期和长期移动平均线的交叉信号进行交易",
            "code": '''
import backtrader as bt

class DoubleMAStrategy(bt.Strategy):
    params = (
        ('fast_period', 5),
        ('slow_period', 20),
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SMA(period=self.p.fast_period)
        self.slow_ma = bt.indicators.SMA(period=self.p.slow_period)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.sell()
'''
        },
        {
            "name": "MACD策略",
            "type": "cta",
            "description": "基于MACD指标的金叉死叉信号进行交易",
            "code": '''
import backtrader as bt

class MACDStrategy(bt.Strategy):
    params = (
        ('fast', 12),
        ('slow', 26),
        ('signal', 9),
    )

    def __init__(self):
        self.macd = bt.indicators.MACD(
            period1=self.p.fast,
            period2=self.p.slow,
            period_signal=self.p.signal
        )

    def next(self):
        if not self.position:
            if self.macd.macd > self.macd.signal:
                self.buy()
        elif self.macd.macd < self.macd.signal:
            self.sell()
'''
        },
        {
            "name": "RSI策略",
            "type": "mean_reversion",
            "description": "基于RSI超买超卖信号进行交易",
            "code": '''
import backtrader as bt

class RSIStrategy(bt.Strategy):
    params = (
        ('period', 14),
        ('upper', 70),
        ('lower', 30),
    )

    def __init__(self):
        self.rsi = bt.indicators.RSI(period=self.p.period)

    def next(self):
        if not self.position:
            if self.rsi < self.p.lower:
                self.buy()
        elif self.rsi > self.p.upper:
            self.sell()
'''
        }
    ]
    return templates


@router.post("", response_model=StrategyResponse)
async def create_strategy(strategy: StrategyCreate):
    """创建新策略"""
    db = db_manager.get_connection()

    strategy_id = str(uuid.uuid4())

    db.execute("""
        INSERT INTO strategies (id, name, description, strategy_type, code, parameters)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [
        strategy_id,
        strategy.name,
        strategy.description,
        strategy.strategy_type,
        strategy.code,
        strategy.parameters
    ])

    # 返回创建的策略
    return await get_strategy(strategy_id)


@router.get("/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(strategy_id: str):
    """获取策略详情"""
    db = db_manager.get_connection()

    row = db.execute(
        "SELECT * FROM strategies WHERE id = ? AND is_deleted = FALSE",
        [strategy_id]
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="策略不存在")

    return {
        "id": row[0],
        "name": row[1],
        "description": row[2],
        "strategy_type": row[3],
        "code": row[4],
        "parameters": json.loads(row[5]) if row[5] else {},
        "created_at": row[6],
        "updated_at": row[7],
        "last_backtest_at": row[8],
        "backtest_count": row[9]
    }


@router.put("/{strategy_id}", response_model=StrategyResponse)
async def update_strategy(strategy_id: str, strategy: StrategyUpdate):
    """更新策略"""
    db = db_manager.get_connection()

    # 检查策略是否存在
    existing = db.execute(
        "SELECT id FROM strategies WHERE id = ? AND is_deleted = FALSE",
        [strategy_id]
    ).fetchone()

    if not existing:
        raise HTTPException(status_code=404, detail="策略不存在")

    # 构建更新语句
    updates = []
    params = []

    if strategy.name is not None:
        updates.append("name = ?")
        params.append(strategy.name)
    if strategy.description is not None:
        updates.append("description = ?")
        params.append(strategy.description)
    if strategy.code is not None:
        updates.append("code = ?")
        params.append(strategy.code)
    if strategy.parameters is not None:
        updates.append("parameters = ?")
        params.append(strategy.parameters)

    updates.append("updated_at = CURRENT_TIMESTAMP")

    if updates:
        query = f"UPDATE strategies SET {', '.join(updates)} WHERE id = ?"
        params.append(strategy_id)
        db.execute(query, params)

    return await get_strategy(strategy_id)


@router.delete("/{strategy_id}")
async def delete_strategy(strategy_id: str):
    """删除策略（软删除）"""
    db = db_manager.get_connection()

    result = db.execute(
        "UPDATE strategies SET is_deleted = TRUE WHERE id = ?",
        [strategy_id]
    )

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="策略不存在")

    return {"message": "策略已删除"}


@router.post("/{strategy_id}/validate")
async def validate_strategy(strategy_id: str):
    """验证策略代码"""
    db = db_manager.get_connection()

    row = db.execute(
        "SELECT code FROM strategies WHERE id = ? AND is_deleted = FALSE",
        [strategy_id]
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="策略不存在")

    code = row[0]

    # 简单的语法检查
    import ast
    try:
        ast.parse(code)
        return {
            "valid": True,
            "message": "代码语法正确"
        }
    except SyntaxError as e:
        return {
            "valid": False,
            "message": f"语法错误: {e.msg}",
            "line": e.lineno,
            "column": e.offset
        }
