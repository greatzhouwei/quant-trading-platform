"""
API路由聚合
"""
from fastapi import APIRouter

from app.api.v1.endpoints import strategies, backtest, market_data, system

api_router = APIRouter()

# 策略管理
api_router.include_router(
    strategies.router,
    prefix="/strategies",
    tags=["策略管理"]
)

# 回测
api_router.include_router(
    backtest.router,
    prefix="/backtest",
    tags=["回测"]
)

# 市场数据
api_router.include_router(
    market_data.router,
    prefix="/market-data",
    tags=["市场数据"]
)

# 系统管理
api_router.include_router(
    system.router,
    prefix="/system",
    tags=["系统管理"]
)
