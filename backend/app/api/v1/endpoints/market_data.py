"""
市场数据API
"""
from typing import List, Optional
from datetime import date

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from app.db.session import db_manager
from app.services.data_sync_service import data_sync_service

router = APIRouter()


class StockInfo(BaseModel):
    ts_code: str
    symbol: str
    name: str
    area: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    list_date: Optional[date] = None


class KLineData(BaseModel):
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    vol: float
    amount: float


@router.get("/stocks", response_model=List[StockInfo])
async def get_stock_list(
    industry: Optional[str] = None,
    market: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """获取股票列表"""
    db = db_manager.get_connection()

    query = "SELECT ts_code, symbol, name, area, industry, market, list_date FROM stocks WHERE 1=1"
    params = []

    if industry:
        query += " AND industry = ?"
        params.append(industry)

    if market:
        query += " AND market = ?"
        params.append(market)

    if search:
        query += " AND (name LIKE ? OR ts_code LIKE ? OR symbol LIKE ?)"
        search_pattern = f"%{search}%"
        params.extend([search_pattern, search_pattern, search_pattern])

    query += " ORDER BY ts_code LIMIT ?"
    params.append(limit)

    results = db.execute(query, params).fetchall()

    return [
        {
            "ts_code": row[0],
            "symbol": row[1],
            "name": row[2],
            "area": row[3],
            "industry": row[4],
            "market": row[5],
            "list_date": row[6]
        }
        for row in results
    ]


@router.get("/stocks/{ts_code}")
async def get_stock_detail(ts_code: str):
    """获取股票详情"""
    db = db_manager.get_connection()

    row = db.execute(
        "SELECT * FROM stocks WHERE ts_code = ?",
        [ts_code]
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="股票不存在")

    return {
        "ts_code": row[0],
        "symbol": row[1],
        "name": row[2],
        "area": row[3],
        "industry": row[4],
        "fullname": row[5],
        "market": row[8],
        "exchange": row[9],
        "list_status": row[11],
        "list_date": row[12],
        "delist_date": row[13]
    }


@router.get("/kline/{ts_code}", response_model=List[KLineData])
async def get_kline(
    ts_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(500, ge=1, le=5000)
):
    """获取K线数据"""
    db = db_manager.get_connection()

    query = """
        SELECT trade_date, open, high, low, close, vol, amount
        FROM kline_daily
        WHERE ts_code = ?
    """
    params = [ts_code]

    if start_date:
        query += " AND trade_date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND trade_date <= ?"
        params.append(end_date)

    query += " ORDER BY trade_date DESC LIMIT ?"
    params.append(limit)

    results = db.execute(query, params).fetchall()

    return [
        {
            "trade_date": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "vol": row[5],
            "amount": row[6]
        }
        for row in results
    ][::-1]  # 反转，按日期升序返回


@router.get("/industries")
async def get_industries():
    """获取行业列表"""
    db = db_manager.get_connection()

    results = db.execute(
        "SELECT DISTINCT industry FROM stocks WHERE industry IS NOT NULL ORDER BY industry"
    ).fetchall()

    return [row[0] for row in results]


@router.get("/data-status")
async def get_data_status():
    """获取数据同步状态"""
    return data_sync_service.get_sync_status()


@router.post("/sync")
async def trigger_data_sync(
    sync_type: str = "daily",
    trade_date: Optional[str] = None
):
    """
    触发数据同步

    Args:
        sync_type: 同步类型 (stocks-股票列表, daily-日线数据)
        trade_date: 指定交易日（仅daily有效）
    """
    import asyncio

    if sync_type == "stocks":
        result = await data_sync_service.sync_stock_list()
    elif sync_type == "daily":
        if trade_date:
            result = await data_sync_service.sync_daily_for_date(trade_date)
        else:
            result = await data_sync_service.sync_daily_for_date()
    else:
        raise HTTPException(status_code=400, detail="无效的同步类型")

    return result
