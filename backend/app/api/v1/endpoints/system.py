"""
系统管理API
"""
import os
import platform

from fastapi import APIRouter

from app.core.config import settings
from app.db.session import db_manager

router = APIRouter()


@router.get("/info")
async def get_system_info():
    """获取系统信息"""
    db = db_manager.get_connection()

    # 获取各表记录数
    stats = {}
    try:
        stocks_count = db.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
        stats['stocks_count'] = stocks_count
    except:
        stats['stocks_count'] = 0

    try:
        kline_count = db.execute("SELECT COUNT(*) FROM kline_daily").fetchone()[0]
        stats['kline_daily_count'] = kline_count
    except:
        stats['kline_daily_count'] = 0

    try:
        strategies_count = db.execute("SELECT COUNT(*) FROM strategies WHERE is_deleted = FALSE").fetchone()[0]
        stats['strategies_count'] = strategies_count
    except:
        stats['strategies_count'] = 0

    try:
        backtest_count = db.execute("SELECT COUNT(*) FROM backtest_records").fetchone()[0]
        stats['backtest_count'] = backtest_count
    except:
        stats['backtest_count'] = 0

    return {
        "app_name": "量化交易回测系统",
        "version": "1.0.0",
        "platform": platform.system(),
        "python_version": platform.python_version(),
        "debug": settings.DEBUG,
        "database": {
            "type": "DuckDB",
            "path": settings.DUCKDB_PATH,
            "stats": stats
        }
    }


@router.get("/config")
async def get_config():
    """获取系统配置（敏感信息已脱敏）"""
    return {
        "api_host": settings.API_HOST,
        "api_port": settings.API_PORT,
        "database_path": settings.DUCKDB_PATH,
        "auto_sync_enabled": settings.AUTO_SYNC_ENABLED,
        "sync_stocks_cron": settings.SYNC_STOCKS_CRON,
        "sync_daily_cron": settings.SYNC_DAILY_CRON,
        "tushare_configured": bool(settings.TUSHARE_TOKEN)
    }
