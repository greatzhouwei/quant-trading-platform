"""
应用配置管理
"""
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """应用配置"""

    # Tushare配置
    TUSHARE_TOKEN: str = ""

    # 数据库配置
    DUCKDB_PATH: str = "data/quant_trading.duckdb"

    # Redis配置（用于Celery）
    REDIS_URL: str = "redis://localhost:6379/0"

    # API配置
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = True

    # 安全配置
    SECRET_KEY: str = "your-secret-key-change-this-in-production"

    # 数据同步配置
    AUTO_SYNC_ENABLED: bool = True
    SYNC_STOCKS_CRON: str = "0 2 * * *"  # 每天凌晨2点
    SYNC_DAILY_CRON: str = "0 3 * * *"   # 每天凌晨3点

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """获取配置实例（单例）"""
    return Settings()


settings = get_settings()
