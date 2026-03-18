"""
数据同步脚本
用法: python scripts/sync_data.py [stocks|daily|all]
"""
import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.data_sync_service import data_sync_service
from app.db.session import init_db


async def main():
    if len(sys.argv) < 2:
        print("用法: python sync_data.py [stocks|daily|all]")
        sys.exit(1)

    sync_type = sys.argv[1]

    # 初始化数据库
    print("初始化数据库...")
    init_db()

    if sync_type == "stocks":
        print("\n同步股票列表...")
        result = await data_sync_service.sync_stock_list()
        print(f"结果: {result}")

    elif sync_type == "daily":
        print("\n同步日线数据...")
        # 同步最新一个交易日的数据（省API调用）
        result = await data_sync_service.sync_daily_for_date()
        print(f"结果: {result}")

    elif sync_type == "all":
        print("\n同步股票列表...")
        result1 = await data_sync_service.sync_stock_list()
        print(f"结果: {result1}")

        print("\n同步日线数据...")
        result2 = await data_sync_service.sync_daily_for_date()
        print(f"结果: {result2}")

    else:
        print(f"未知类型: {sync_type}")
        print("用法: python sync_data.py [stocks|daily|all]")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
