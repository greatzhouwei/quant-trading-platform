"""
启动脚本 - 初始化数据库并运行服务
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.db.session import init_db
from app.main import app
import uvicorn
from app.core.config import settings

if __name__ == "__main__":
    print("=" * 50)
    print("量化交易回测系统 - 后端服务")
    print("=" * 50)

    # 初始化数据库
    print("\n[1/2] 正在初始化数据库...")
    try:
        init_db()
        print("[OK] 数据库初始化完成")
    except Exception as e:
        print(f"[ERROR] 数据库初始化失败: {e}")
        sys.exit(1)

    # 启动服务
    print(f"\n[2/2] 正在启动API服务...")
    print(f"   地址: http://{settings.API_HOST}:{settings.API_PORT}")
    print(f"   文档: http://{settings.API_HOST}:{settings.API_PORT}/docs")
    print("\n按 Ctrl+C 停止服务\n")

    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="info"
    )
