# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个基于中国A股的量化交易回测系统，类似聚宽平台，支持：
- Python策略代码编辑（Monaco Editor）
- A股历史数据回测（backtrader引擎）
- 回测结果可视化（收益曲线、交易记录）
- 股票数据管理（Tushare数据源）

**GitHub仓库**: https://github.com/greatzhouwei/quant-trading-platform

## 技术栈

- **前端**: React + TypeScript + Vite + Ant Design + lightweight-charts + Monaco Editor
- **后端**: Python + FastAPI + backtrader + DuckDB + tushare
- **数据库**: DuckDB (嵌入式分析型数据库)
- **数据源**: Tushare Pro (需要Token)

## 项目结构

```
quant-trading-platform/
├── frontend/               # React前端
│   ├── src/
│   │   ├── api/           # API客户端 (axios封装)
│   │   ├── pages/         # 页面组件
│   │   │   ├── StrategyEditor.tsx   # 策略编辑器
│   │   │   ├── BacktestPage.tsx     # 回测页面
│   │   │   └── DataCenter.tsx       # 数据中心
│   │   └── types/         # TypeScript类型定义
│   └── package.json
├── backend/               # Python后端
│   ├── app/
│   │   ├── api/v1/endpoints/   # API路由
│   │   │   ├── strategies.py   # 策略CRUD
│   │   │   ├── backtest.py     # 回测执行
│   │   │   └── market_data.py  # 市场数据
│   │   ├── engine/        # 回测引擎封装
│   │   ├── db/session.py  # DuckDB连接管理
│   │   └── services/      # 业务逻辑
│   ├── data/              # DuckDB数据库文件
│   └── startup.py         # 启动脚本
└── CLAUDE.md              # 本文件
```

## 快速启动

### 1. 启动后端

```bash
cd backend
source venv/Scripts/activate  # Windows
# source venv/bin/activate    # Linux/Mac

# 启动服务（不要使用--reload，会导致DuckDB文件被占用）
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

后端服务将在 http://localhost:8000 运行
API文档: http://localhost:8000/docs

### 2. 启动前端

```bash
cd frontend
npm install  # 首次运行
npm run dev
```

前端开发服务器将在 http://localhost:5173 运行

## 端口配置

| 服务 | 端口 | 说明 |
|------|------|------|
| 后端API | 8000 | FastAPI服务 |
| 前端开发 | 5173 | Vite开发服务器 |
| CORS | 5173-5180 | 后端已配置允许多个前端端口 |

## 后端开发命令

```bash
cd backend
source venv/Scripts/activate

# 安装依赖
pip install -r requirements.txt

# 初始化数据库
python -c "from app.db.session import init_db; init_db()"

# 同步股票列表
python scripts/sync_data.py stocks

# 同步日线数据
python scripts/sync_data.py daily

# 同步所有数据
python scripts/sync_data.py all
```

## 配置

### 后端配置 (.env)

```bash
TUSHARE_TOKEN=your_token_here
DUCKDB_PATH=data/quant_trading.duckdb
API_PORT=8000
DEBUG=false
```

### 获取Tushare Token

1. 访问 https://tushare.pro
2. 注册并登录
3. 在个人中心获取Token

## API端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/v1/strategies` | GET/POST | 策略列表/创建 |
| `/api/v1/strategies/{id}` | PUT/DELETE | 策略更新/删除 |
| `/api/v1/strategies/templates` | GET | 获取策略模板 |
| `/api/v1/backtest/run` | POST | 执行回测 |
| `/api/v1/backtest/history` | GET | 回测历史 |
| `/api/v1/market-data/stocks` | GET | 股票列表 |
| `/api/v1/market-data/kline/{code}` | GET | K线数据 |
| `/health` | GET | 健康检查 |

## 关键文件

- `backend/app/api/v1/endpoints/strategies.py` - 策略管理API（注意：parameters字段需要JSON解析）
- `backend/app/engine/backtrader_wrapper.py` - 回测引擎封装
- `backend/app/services/data_sync_service.py` - Tushare数据同步
- `backend/app/db/session.py` - DuckDB连接管理（线程本地存储）
- `frontend/src/pages/StrategyEditor.tsx` - 策略编辑器（Monaco Editor）
- `frontend/src/pages/BacktestPage.tsx` - 回测页面（lightweight-charts图表）

## 开发注意事项

### 1. DuckDB并发限制
- **不要**使用 `--reload` 模式启动uvicorn，会导致DuckDB文件被占用错误
- 正确命令: `uvicorn app.main:app --host 0.0.0.0 --port 8000`

### 2. TypeScript类型导入
前端必须使用 `import type` 避免运行时错误：
```typescript
// 正确
import type { Strategy } from '../api/strategies';
import type { IChartApi } from 'lightweight-charts';

// 错误（会导致运行时错误）
import { Strategy } from '../api/strategies';
```

### 3. CORS配置
后端已配置允许以下前端端口：
- http://localhost:5173-5180
- http://127.0.0.1:5173-5180

### 4. 后端Pydantic模型
策略的 `parameters` 字段在DuckDB中存储为JSON字符串，API响应时自动解析为Python dict。

### 5. 依赖版本要求
```
Python >= 3.8
FastAPI >= 0.100.0
DuckDB >= 0.9.0
backtrader >= 1.9.76
Node.js >= 18
```

## 已知问题与解决方案

| 问题 | 原因 | 解决 |
|------|------|------|
| `加载策略列表失败` | parameters字段类型不匹配 | 已修复：添加json解析 |
| `DuckDB文件被占用` | --reload模式多进程冲突 | 启动时不加--reload |
| `CORS错误` | 前端端口不在允许列表 | 使用5173-5180端口 |
| `TypeScript导入错误` | 类型导入方式错误 | 使用 `import type` |

## 策略代码示例

```python
import backtrader as bt

class MyStrategy(bt.Strategy):
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
```

## Git提交规范

```bash
# 添加所有更改
git add .

# 提交
git commit -m "描述更改内容"

# 推送到GitHub
git push origin master
```
