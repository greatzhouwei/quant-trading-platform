import { Card, Descriptions, Tag, Spin } from 'antd';
import { useState, useEffect } from 'react';
import apiClient from '../api/client';

interface SystemInfo {
  app_name: string;
  version: string;
  platform: string;
  python_version: string;
  debug: boolean;
  database: {
    type: string;
    path: string;
    stats: {
      stocks_count: number;
      kline_daily_count: number;
      strategies_count: number;
      backtest_count: number;
    };
  };
}

export default function SystemSettings() {
  const [systemInfo, setSystemInfo] = useState<SystemInfo | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadSystemInfo();
  }, []);

  const loadSystemInfo = async () => {
    try {
      const response = await apiClient.get('/system/info');
      setSystemInfo(response.data);
    } catch (error) {
      console.error('加载系统信息失败', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <Card style={{ textAlign: 'center', padding: 50 }}>
        <Spin size="large" />
      </Card>
    );
  }

  if (!systemInfo) {
    return <Card>无法加载系统信息</Card>;
  }

  return (
    <div>
      <Card title="系统信息" style={{ marginBottom: 16 }}>
        <Descriptions bordered column={2}>
          <Descriptions.Item label="应用名称">{systemInfo.app_name}</Descriptions.Item>
          <Descriptions.Item label="版本">{systemInfo.version}</Descriptions.Item>
          <Descriptions.Item label="平台">{systemInfo.platform}</Descriptions.Item>
          <Descriptions.Item label="Python版本">{systemInfo.python_version}</Descriptions.Item>
          <Descriptions.Item label="调试模式">
            <Tag color={systemInfo.debug ? 'orange' : 'green'}>
              {systemInfo.debug ? '开启' : '关闭'}
            </Tag>
          </Descriptions.Item>
          <Descriptions.Item label="数据库类型">{systemInfo.database.type}</Descriptions.Item>
        </Descriptions>
      </Card>

      <Card title="数据库统计">
        <Descriptions bordered column={2}>
          <Descriptions.Item label="股票数量">
            {systemInfo.database.stats.stocks_count}
          </Descriptions.Item>
          <Descriptions.Item label="日线数据">
            {systemInfo.database.stats.kline_daily_count}
          </Descriptions.Item>
          <Descriptions.Item label="策略数量">
            {systemInfo.database.stats.strategies_count}
          </Descriptions.Item>
          <Descriptions.Item label="回测记录">
            {systemInfo.database.stats.backtest_count}
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Card title="关于" style={{ marginTop: 16 }}>
        <p>量化交易回测系统 - 基于中国A股的量化策略回测平台</p>
        <p>技术栈：React + TypeScript + FastAPI + DuckDB + backtrader</p>
        <p>数据源：Tushare Pro</p>
      </Card>
    </div>
  );
}
