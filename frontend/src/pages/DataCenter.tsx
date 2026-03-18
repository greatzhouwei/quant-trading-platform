import { useState, useEffect } from 'react';
import { Card, Table, Button, Tag, message, Progress, Statistic, Row, Col, Input } from 'antd';
import { SyncOutlined, DatabaseOutlined } from '@ant-design/icons';
import { marketDataApi } from '../api/marketData';
import type { StockInfo, SyncStatus } from '../api/marketData';

export default function DataCenter() {
  const [stocks, setStocks] = useState<StockInfo[]>([]);
  const [syncStatus, setSyncStatus] = useState<Record<string, SyncStatus>>({});
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [searchText, setSearchText] = useState('');

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      const [stocksData, statusData] = await Promise.all([
        marketDataApi.getStocks({ limit: 1000 }),
        marketDataApi.getDataStatus(),
      ]);
      setStocks(stocksData);
      setSyncStatus(statusData);
    } catch (error) {
      message.error('加载数据失败');
    } finally {
      setLoading(false);
    }
  };

  const handleSyncStocks = async () => {
    try {
      setSyncing(true);
      const result = await marketDataApi.syncData('stocks');
      if (result.status === 'success') {
        message.success(`股票列表同步完成，共 ${result.record_count} 只`);
        loadData();
      } else {
        message.error(result.message);
      }
    } catch (error) {
      message.error('同步失败');
    } finally {
      setSyncing(false);
    }
  };

  const handleSyncDaily = async () => {
    try {
      setSyncing(true);
      const result = await marketDataApi.syncData('daily');
      if (result.status === 'success') {
        message.success(`日线数据同步完成，共 ${result.record_count} 条`);
        loadData();
      } else {
        message.error(result.message);
      }
    } catch (error) {
      message.error('同步失败');
    } finally {
      setSyncing(false);
    }
  };

  const columns = [
    {
      title: '代码',
      dataIndex: 'ts_code',
      key: 'ts_code',
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '行业',
      dataIndex: 'industry',
      key: 'industry',
    },
    {
      title: '市场',
      dataIndex: 'market',
      key: 'market',
    },
    {
      title: '上市日期',
      dataIndex: 'list_date',
      key: 'list_date',
    },
  ];

  const filteredStocks = stocks.filter(
    (s) =>
      s.name.includes(searchText) ||
      s.ts_code.toLowerCase().includes(searchText.toLowerCase())
  );

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="股票总数"
              value={stocks.length}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="股票列表更新"
              value={syncStatus.stocks?.last_sync_date || '-'}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="日线数据更新"
              value={syncStatus.kline_daily?.last_sync_date || '-'}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <div style={{ marginBottom: 8 }}>数据同步</div>
            <Button
              type="primary"
              icon={<SyncOutlined spin={syncing} />}
              loading={syncing}
              onClick={handleSyncStocks}
              style={{ marginRight: 8 }}
            >
              同步股票
            </Button>
            <Button
              icon={<SyncOutlined spin={syncing} />}
              loading={syncing}
              onClick={handleSyncDaily}
            >
              同步日线
            </Button>
          </Card>
        </Col>
      </Row>

      <Card
        title="股票列表"
        extra={
          <Input.Search
            placeholder="搜索股票名称或代码"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 250 }}
          />
        }
      >
        <Table
          columns={columns}
          dataSource={filteredStocks}
          rowKey="ts_code"
          loading={loading}
          pagination={{ pageSize: 20 }}
          scroll={{ y: 500 }}
        />
      </Card>
    </div>
  );
}
