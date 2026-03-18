import { useState, useEffect, useRef } from 'react';
import { Card, Form, Select, DatePicker, InputNumber, Button, Table, message, Row, Col, Statistic, Spin, Space } from 'antd';
import { PlayCircleOutlined, HistoryOutlined } from '@ant-design/icons';
import type { Dayjs } from 'dayjs';
import dayjs from 'dayjs';
import { createChart, type IChartApi, LineSeries } from 'lightweight-charts';
import type { Time } from 'lightweight-charts';
import { strategyApi } from '../api/strategies';
import type { Strategy } from '../api/strategies';
import { backtestApi } from '../api/backtest';
import type { BacktestConfig, BacktestResult } from '../api/backtest';
import { marketDataApi } from '../api/marketData';
import type { StockInfo } from '../api/marketData';

const { Option } = Select;
const { RangePicker } = DatePicker;

export default function BacktestPage() {
  const [form] = Form.useForm();
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [stocks, setStocks] = useState<StockInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [selectedStrategyType, setSelectedStrategyType] = useState<string>('');
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  // 检测策略类型
  const detectStrategyType = (code: string): 'jqdata' | 'backtrader' => {
    const jqPatterns = [
      /def\s+initialize\s*\(\s*context\s*\)/,
      /def\s+handle_data\s*\(\s*context\s*,\s*data\s*\)/,
      /from\s+jqdata\s+import/,
      /set_benchmark\s*\(/,
      /get_all_securities\s*\(/,
      /get_fundamentals\s*\(/,
      /get_current_data\s*\(/,
    ];

    for (const pattern of jqPatterns) {
      if (pattern.test(code)) {
        return 'jqdata';
      }
    }
    return 'backtrader';
  };

  useEffect(() => {
    loadStrategies();
    loadStocks();
    loadHistory();
  }, []);

  useEffect(() => {
    if (result && chartContainerRef.current) {
      renderChart();
    }
  }, [result]);

  const loadStrategies = async () => {
    try {
      const data = await strategyApi.list();
      setStrategies(data);
    } catch (error) {
      message.error('加载策略失败');
    }
  };

  const loadStocks = async () => {
    try {
      const data = await marketDataApi.getStocks({ limit: 100 });
      setStocks(data);
    } catch (error) {
      message.error('加载股票列表失败');
    }
  };

  const loadHistory = async () => {
    try {
      const data = await backtestApi.getHistory({ limit: 10 });
      setHistory(data);
    } catch (error) {
      console.error('加载历史失败', error);
    }
  };

  const loadBacktestDetail = async (backtestId: string) => {
    try {
      setLoading(true);
      const data = await backtestApi.getResult(backtestId);
      setResult(data);
      message.success('加载历史回测结果成功');
    } catch (error) {
      message.error('加载历史回测结果失败');
    } finally {
      setLoading(false);
    }
  };

  const renderChart = () => {
    if (!chartContainerRef.current || !result) return;

    if (chartRef.current) {
      chartRef.current.remove();
    }

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: '#ffffff' },
        textColor: '#333',
      },
      grid: {
        vertLines: { color: '#f0f0f0' },
        horzLines: { color: '#f0f0f0' },
      },
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        borderColor: '#cccccc',
      },
      timeScale: {
        borderColor: '#cccccc',
        timeVisible: true,
      },
      height: 350,
    });

    // 策略收益曲线（蓝色）- 使用百分比
    const lineSeries = chart.addSeries(LineSeries, {
      color: '#1890ff',
      lineWidth: 2,
      title: '策略收益',
    });

    const data = result.equity_curve.map((point) => ({
      time: new Date(point.date).getTime() / 1000 as Time,
      value: (point.value - 1) * 100, // 转换为百分比收益率
    }));

    lineSeries.setData(data);

    // 基准曲线（橙色虚线）- 使用百分比
    if (result.benchmark_curve && result.benchmark_curve.length > 0) {
      const benchmarkSeries = chart.addSeries(LineSeries, {
        color: '#ff9800',
        lineWidth: 1,
        lineStyle: 2, // 虚线
        title: '基准指数',
      });

      const benchmarkData = result.benchmark_curve.map((point) => ({
        time: new Date(point.date).getTime() / 1000 as Time,
        value: (point.value - 1) * 100, // 转换为百分比收益率
      }));

      benchmarkSeries.setData(benchmarkData);
    }

    chart.timeScale().fitContent();
    chartRef.current = chart;
  };

  const handleRunBacktest = async (values: any) => {
    try {
      setLoading(true);
      const [startDate, endDate] = values.date_range;

      const config: BacktestConfig = {
        strategy_id: values.strategy_id,
        symbol: values.symbol,
        start_date: startDate.format('YYYY-MM-DD'),
        end_date: endDate.format('YYYY-MM-DD'),
        timeframe: values.timeframe || '1d',
        initial_cash: values.initial_cash || 100000,
        commission: values.commission || 0.00025,
        slippage: values.slippage || 0.001,
        strategy_type: selectedStrategyType,
      };

      const response = await backtestApi.run(config);

      if (response.result && response.status === 'completed') {
        setResult(response.result);
        message.success('回测完成');
        loadHistory();
      } else if (response.status === 'failed') {
        message.error('回测执行失败: ' + (response.result?.error_message || '未知错误'));
      } else {
        message.error('回测执行失败');
      }
    } catch (error) {
      message.error('回测执行失败');
    } finally {
      setLoading(false);
    }
  };

  const tradeColumns = [
    { title: '时间', dataIndex: 'datetime', key: 'datetime' },
    { title: '类型', dataIndex: 'type', key: 'type' },
    { title: '价格', dataIndex: 'price', key: 'price' },
    { title: '数量', dataIndex: 'size', key: 'size' },
    { title: '收益', dataIndex: 'pnl', key: 'pnl', render: (val: number) => val?.toFixed(2) },
  ];

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      {/* 第一行：回测配置 */}
      <Card title="回测配置" style={{ marginBottom: 16 }}>
        <Form
          form={form}
          layout="inline"
          onFinish={handleRunBacktest}
          style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', alignItems: 'flex-start' }}
        >
          <Form.Item
            name="strategy_id"
            label="选择策略"
            rules={[{ required: true, message: '请选择策略' }]}
            style={{ marginBottom: 8 }}
          >
            <Select
              placeholder="选择策略"
              style={{ width: 160 }}
              onChange={(value) => {
                const strategy = strategies.find(s => s.id === value);
                if (strategy) {
                  const type = detectStrategyType(strategy.code);
                  setSelectedStrategyType(type);
                  if (type === 'jqdata') {
                    form.setFieldsValue({ symbol: '000300.SH' });
                  }
                }
              }}
            >
              {strategies.map((s) => (
                <Option key={s.id} value={s.id}>{s.name}</Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            name="symbol"
            label={selectedStrategyType === 'jqdata' ? "基准指数" : "选择股票"}
            rules={[{ required: selectedStrategyType !== 'jqdata', message: '请选择' }]}
            style={{ marginBottom: 8 }}
          >
            <Select
              placeholder={selectedStrategyType === 'jqdata' ? "选择基准指数" : "选择股票"}
              style={{ width: 160 }}
              showSearch
              allowClear={selectedStrategyType === 'jqdata'}
            >
              {selectedStrategyType === 'jqdata' ? (
                <>
                  <Option value="000300.SH">沪深300</Option>
                  <Option value="000905.SH">中证500</Option>
                  <Option value="000001.SH">上证指数</Option>
                  <Option value="399001.SZ">深证成指</Option>
                  <Option value="399006.SZ">创业板指</Option>
                </>
              ) : (
                stocks.map((s) => (
                  <Option key={s.ts_code} value={s.ts_code}>
                    {s.name}
                  </Option>
                ))
              )}
            </Select>
          </Form.Item>

          <Form.Item
            name="date_range"
            label="回测区间"
            rules={[{ required: true, message: '请选择回测区间' }]}
            style={{ marginBottom: 8 }}
          >
            <RangePicker style={{ width: 240 }} />
          </Form.Item>

          <Form.Item name="timeframe" label="时间周期" initialValue="1d" style={{ marginBottom: 8 }}>
            <Select style={{ width: 100 }}>
              <Option value="1d">日线</Option>
              <Option value="1w">周线</Option>
              <Option value="1m">月线</Option>
            </Select>
          </Form.Item>

          <Form.Item name="initial_cash" label="初始资金" initialValue={100000} style={{ marginBottom: 8 }}>
            <InputNumber
              style={{ width: 140 }}
              formatter={(value) => `¥ ${value}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
              parser={(value) => value!.replace(/\$\s?|(,*)/g, '')}
            />
          </Form.Item>

          <Form.Item style={{ marginBottom: 8, marginTop: 29 }}>
            <Button
              type="primary"
              htmlType="submit"
              icon={<PlayCircleOutlined />}
              loading={loading}
            >
              开始回测
            </Button>
          </Form.Item>
        </Form>

        {selectedStrategyType === 'jqdata' && (
          <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
            提示：聚宽策略会自动选股，此处仅用于基准对比
          </div>
        )}
      </Card>

      {/* 第二行：回测结果 */}
      {loading ? (
        <Card style={{ textAlign: 'center', padding: 80 }}>
          <Spin size="large" />
          <p style={{ marginTop: 16 }}>正在执行回测...</p>
        </Card>
      ) : result ? (
        <>
          {/* 收益概述 - 第一行 */}
          <Card title="收益概述" style={{ marginBottom: 16 }}>
            <Row gutter={[16, 16]}>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>策略收益</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold', color: (result.metrics?.total_return || 0) >= 0 ? '#cf1322' : '#3f8600' }}>
                    {(result.metrics?.total_return || 0).toFixed(2)}%
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>策略年化收益</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold', color: (result.metrics?.annual_return || 0) >= 0 ? '#cf1322' : '#3f8600' }}>
                    {(result.metrics?.annual_return || 0).toFixed(2)}%
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>超额收益</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold', color: (result.metrics?.excess_return || 0) >= 0 ? '#cf1322' : '#3f8600' }}>
                    {(result.metrics?.excess_return || 0).toFixed(2)}%
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>基准收益</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold', color: (result.metrics?.benchmark_return || 0) >= 0 ? '#cf1322' : '#3f8600' }}>
                    {(result.metrics?.benchmark_return || 0).toFixed(2)}%
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>阿尔法</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold' }}>
                    {(result.metrics?.alpha || 0).toFixed(3)}
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>贝塔</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold' }}>
                    {(result.metrics?.beta || 0).toFixed(3)}
                  </div>
                </div>
              </Col>
            </Row>
            <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>夏普比率</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold' }}>
                    {(result.metrics?.sharpe_ratio || 0).toFixed(3)}
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>胜率</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold' }}>
                    {(result.metrics?.win_rate || 0).toFixed(1)}%
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>盈亏比</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold' }}>
                    {(result.metrics?.profit_factor || 0).toFixed(3)}
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>最大回撤</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold', color: '#3f8600' }}>
                    {(result.metrics?.max_drawdown || 0).toFixed(2)}%
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>索提诺比率</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold' }}>
                    {(result.metrics?.sortino_ratio || 0).toFixed(3)}
                  </div>
                </div>
              </Col>
              <Col span={4}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 12, color: '#666' }}>信息比率</div>
                  <div style={{ fontSize: 20, fontWeight: 'bold' }}>
                    {(result.metrics?.information_ratio || 0).toFixed(3)}
                  </div>
                </div>
              </Col>
            </Row>

            {/* 第二行 - 详细指标 */}
            <div style={{ marginTop: 24, paddingTop: 16, borderTop: '1px solid #f0f0f0' }}>
              <Row gutter={[16, 16]}>
                <Col span={4}>
                  <div style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: 12, color: '#666' }}>日均超额收益</div>
                    <div style={{ fontSize: 16, fontWeight: 'bold' }}>
                      {(result.metrics?.daily_excess_return || 0).toFixed(3)}%
                    </div>
                  </div>
                </Col>
                <Col span={4}>
                  <div style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: 12, color: '#666' }}>超额收益最大回撤</div>
                    <div style={{ fontSize: 16, fontWeight: 'bold' }}>
                      {(result.metrics?.excess_drawdown || 0).toFixed(2)}%
                    </div>
                  </div>
                </Col>
                <Col span={4}>
                  <div style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: 12, color: '#666' }}>日胜率</div>
                    <div style={{ fontSize: 16, fontWeight: 'bold' }}>
                      {(result.metrics?.daily_win_rate || 0).toFixed(1)}%
                    </div>
                  </div>
                </Col>
                <Col span={4}>
                  <div style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: 12, color: '#666' }}>盈利/亏损次数</div>
                    <div style={{ fontSize: 16, fontWeight: 'bold' }}>
                      {result.metrics?.winning_trades || 0}/{result.metrics?.losing_trades || 0}
                    </div>
                  </div>
                </Col>
                <Col span={4}>
                  <div style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: 12, color: '#666' }}>策略波动率</div>
                    <div style={{ fontSize: 16, fontWeight: 'bold' }}>
                      {(result.metrics?.volatility || 0).toFixed(3)}
                    </div>
                  </div>
                </Col>
                <Col span={4}>
                  <div style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: 12, color: '#666' }}>基准波动率</div>
                    <div style={{ fontSize: 16, fontWeight: 'bold' }}>
                      {(result.metrics?.benchmark_volatility || 0).toFixed(3)}
                    </div>
                  </div>
                </Col>
              </Row>
            </div>
          </Card>

          {/* 收益曲线和交易记录 */}
          <Row gutter={[16, 16]}>
            <Col span={16}>
              <Card title="收益曲线">
                <div ref={chartContainerRef} />
              </Card>
            </Col>
            <Col span={8}>
              <Card title="交易记录" style={{ height: '100%' }}>
                <Table
                  dataSource={result.trades}
                  columns={tradeColumns}
                  rowKey={(record, index) => index?.toString() || '0'}
                  pagination={{ pageSize: 5 }}
                  size="small"
                  scroll={{ y: 280 }}
                />
              </Card>
            </Col>
          </Row>
        </>
      ) : (
        <Card style={{ textAlign: 'center', padding: 80, color: '#999' }}>
          <HistoryOutlined style={{ fontSize: 48, marginBottom: 16 }} />
          <p>配置回测参数并点击"开始回测"</p>
          <p style={{ fontSize: 12, marginTop: 8 }}>
            提示: 可在下方回测历史中选择查看历史结果
          </p>
        </Card>
      )}

      {/* 第三行：回测历史 */}
      <Card title="回测历史" style={{ marginTop: 16 }}>
        <Table
          dataSource={history}
          rowKey="id"
          pagination={{ pageSize: 5 }}
          size="small"
          columns={[
            { title: '策略', dataIndex: 'strategy_name', key: 'strategy_name' },
            { title: '收益', dataIndex: 'total_return', key: 'total_return', render: (v: number) => `${(v ?? 0).toFixed(2)}%` },
            { title: '状态', dataIndex: 'status', key: 'status' },
            { title: '时间', dataIndex: 'created_at', key: 'created_at', render: (v: string) => v?.slice(0, 19) },
            {
              title: '操作',
              key: 'action',
              render: (_, record) => (
                <Button type="link" size="small" onClick={() => loadBacktestDetail(record.id)}>
                  查看
                </Button>
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}
