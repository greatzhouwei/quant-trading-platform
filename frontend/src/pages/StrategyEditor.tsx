import { useState, useEffect } from 'react';
import { Row, Col, Card, Button, Input, Select, Table, message, Modal, Form } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, PlayCircleOutlined, CopyOutlined } from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import { strategyApi } from '../api/strategies';
import type { Strategy, StrategyTemplate } from '../api/strategies';

const { TextArea } = Input;
const { Option } = Select;

const defaultStrategyCode = `import backtrader as bt

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
`;

const defaultJQStrategyCode = `import pandas as pd
from jqdata import *

def initialize(context):
    set_benchmark('000300.XSHG')
    log.set_level('order', 'error')
    set_option('use_real_price', True)
    set_slippage(FixedSlippage(0.02))
    g.stock_num = 10
    g.month = context.current_dt.month - 1

def before_trading_start(context):
    pass

def handle_data(context, data):
    hour = context.current_dt.hour
    minute = context.current_dt.minute
    # 每月第一个交易日开盘调仓
    if context.current_dt.month != g.month and hour == 9 and minute == 30:
        # 选股逻辑
        stocks = get_all_securities('stock', context.previous_date).index.tolist()
        # ... 在此处添加选股逻辑
        g.month = context.current_dt.month
`;

// 检测策略类型
function detectStrategyType(code: string): 'backtrader' | 'jqdata' {
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
}

export default function StrategyEditor() {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [selectedStrategy, setSelectedStrategy] = useState<Strategy | null>(null);
  const [code, setCode] = useState(defaultStrategyCode);
  const [isEditing, setIsEditing] = useState(false);
  const [form] = Form.useForm();
  const [templates, setTemplates] = useState<StrategyTemplate[]>([]);
  const [isTemplateModalVisible, setIsTemplateModalVisible] = useState(false);
  const [detectedType, setDetectedType] = useState<'backtrader' | 'jqdata'>('backtrader');

  useEffect(() => {
    loadStrategies();
    loadTemplates();
  }, []);

  const loadStrategies = async () => {
    try {
      const data = await strategyApi.list();
      setStrategies(data);
    } catch (error) {
      message.error('加载策略列表失败');
    }
  };

  const loadTemplates = async () => {
    try {
      const data = await strategyApi.getTemplates();
      setTemplates(data);
    } catch (error) {
      console.error('加载模板失败', error);
    }
  };

  const handleCreate = () => {
    setSelectedStrategy(null);
    setCode(defaultStrategyCode);
    setDetectedType('backtrader');
    setIsEditing(true);
    form.resetFields();
  };

  const handleCreateJQ = () => {
    setSelectedStrategy(null);
    setCode(defaultJQStrategyCode);
    setDetectedType('jqdata');
    setIsEditing(true);
    form.resetFields();
    form.setFieldsValue({
      strategy_type: 'jqdata',
    });
  };

  const handleEdit = (strategy: Strategy) => {
    setSelectedStrategy(strategy);
    setCode(strategy.code);
    const type = detectStrategyType(strategy.code);
    setDetectedType(type);
    setIsEditing(true);
    form.setFieldsValue({
      name: strategy.name,
      description: strategy.description,
      strategy_type: strategy.strategy_type,
    });
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      const strategyData = {
        ...values,
        code,
        parameters: {},
      };

      if (selectedStrategy) {
        await strategyApi.update(selectedStrategy.id, strategyData);
        message.success('策略更新成功');
      } else {
        await strategyApi.create(strategyData);
        message.success('策略创建成功');
      }

      setIsEditing(false);
      loadStrategies();
    } catch (error) {
      message.error('保存失败');
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await strategyApi.delete(id);
      message.success('删除成功');
      loadStrategies();
    } catch (error) {
      message.error('删除失败');
    }
  };

  const handleUseTemplate = (template: StrategyTemplate) => {
    setCode(template.code);
    form.setFieldsValue({
      name: template.name,
      description: template.description,
      strategy_type: template.type,
    });
    setIsTemplateModalVisible(false);
    setIsEditing(true);
  };

  const columns = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '类型',
      dataIndex: 'strategy_type',
      key: 'strategy_type',
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '回测次数',
      dataIndex: 'backtest_count',
      key: 'backtest_count',
    },
    {
      title: '更新时间',
      dataIndex: 'updated_at',
      key: 'updated_at',
      render: (text: string) => new Date(text).toLocaleString(),
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, record: Strategy) => (
        <>
          <Button
            type="link"
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          >
            编辑
          </Button>
          <Button
            type="link"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDelete(record.id)}
          >
            删除
          </Button>
        </>
      ),
    },
  ];

  if (isEditing) {
    return (
      <div>
        <div style={{ marginBottom: 16 }}>
          <Button onClick={() => setIsEditing(false)} style={{ marginRight: 8 }}>
            返回列表
          </Button>
          <Button type="primary" onClick={handleSave}>
            保存策略
          </Button>
          <Button
            icon={<CopyOutlined />}
            onClick={() => setIsTemplateModalVisible(true)}
            style={{ marginLeft: 8 }}
          >
            使用模板
          </Button>
        </div>

        <Form form={form} layout="vertical">
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item
                name="name"
                label="策略名称"
                rules={[{ required: true, message: '请输入策略名称' }]}
              >
                <Input placeholder="请输入策略名称" />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="strategy_type" label="策略类型">
                <Select placeholder="选择策略类型">
                  <Option value="cta">CTA策略</Option>
                  <Option value="mean_reversion">均值回归</Option>
                  <Option value="multi_factor">多因子</Option>
                  <Option value="jqdata">聚宽策略</Option>
                  <Option value="custom">自定义</Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="description" label="策略描述">
            <TextArea rows={2} placeholder="请输入策略描述" />
          </Form.Item>
        </Form>

        <Card
          title={
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span>策略代码</span>
              <span style={{ fontSize: 12, color: '#666' }}>
                检测到的类型: {detectedType === 'jqdata' ? '聚宽策略 (JoinQuant)' : 'Backtrader策略'}
              </span>
            </div>
          }
          style={{ marginTop: 16 }}
        >
          <Editor
            height="500px"
            language="python"
            value={code}
            onChange={(value) => {
              const newCode = value || '';
              setCode(newCode);
              setDetectedType(detectStrategyType(newCode));
            }}
            options={{
              minimap: { enabled: false },
              fontSize: 14,
              lineNumbers: 'on',
              automaticLayout: true,
            }}
          />
        </Card>

        <Modal
          title="选择策略模板"
          open={isTemplateModalVisible}
          onCancel={() => setIsTemplateModalVisible(false)}
          footer={null}
          width={800}
        >
          {templates.map((template) => (
            <Card
              key={template.name}
              title={template.name}
              style={{ marginBottom: 16 }}
              extra={
                <Button type="primary" onClick={() => handleUseTemplate(template)}>
                  使用此模板
                </Button>
              }
            >
              <p>{template.description}</p>
              <pre style={{ background: '#f5f5f5', padding: 12, borderRadius: 4, overflow: 'auto', maxHeight: 200 }}>
                <code>{template.code}</code>
              </pre>
            </Card>
          ))}
        </Modal>
      </div>
    );
  }

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
          新建策略
        </Button>
        <Button style={{ marginLeft: 8 }} onClick={handleCreateJQ}>
          新建聚宽策略
        </Button>
      </div>
      <Table columns={columns} dataSource={strategies} rowKey="id" />
    </div>
  );
}
