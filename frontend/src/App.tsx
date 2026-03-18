import { Layout, Menu, theme } from 'antd';
import {
  LineChartOutlined,
  CodeOutlined,
  DatabaseOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useState } from 'react';
import StrategyEditor from './pages/StrategyEditor';
import BacktestPage from './pages/BacktestPage';
import DataCenter from './pages/DataCenter';
import SystemSettings from './pages/SystemSettings';

const { Header, Sider, Content } = Layout;

type MenuKey = 'strategy' | 'backtest' | 'data' | 'settings';

function App() {
  const [selectedKey, setSelectedKey] = useState<MenuKey>('strategy');
  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken();

  const menuItems = [
    {
      key: 'strategy',
      icon: <CodeOutlined />,
      label: '策略编辑器',
    },
    {
      key: 'backtest',
      icon: <LineChartOutlined />,
      label: '回测中心',
    },
    {
      key: 'data',
      icon: <DatabaseOutlined />,
      label: '数据中心',
    },
    {
      key: 'settings',
      icon: <SettingOutlined />,
      label: '系统设置',
    },
  ];

  const renderContent = () => {
    switch (selectedKey) {
      case 'strategy':
        return <StrategyEditor />;
      case 'backtest':
        return <BacktestPage />;
      case 'data':
        return <DataCenter />;
      case 'settings':
        return <SystemSettings />;
      default:
        return <StrategyEditor />;
    }
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header style={{ display: 'flex', alignItems: 'center', background: '#001529' }}>
        <div style={{ color: '#fff', fontSize: 18, fontWeight: 'bold', marginRight: 24 }}>
          量化交易回测系统
        </div>
      </Header>
      <Layout>
        <Sider width={200} style={{ background: colorBgContainer }}>
          <Menu
            mode="inline"
            selectedKeys={[selectedKey]}
            style={{ height: '100%', borderRight: 0 }}
            items={menuItems}
            onClick={({ key }) => setSelectedKey(key as MenuKey)}
          />
        </Sider>
        <Layout style={{ padding: '24px' }}>
          <Content
            style={{
              background: colorBgContainer,
              padding: 24,
              margin: 0,
              borderRadius: borderRadiusLG,
              minHeight: 280,
            }}
          >
            {renderContent()}
          </Content>
        </Layout>
      </Layout>
    </Layout>
  );
}

export default App;
