import apiClient from './client';

export type BacktestConfig = {
  strategy_id: string;
  symbol?: string;
  start_date: string;
  end_date: string;
  timeframe?: string;
  initial_cash?: number;
  commission?: number;
  slippage?: number;
  parameters?: Record<string, any>;
  strategy_type?: string;
};

export type BacktestMetrics = {
  total_return: number;
  annual_return: number;
  max_drawdown: number;
  max_drawdown_duration: number;
  sharpe_ratio: number;
  win_rate: number;
  profit_factor: number;
  total_trades: number;
  winning_trades: number;
  losing_trades: number;
};

export type TradeRecord = {
  datetime: string;
  type: string;
  price: number;
  size: number;
  value: number;
  commission: number;
  pnl?: number;
};

export type BacktestResult = {
  id: string;
  config: BacktestConfig;
  status: string;
  metrics?: BacktestMetrics;
  equity_curve: { date: string; value: number }[];
  trades: TradeRecord[];
  drawdown_curve: { date: string; value: number }[];
  monthly_returns: { year: number; month: number; return: number }[];
  logs: string[];
  error_message?: string;
  created_at: string;
  completed_at?: string;
  execution_time?: number;
};

export const backtestApi = {
  getHistory: async (params?: { strategy_id?: string; limit?: number }) => {
    const response = await apiClient.get<BacktestResult[]>('/backtest/history', { params });
    return response.data;
  },

  run: async (config: BacktestConfig) => {
    const response = await apiClient.post<{ id: string; status: string; result?: BacktestResult }>(
      '/backtest/run',
      config
    );
    return response.data;
  },

  getResult: async (id: string) => {
    const response = await apiClient.get<BacktestResult>(`/backtest/${id}`);
    return response.data;
  },

  delete: async (id: string) => {
    await apiClient.delete(`/backtest/${id}`);
  },
};
