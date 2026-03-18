import apiClient from './client';

export type StockInfo = {
  ts_code: string;
  symbol: string;
  name: string;
  area?: string;
  industry?: string;
  market?: string;
  list_date?: string;
};

export type KLineData = {
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  vol: number;
  amount: number;
};

export type SyncStatus = {
  last_sync_date?: string;
  last_sync_time?: string;
  record_count?: number;
  status?: string;
  message?: string;
};

export const marketDataApi = {
  getStocks: async (params?: { industry?: string; market?: string; search?: string; limit?: number }) => {
    const response = await apiClient.get<StockInfo[]>('/market-data/stocks', { params });
    return response.data;
  },

  getStockDetail: async (tsCode: string) => {
    const response = await apiClient.get(`/market-data/stocks/${tsCode}`);
    return response.data;
  },

  getKLine: async (tsCode: string, params?: { start_date?: string; end_date?: string; limit?: number }) => {
    const response = await apiClient.get<KLineData[]>(`/market-data/kline/${tsCode}`, { params });
    return response.data;
  },

  getIndustries: async () => {
    const response = await apiClient.get<string[]>('/market-data/industries');
    return response.data;
  },

  getDataStatus: async () => {
    const response = await apiClient.get<Record<string, SyncStatus>>('/market-data/data-status');
    return response.data;
  },

  syncData: async (type: 'stocks' | 'daily', tradeDate?: string) => {
    const response = await apiClient.post('/market-data/sync', null, {
      params: { sync_type: type, trade_date: tradeDate },
    });
    return response.data;
  },
};
