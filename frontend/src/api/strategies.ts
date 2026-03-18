import apiClient from './client';

export interface Strategy {
  id: string;
  name: string;
  description?: string;
  strategy_type: string;
  code: string;
  parameters: Record<string, any>;
  created_at: string;
  updated_at: string;
  last_backtest_at?: string;
  backtest_count: number;
}

export interface StrategyTemplate {
  name: string;
  type: string;
  description: string;
  code: string;
}

export const strategyApi = {
  list: async (params?: { skip?: number; limit?: number; strategy_type?: string }) => {
    const response = await apiClient.get<Strategy[]>('/strategies', { params });
    return response.data;
  },

  get: async (id: string) => {
    const response = await apiClient.get<Strategy>(`/strategies/${id}`);
    return response.data;
  },

  create: async (data: Omit<Strategy, 'id' | 'created_at' | 'updated_at' | 'backtest_count'>) => {
    const response = await apiClient.post<Strategy>('/strategies', data);
    return response.data;
  },

  update: async (id: string, data: Partial<Strategy>) => {
    const response = await apiClient.put<Strategy>(`/strategies/${id}`, data);
    return response.data;
  },

  delete: async (id: string) => {
    await apiClient.delete(`/strategies/${id}`);
  },

  getTemplates: async () => {
    const response = await apiClient.get<StrategyTemplate[]>('/strategies/templates');
    return response.data;
  },

  validate: async (id: string) => {
    const response = await apiClient.post<{ valid: boolean; message: string; line?: number; column?: number }>(
      `/strategies/${id}/validate`
    );
    return response.data;
  },
};
