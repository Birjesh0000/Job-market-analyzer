import axios from 'axios';

// Use environment variable or fallback to relative path for local development
const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
});

// API endpoints
export const api = {
  // Jobs
  getJobs: (page = 1, limit = 20, filters = {}) => {
    const params = new URLSearchParams({
      page,
      limit,
      ...filters,
    });
    return apiClient.get(`/jobs?${params}`);
  },

  // Insights
  getTopSkills: (limit = 10) => 
    apiClient.get(`/insights/top-skills?limit=${limit}`),

  getTopCompanies: (limit = 10) => 
    apiClient.get(`/insights/top-companies?limit=${limit}`),

  getHiringTrends: (groupBy = 'day') => 
    apiClient.get(`/insights/hiring-trends?groupBy=${groupBy}`),

  getStats: () => 
    apiClient.get('/insights/stats'),

  // Scheduler
  getSchedulerStatus: () => 
    apiClient.get('/scheduler/status'),

  startScheduler: () => 
    apiClient.post('/scheduler/start'),

  stopScheduler: () => 
    apiClient.post('/scheduler/stop'),
};

export default api;
