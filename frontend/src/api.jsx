import axios from 'axios';

// Determine API URL based on environment
let API_BASE_URL;

if (import.meta.env.VITE_API_URL) {
  // Use env variable if set
  API_BASE_URL = import.meta.env.VITE_API_URL;
} else if (typeof window !== 'undefined' && window.location.hostname !== 'localhost') {
  // Production: use full URL to Render backend
  API_BASE_URL = 'https://job-market-analyzer-vejc.onrender.com/api';
} else {
  // Local development: use relative path
  API_BASE_URL = '/api';
}

console.log('API_BASE_URL:', API_BASE_URL);

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
