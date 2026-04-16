const express = require('express');
const cors = require('cors');
require('dotenv').config({ path: '../.env' });

const app = express();
const PORT = process.env.BACKEND_PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'Backend is running' });
});

// Placeholder route
app.get('/api/jobs', (req, res) => {
  res.json({ message: 'Jobs endpoint - Coming soon' });
});

app.get('/api/insights/top-skills', (req, res) => {
  res.json({ message: 'Top skills endpoint - Coming soon' });
});

app.get('/api/insights/top-companies', (req, res) => {
  res.json({ message: 'Top companies endpoint - Coming soon' });
});

app.get('/api/insights/hiring-trends', (req, res) => {
  res.json({ message: 'Hiring trends endpoint - Coming soon' });
});

// Error handling
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Internal Server Error' });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Not Found' });
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
