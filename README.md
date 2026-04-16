# Job Market Analyzer

Real-time job market insights and trends analysis platform.

## Live Demo

- **Frontend**: https://job-market-analyzer.vercel.app
- **Backend API**: https://job-market-analyzer-vejc.onrender.com

## Features

- Real-time job market data collection and analysis
- Job listings by position, company, and skills
- Hiring trend analysis (daily, weekly, monthly)
- Market insights and statistics
- Interactive dashboard with data visualizations

## Tech Stack

- **Frontend**: React 18 + Vite
- **Backend**: Node.js + Express
- **Database**: MongoDB Atlas
- **Data Collection**: Python web scraper with APScheduler
- **Deployment**: Vercel (frontend), Render (backend)

## Local Setup

### Prerequisites

- Node.js 18+
- Python 3.8+
- MongoDB connection string

### Installation

1. **Clone repository**
   ```bash
   git clone https://github.com/Birjesh0000/Job-market-analyzer.git
   cd Job-market-analyzer
   ```

2. **Backend setup**
   ```bash
   cd backend
   npm install
   ```

3. **Frontend setup**
   ```bash
   cd frontend
   npm install
   ```

4. **Environment variables**
   - Create `.env` in root directory
   - Add: `MONGODB_URI`, `DB_NAME`, `BACKEND_PORT`

### Running Locally

**Backend** (from backend/ directory):
```bash
node server.js
```

**Frontend** (from frontend/ directory):
```bash
npm run dev
```

**Scraper** (from root directory):
```bash
python scraper.py
```

## API Endpoints

- `GET /api/health` - API health check
- `GET /api/jobs` - Get job listings
- `GET /api/skills` - Get trending skills
- `GET /api/companies` - Get company data
- `GET /api/trends` - Get hiring trends
- `GET /api/stats` - Get market statistics

## Project Structure

```
Job-market-analyzer/
├── backend/          # Express API server
├── frontend/         # React + Vite dashboard
├── scraper.py        # Data collection script
├── cleaner.py        # Data cleaning pipeline
└── scheduler.py      # Automated data collection
```

## License

MIT
