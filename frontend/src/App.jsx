import React, { useState, useEffect } from 'react';
import api from './api';
import Header from './components/Header';
import StatsCards from './components/StatsCards';
import ChartsSection from './components/ChartsSection';
import JobsSection from './components/JobsSection';
import SkillsSection from './components/SkillsSection';
import './App.css';

function App() {
  const [stats, setStats] = useState(null);
  const [topSkills, setTopSkills] = useState([]);
  const [topCompanies, setTopCompanies] = useState([]);
  const [hiringTrends, setHiringTrends] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [trendGrouping, setTrendGrouping] = useState('day');

  // Fetch all dashboard data
  useEffect(() => {
    fetchDashboardData();
  }, [currentPage, trendGrouping]);

  const fetchDashboardData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [statsRes, skillsRes, companiesRes, trendsRes, jobsRes] = await Promise.all([
        api.getStats(),
        api.getTopSkills(10),
        api.getTopCompanies(10),
        api.getHiringTrends(trendGrouping),
        api.getJobs(currentPage, 10),
      ]);

      setStats(statsRes.data.data);
      setTopSkills(skillsRes.data.data);
      setTopCompanies(companiesRes.data.data);
      setHiringTrends(trendsRes.data.data);
      setJobs(jobsRes.data.data);
    } catch (err) {
      console.error('Error fetching dashboard data:', err);
      setError(err.response?.data?.error || 'Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="app">
        <Header />
        <div className="loading-container">
          <div className="spinner"></div>
          <p>Loading dashboard data...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <Header />
      {error && <div className="error-banner">{error}</div>}
      
      <main className="dashboard">
        <section className="dashboard-section">
          <h1 className="section-title">Market Overview</h1>
          {stats && <StatsCards stats={stats} />}
        </section>

        <section className="dashboard-section">
          <h1 className="section-title">Market Insights</h1>
          <ChartsSection
            trends={hiringTrends}
            trendGrouping={trendGrouping}
            onTrendGroupingChange={setTrendGrouping}
          />
        </section>

        <section className="dashboard-section">
          <div className="section-row">
            <div className="section-column">
              <h2 className="subsection-title">Top Skills in Demand</h2>
              <SkillsSection skills={topSkills} />
            </div>
            <div className="section-column">
              <h2 className="subsection-title">Top Hiring Companies</h2>
              <div className="companies-list">
                {topCompanies.map((company, idx) => (
                  <div key={idx} className="company-item">
                    <span className="company-rank">{idx + 1}</span>
                    <div className="company-info">
                      <p className="company-name">{company.company}</p>
                      <p className="company-jobs">{company.job_count} jobs</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section className="dashboard-section">
          <h1 className="section-title">Latest Jobs</h1>
          <JobsSection
            jobs={jobs}
            page={currentPage}
            onPageChange={setCurrentPage}
            onRefresh={fetchDashboardData}
          />
        </section>
      </main>
    </div>
  );
}

export default App;
