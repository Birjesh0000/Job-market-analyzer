import React from 'react';
import { Briefcase, Building2, MapPin, Users } from 'lucide-react';
import './StatsCards.css';

function StatsCards({ stats }) {
  const getStatValue = (item) => {
    if (Array.isArray(item)) {
      return item.length > 0 ? item[0].count : 0;
    }
    return 0;
  };

  const statCards = [
    {
      title: 'Total Jobs',
      value: getStatValue(stats?.total_jobs),
      icon: Briefcase,
      color: 'blue',
      description: 'Active job postings',
    },
    {
      title: 'Companies Hiring',
      value: getStatValue(stats?.total_companies),
      icon: Building2,
      color: 'green',
      description: 'Unique companies',
    },
    {
      title: 'Locations',
      value: getStatValue(stats?.unique_locations),
      icon: MapPin,
      color: 'purple',
      description: 'Job markets covered',
    },
    {
      title: 'Avg Jobs/Company',
      value: (getStatValue(stats?.total_jobs) / Math.max(getStatValue(stats?.total_companies), 1)).toFixed(1),
      icon: Users,
      color: 'orange',
      description: 'Average jobs per company',
    },
  ];

  return (
    <div className="stats-grid">
      {statCards.map((card, idx) => {
        const Icon = card.icon;
        return (
          <div key={idx} className={`stat-card stat-card-${card.color}`}>
            <div className="stat-icon">
              <Icon size={24} />
            </div>
            <div className="stat-content">
              <p className="stat-label">{card.title}</p>
              <p className="stat-value">{card.value}</p>
              <p className="stat-description">{card.description}</p>
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default StatsCards;
