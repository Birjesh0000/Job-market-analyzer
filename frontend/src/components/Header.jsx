import React from 'react';
import { TrendingUp } from 'lucide-react';
import './Header.css';

function Header() {
  return (
    <header className="header">
      <div className="header-container">
        <div className="header-brand">
          <div className="brand-icon">
            <TrendingUp size={28} />
          </div>
          <div className="brand-text">
            <h1 className="brand-title">Job Market Analyzer</h1>
            <p className="brand-subtitle">Real-time job market insights & trends</p>
          </div>
        </div>
        <nav className="header-nav">
          <p className="nav-item">Data Analytics Platform</p>
        </nav>
      </div>
    </header>
  );
}

export default Header;
