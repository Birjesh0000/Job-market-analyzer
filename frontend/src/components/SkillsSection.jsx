import React from 'react';
import { TrendingUp } from 'lucide-react';
import './SkillsSection.css';

function SkillsSection({ skills }) {
  if (!skills || skills.length === 0) {
    return (
      <div className="skills-section">
        <p className="no-data">No skills data available</p>
      </div>
    );
  }

  const maxCount = Math.max(...skills.map(s => s.count));

  return (
    <div className="skills-section">
      {skills.map((skill, idx) => {
        const percentage = (skill.count / maxCount) * 100;
        return (
          <div key={idx} className="skill-item">
            <div className="skill-header">
              <span className="skill-rank">{idx + 1}</span>
              <span className="skill-name">{skill.skill}</span>
              <span className="skill-count">{skill.count}</span>
            </div>
            <div className="skill-bar-container">
              <div
                className="skill-bar"
                style={{
                  width: `${percentage}%`,
                  animation: `fillBar 0.6s ease-out ${idx * 0.08}s both`,
                }}
              ></div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default SkillsSection;
