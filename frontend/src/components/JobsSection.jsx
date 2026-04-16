import React, { useState } from 'react';
import { Briefcase, MapPin, DollarSign, ChevronLeft, ChevronRight } from 'lucide-react';
import './JobsSection.css';

function JobsSection({ jobs, page, onPageChange, onRefresh }) {
  const [selectedJob, setSelectedJob] = useState(null);

  if (!jobs || jobs.length === 0) {
    return (
      <div className="jobs-section">
        <p className="no-data">No jobs found</p>
      </div>
    );
  }

  return (
    <div className="jobs-section">
      <div className="jobs-list">
        {jobs.map((job, idx) => (
          <div
            key={idx}
            className={`job-card ${selectedJob === idx ? 'active' : ''}`}
            onClick={() => setSelectedJob(selectedJob === idx ? null : idx)}
          >
            <div className="job-header">
              <div className="job-title-company">
                <h3 className="job-title">{job.title}</h3>
                <p className="job-company">{job.company}</p>
              </div>
              <span className="job-type">{job.type || 'Full-time'}</span>
            </div>

            <div className="job-meta">
              <span className="job-meta-item">
                <MapPin size={16} />
                {job.location}
              </span>
              {job.salary_min && job.salary_max && (
                <span className="job-meta-item">
                  <DollarSign size={16} />
                  {job.currency} {job.salary_min}K - {job.salary_max}K
                </span>
              )}
            </div>

            {selectedJob === idx && (
              <div className="job-details">
                <p className="job-description">{job.description}</p>
                {job.skills && job.skills.length > 0 && (
                  <div className="job-skills">
                    <p className="skills-label">Required Skills:</p>
                    <div className="skills-tags">
                      {job.skills.slice(0, 5).map((skill, sidx) => (
                        <span key={sidx} className="skill-tag">
                          {skill}
                        </span>
                      ))}
                      {job.skills.length > 5 && (
                        <span className="skill-tag more">+{job.skills.length - 5}</span>
                      )}
                    </div>
                  </div>
                )}
                <a href={job.url} target="_blank" rel="noopener noreferrer" className="job-link">
                  View Full Job
                </a>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="jobs-footer">
        <button
          className="pagination-btn"
          onClick={() => onPageChange(page - 1)}
          disabled={page === 1}
        >
          <ChevronLeft size={18} />
          Previous
        </button>
        <span className="page-info">Page {page}</span>
        <button className="pagination-btn" onClick={() => onPageChange(page + 1)}>
          Next
          <ChevronRight size={18} />
        </button>
      </div>
    </div>
  );
}

export default JobsSection;
