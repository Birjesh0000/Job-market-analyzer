/**
 * MongoDB Connection Manager for Node.js Backend
 * Mirrors Python database manager functionality
 */

const { MongoClient, ObjectId } = require('mongodb');
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });

let cachedClient = null;
let cachedDb = null;

class MongoConnection {
  constructor() {
    this.uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';
    this.dbName = process.env.DB_NAME || 'job_market_analyzer';
  }

  async connect() {
    if (cachedDb) {
      return cachedDb;
    }

    try {
      const client = new MongoClient(this.uri, {
        serverSelectionTimeoutMS: 10000,
        connectTimeoutMS: 10000,
        socketTimeoutMS: 10000,
        ssl: true,
        tlsInsecure: true,
        retryWrites: true,
      });

      await client.connect();
      console.log('Connected to MongoDB');

      cachedClient = client;
      cachedDb = client.db(this.dbName);
      return cachedDb;
    } catch (error) {
      console.error('MongoDB connection failed:', error.message);
      throw error;
    }
  }

  async disconnect() {
    if (cachedClient) {
      await cachedClient.close();
      cachedClient = null;
      cachedDb = null;
    }
  }
}

class DatabaseQuery {
  constructor(db) {
    this.db = db;
    this.jobsCollection = db.collection('jobs');
  }

  /**
   * Get paginated jobs with optional filters
   */
  async getJobs(page = 1, limit = 20, filters = {}) {
    const skip = (page - 1) * limit;
    const query = this._buildQuery(filters);

    const [jobs, totalCount] = await Promise.all([
      this.jobsCollection
        .find(query)
        .sort({ posted_date: -1 })
        .skip(skip)
        .limit(limit)
        .toArray(),
      this.jobsCollection.countDocuments(query),
    ]);

    return {
      jobs,
      pagination: {
        page,
        limit,
        total: totalCount,
        pages: Math.ceil(totalCount / limit),
      },
    };
  }

  /**
   * Get top skills by frequency
   */
  async getTopSkills(limit = 10) {
    const results = await this.jobsCollection
      .aggregate([
        { $unwind: '$skills' },
        {
          $group: {
            _id: '$skills',
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
        { $limit: limit },
        {
          $project: {
            skill: '$_id',
            count: 1,
            _id: 0,
          },
        },
      ])
      .toArray();

    return results;
  }

  /**
   * Get top companies by job count
   */
  async getTopCompanies(limit = 10) {
    const results = await this.jobsCollection
      .aggregate([
        {
          $group: {
            _id: '$company',
            job_count: { $sum: 1 },
          },
        },
        { $sort: { job_count: -1 } },
        { $limit: limit },
        {
          $project: {
            company: '$_id',
            job_count: 1,
            _id: 0,
          },
        },
      ])
      .toArray();

    return results;
  }

  /**
   * Get hiring trends - jobs posted over time
   */
  async getHiringTrends(groupBy = 'day') {
    let dateFormat;

    switch (groupBy) {
      case 'week':
        dateFormat = '%Y-W%V';
        break;
      case 'month':
        dateFormat = '%Y-%m';
        break;
      case 'day':
      default:
        dateFormat = '%Y-%m-%d';
    }

    const results = await this.jobsCollection
      .aggregate([
        {
          $match: {
            posted_date: { $exists: true, $ne: null },
          },
        },
        {
          $group: {
            _id: {
              $dateToString: {
                format: dateFormat,
                date: { $toDate: '$posted_date' },
              },
            },
            jobs_posted: { $sum: 1 },
          },
        },
        { $sort: { _id: 1 } },
        {
          $project: {
            date: '$_id',
            jobs_posted: 1,
            _id: 0,
          },
        },
      ])
      .toArray();

    return results;
  }

  /**
   * Get job stats summary
   */
  async getJobStats() {
    const stats = await this.jobsCollection
      .aggregate([
        {
          $facet: {
            total_jobs: [{ $count: 'count' }],
            total_companies: [
              {
                $group: {
                  _id: '$company',
                },
              },
              { $count: 'count' },
            ],
            unique_locations: [
              {
                $group: {
                  _id: '$location',
                },
              },
              { $count: 'count' },
            ],
            job_types: [
              {
                $group: {
                  _id: '$type',
                  count: { $sum: 1 },
                },
              },
            ],
          },
        },
      ])
      .toArray();

    return stats[0] || {};
  }

  /**
   * Build MongoDB query from filters
   */
  _buildQuery(filters) {
    const query = {};

    if (filters.skills && Array.isArray(filters.skills) && filters.skills.length > 0) {
      query.skills = { $in: filters.skills };
    }

    if (filters.location && filters.location.trim()) {
      query.location = { $regex: filters.location, $options: 'i' };
    }

    if (filters.company && filters.company.trim()) {
      query.company = { $regex: filters.company, $options: 'i' };
    }

    if (filters.job_type && filters.job_type.trim()) {
      query.type = filters.job_type;
    }

    return query;
  }
}

/**
 * Initialize database connection and query handler
 */
async function initializeDatabase() {
  const mongoConnection = new MongoConnection();
  const db = await mongoConnection.connect();
  return new DatabaseQuery(db);
}

module.exports = {
  MongoConnection,
  DatabaseQuery,
  initializeDatabase,
};
