/**
 * Cube.js Configuration
 *
 * Connects to DuckLake (DuckDB + PostgreSQL metadata catalog)
 * with S3 storage on SeaweedFS.
 *
 * Environment variables:
 *   - CUBEJS_DB_DUCKDB_S3_* (native Cube.js S3 config)
 *   - DUCKLAKE_DB_HOST (postgres catalog host)
 *   - DUCKLAKE_DB_PASSWORD (postgres catalog password)
 *   - CUBEJS_API_SECRET
 */

module.exports = {
  dbType: 'duckdb',

  // DuckDB driver configuration
  // S3 credentials are handled by CUBEJS_DB_DUCKDB_S3_* env vars
  driverFactory: async () => {
    const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');

    const pgHost = process.env.DUCKLAKE_DB_HOST || 'ducklake-db-rw.lotus-lake.svc.cluster.local';
    const pgPass = process.env.DUCKLAKE_DB_PASSWORD || '';

    return new DuckDBDriver({
      database: ':memory:',
      initSql: `
        INSTALL ducklake; LOAD ducklake;

        ATTACH 'ducklake:postgres:host=${pgHost} port=5432 dbname=ducklake user=ducklake password=${pgPass}'
        AS lakehouse (DATA_PATH 's3://landing/raw/');
      `,
    });
  },

  // API configuration
  apiSecret: process.env.CUBEJS_API_SECRET,

  // Enable PostgreSQL protocol for BI tools (Power BI, etc.)
  // Exposed via Traefik on host port 5432
  sqlPort: 5432,
  sqlUser: process.env.CUBEJS_SQL_USER || 'cube',
  sqlPassword: process.env.CUBEJS_SQL_PASSWORD || process.env.CUBEJS_API_SECRET,

  // Telemetry
  telemetry: false,
};
