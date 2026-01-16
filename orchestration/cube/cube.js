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
  // S3 credentials must be configured via CREATE SECRET for DuckLake to access parquet files
  driverFactory: async () => {
    const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');

    const pgHost = process.env.DUCKLAKE_DB_HOST || 'ducklake-db-rw.lotus-lake.svc.cluster.local';
    const pgPass = process.env.DUCKLAKE_DB_PASSWORD || '';

    // S3 credentials - same pattern as Dagster's lib.py
    const s3Key = process.env.CUBEJS_DB_DUCKDB_S3_ACCESS_KEY_ID || '';
    const s3Secret = process.env.CUBEJS_DB_DUCKDB_S3_SECRET_ACCESS_KEY || '';
    const s3Endpoint = process.env.CUBEJS_DB_DUCKDB_S3_ENDPOINT || '';

    return new DuckDBDriver({
      database: ':memory:',
      initSql: `
        INSTALL ducklake; LOAD ducklake;
        INSTALL httpfs; LOAD httpfs;

        CREATE SECRET s3_secret (
          TYPE S3,
          KEY_ID '${s3Key}',
          SECRET '${s3Secret}',
          ENDPOINT '${s3Endpoint}',
          USE_SSL false,
          URL_STYLE 'path'
        );

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
