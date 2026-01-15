/**
 * Cube.js Configuration
 *
 * Connects to DuckLake (DuckDB + PostgreSQL metadata catalog)
 * with S3 storage on SeaweedFS.
 *
 * Environment variables (same as Dagster/dbt):
 *   - S3_ACCESS_KEY_ID
 *   - S3_SECRET_ACCESS_KEY
 *   - S3_ENDPOINT
 *   - DUCKLAKE_DB_HOST
 *   - DUCKLAKE_DB_PASSWORD
 *   - CUBEJS_API_SECRET
 */

module.exports = {
  dbType: 'duckdb',

  // DuckDB driver configuration
  driverFactory: () => {
    const s3Key = process.env.S3_ACCESS_KEY_ID || '';
    const s3Secret = process.env.S3_SECRET_ACCESS_KEY || '';
    const s3Endpoint = process.env.S3_ENDPOINT || '';
    const pgHost = process.env.DUCKLAKE_DB_HOST || 'ducklake-db-rw.lotus-lake.svc.cluster.local';
    const pgPass = process.env.DUCKLAKE_DB_PASSWORD || '';

    return {
      type: 'duckdb',
      // Initialization SQL - runs once when connection is created
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
    };
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
