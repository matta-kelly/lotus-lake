/**
 * Enriched Models Factory
 *
 * Auto-generates Cube.js cubes for all dbt models tagged 'enriched'.
 * These are the fct_* fact tables (1 currently, scales as you add more).
 *
 * Reads from dbt manifest.json to discover models, then defines cubes
 * that query lakehouse.main.{model_name}.
 */

const fs = require('fs');
const path = require('path');

// Load dbt manifest (copied into image at build time)
const manifestPath = path.join(__dirname, '..', 'target', 'manifest.json');
let manifest = { nodes: {} };

try {
  manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
} catch (err) {
  console.warn('Could not load manifest.json, using empty manifest:', err.message);
}

// Type mapping: DuckDB/dbt types → Cube.js types
const TYPE_MAP = {
  // Numeric
  'bigint': 'number',
  'integer': 'number',
  'int': 'number',
  'decimal': 'number',
  'double': 'number',
  'float': 'number',
  'real': 'number',
  'numeric': 'number',
  // String
  'varchar': 'string',
  'text': 'string',
  'string': 'string',
  'char': 'string',
  // Time
  'timestamp': 'time',
  'timestamptz': 'time',
  'date': 'time',
  'datetime': 'time',
  // Boolean
  'boolean': 'boolean',
  'bool': 'boolean',
};

function toCubeType(dbtType) {
  if (!dbtType) return 'string';
  const normalized = dbtType.toLowerCase().split('(')[0].trim();
  return TYPE_MAP[normalized] || 'string';
}

function isNumericType(dbtType) {
  if (!dbtType) return false;
  const normalized = dbtType.toLowerCase().split('(')[0].trim();
  return ['bigint', 'integer', 'int', 'decimal', 'double', 'float', 'real', 'numeric'].includes(normalized);
}

// Filter to enriched models (tagged 'enriched' OR name starts with 'fct_')
const enrichedModels = Object.values(manifest.nodes)
  .filter(node => node.resource_type === 'model')
  .filter(node =>
    (node.tags && node.tags.includes('enriched')) ||
    node.name.startsWith('fct_')  // Fallback: naming convention
  );

console.log(`[enriched.js] Found ${enrichedModels.length} enriched models`);

// Generate cube definitions
module.exports = enrichedModels.map(model => {
  const columns = Object.entries(model.columns || {});
  const uniqueKey = model.config?.unique_key;

  console.log(`[enriched.js] Generating cube: ${model.name} (${columns.length} columns)`);

  return {
    name: model.name,
    sql_table: `lakehouse.main.${model.name}`,

    // All columns become dimensions
    dimensions: columns.length > 0
      ? columns.map(([colName, colMeta]) => ({
          name: colName,
          sql: colName,
          type: toCubeType(colMeta.data_type),
          primary_key: uniqueKey === colName,
        }))
      : [
          // Fallback: if no columns in manifest, at least define a count
          { name: 'id', sql: '1', type: 'number', primary_key: true },
        ],

    // Measures: count + sum for numeric columns
    measures: [
      { name: 'count', type: 'count' },
      // Auto-add sum measures for numeric columns
      ...columns
        .filter(([_, colMeta]) => isNumericType(colMeta.data_type))
        .map(([colName, _]) => ({
          name: `total_${colName}`,
          sql: colName,
          type: 'sum',
        })),
    ],
  };
});
