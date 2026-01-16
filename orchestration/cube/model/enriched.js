/**
 * Enriched Models Factory
 *
 * Auto-generates Cube.js cubes for all dbt models tagged 'enriched'.
 * These are the fct_* fact tables (1 currently, scales as you add more).
 *
 * Parses column names directly from dbt model SQL (raw_code in manifest).
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

/**
 * Parse column names from dbt SELECT SQL.
 * Handles: "col", "col as alias", "expr as alias", comments, CTEs
 */
function parseColumnsFromSql(sql) {
  if (!sql) return [];

  // Remove jinja blocks {{ ... }}
  sql = sql.replace(/\{\{[^}]*\}\}/g, '');
  // Remove {% ... %} blocks
  sql = sql.replace(/\{%[^%]*%\}/g, '');

  // If there's a CTE (WITH ... AS), find the final SELECT after all CTEs
  // CTEs end with ) followed by the main SELECT
  const cteMatch = sql.match(/\)\s*select\s+([\s\S]*?)\s+from\s+/i);
  const regularMatch = sql.match(/select\s+([\s\S]*?)\s+from\s+/i);

  // Use CTE match if available, otherwise regular match
  const selectMatch = cteMatch || regularMatch;
  if (!selectMatch) return [];

  const selectClause = selectMatch[1];

  // Split by comma, handling parentheses
  const columns = [];
  let current = '';
  let parenDepth = 0;

  for (const char of selectClause) {
    if (char === '(') parenDepth++;
    else if (char === ')') parenDepth--;
    else if (char === ',' && parenDepth === 0) {
      columns.push(current.trim());
      current = '';
      continue;
    }
    current += char;
  }
  if (current.trim()) columns.push(current.trim());

  // Extract column names (handle "x as y" → y, "x" → x)
  return columns
    .map(col => {
      // Remove comments
      col = col.replace(/--.*$/gm, '').trim();
      if (!col) return null;

      // "expr as alias" → alias
      const asMatch = col.match(/\s+as\s+(\w+)\s*$/i);
      if (asMatch) return asMatch[1];

      // Simple column reference
      const simpleMatch = col.match(/^(\w+)$/);
      if (simpleMatch) return simpleMatch[1];

      // table.column → column
      const dotMatch = col.match(/\.(\w+)$/);
      if (dotMatch) return dotMatch[1];

      return null;
    })
    .filter(Boolean);
}

/**
 * Guess type from column name (heuristic).
 */
function guessTypeFromName(colName) {
  const name = colName.toLowerCase();

  // Timestamps
  if (name.includes('_at') || name.includes('date') || name.includes('timestamp')) {
    return 'time';
  }
  // Counts/amounts
  if (name.includes('count') || name.includes('amount') || name.includes('total') ||
      name.includes('spent') || name.includes('quantity') || name.includes('price') ||
      name.includes('revenue') || name.includes('discount')) {
    return 'number';
  }
  // IDs
  if (name.endsWith('_id') || name === 'id') {
    return 'string';
  }
  // Year/month/day partitions
  if (name === 'year' || name === 'month' || name === 'day') {
    return 'number';
  }

  return 'string';
}

// Filter to enriched models (tagged 'enriched' OR name starts with 'fct_')
const enrichedModels = Object.values(manifest.nodes)
  .filter(node => node.resource_type === 'model')
  .filter(node =>
    (node.tags && node.tags.includes('enriched')) ||
    node.name.startsWith('fct_')
  );

console.log(`[enriched.js] Found ${enrichedModels.length} enriched models`);

// Generate cube definitions using global cube() function
enrichedModels.forEach(model => {
  const columns = parseColumnsFromSql(model.raw_code);
  const uniqueKey = model.config?.unique_key;

  console.log(`[enriched.js] Generating cube: ${model.name} (${columns.length} columns: ${columns.slice(0, 5).join(', ')}${columns.length > 5 ? '...' : ''})`);

  cube(model.name, {
    sql_table: `lakehouse.main.${model.name}`,

    dimensions: Object.fromEntries(
      columns.map(colName => [
        colName,
        {
          sql: () => colName,
          type: guessTypeFromName(colName),
          primary_key: uniqueKey === colName,
        },
      ])
    ),

    measures: {
      count: { type: 'count' },
      // Sum measures for numeric columns
      ...Object.fromEntries(
        columns
          .filter(colName => guessTypeFromName(colName) === 'number')
          .map(colName => [
            `total_${colName}`,
            { sql: () => colName, type: 'sum' },
          ])
      ),
    },
  });
});
