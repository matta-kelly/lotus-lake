// Simple static test cube to verify basic Cube.js + DuckLake setup works
// If this loads successfully, the issue is in dynamic generation
// If this fails, the issue is in sql_table schema or connection

cube(`test_cube`, {
  sql_table: `lakehouse.main.int_shopify__orders`,

  dimensions: {
    order_id: {
      sql: `order_id`,
      type: `string`,
      primary_key: true
    }
  },

  measures: {
    count: {
      type: `count`
    }
  }
});
