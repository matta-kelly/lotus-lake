CREATE NAMESPACE IF NOT EXISTS shopify;

CREATE TABLE IF NOT EXISTS shopify.products_raw (
    -- Product fields (flat, matching GraphQL structure)
    id STRING,
    handle STRING,
    title STRING,
    tags ARRAY<STRING>,
    productType STRING,
    status STRING,
    createdAt TIMESTAMP,
    updatedAt TIMESTAMP,
    publishedAt TIMESTAMP,
    
    -- Nested variants (preserving GraphQL structure)
    variants STRUCT<
        edges: ARRAY<STRUCT<
            node: STRUCT<
                id: STRING,
                sku: STRING,
                price: STRING,
                compareAtPrice: STRING,
                createdAt: TIMESTAMP,
                updatedAt TIMESTAMP
            >
        >>
    >,

    -- Partition key
    ingestion_date STRING
)
USING ICEBERG
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);