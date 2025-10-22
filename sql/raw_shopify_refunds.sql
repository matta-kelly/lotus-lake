CREATE TABLE IF NOT EXISTS shopify.refunds_raw (
    id STRING,
    order_id STRING,
    order_name STRING,
    createdAt TIMESTAMP,
    updatedAt TIMESTAMP,

    -- ======================
    -- Total refunded amount
    -- ======================
    totalRefundedSet STRUCT<
        shopMoney: STRUCT<
            amount: STRING,
            currencyCode: STRING
        >
    >,

    -- ======================
    -- Refund line items
    -- ======================
    refundLineItems STRUCT<
        edges: ARRAY<STRUCT<
            node: STRUCT<
                quantity: INT,
                lineItem: STRUCT<
                    id: STRING,
                    title: STRING,
                    sku: STRING,
                    product: STRUCT<
                        id: STRING,
                        productType: STRING
                    >,
                    variant: STRUCT<
                        id: STRING,
                        sku: STRING
                    >
                >,
                subtotalSet: STRUCT<
                    shopMoney: STRUCT<
                        amount: STRING,
                        currencyCode: STRING
                    >
                >,
                totalTaxSet: STRUCT<
                    shopMoney: STRUCT<
                        amount: STRING,
                        currencyCode: STRING
                    >
                >
            >
        >>
    >,

    -- ======================
    -- Transactions
    -- ======================
    transactions STRUCT<
        edges: ARRAY<STRUCT<
            node: STRUCT<
                id: STRING,
                amount: STRING,
                kind: STRING,
                status: STRING,
                createdAt: TIMESTAMP
            >
        >>
    >,

    ingestion_date STRING
)
USING ICEBERG
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);
