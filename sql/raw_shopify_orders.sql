CREATE NAMESPACE IF NOT EXISTS shopify;

CREATE TABLE IF NOT EXISTS shopify.orders_raw (
    id STRING,
    name STRING,
    createdAt TIMESTAMP,
    updatedAt TIMESTAMP,
    displayFinancialStatus STRING,

    customer STRUCT<
        id: STRING,
        email: STRING,
        firstName: STRING,
        lastName: STRING
    >,

    channelInformation STRUCT<
        channelDefinition: STRUCT<
            channelName: STRING,
            subChannelName: STRING
        >
    >,

    totalTaxSet STRUCT<
        shopMoney: STRUCT<
            amount: STRING,
            currencyCode: STRING
        >
    >,

    currentSubtotalPriceSet STRUCT<
        shopMoney: STRUCT<
            amount: STRING,
            currencyCode: STRING
        >
    >,

    currentTotalDiscountsSet STRUCT<
        shopMoney: STRUCT<
            amount: STRING,
            currencyCode: STRING
        >
    >,

    currentTotalPriceSet STRUCT<
        shopMoney: STRUCT<
            amount: STRING,
            currencyCode: STRING
        >
    >,

    shippingAddress STRUCT<
        address1: STRING,
        city: STRING,
        zip: STRING,
        countryCode: STRING
    >,

    -- ======================
    -- Order-level discounts
    -- ======================
    discountApplications STRUCT<
        edges: ARRAY<STRUCT<
            node: STRUCT<
                __typename: STRING,
                index: INT,
                allocationMethod: STRING,
                targetSelection: STRING,
                targetType: STRING,
                code: STRING,
                value: STRUCT<
                    __typename: STRING,
                    percentage: DOUBLE,
                    amount: STRING,
                    currencyCode: STRING
                >
            >
        >>
    >,

    -- ======================
    -- Shipping line data
    -- ======================
    shippingLine STRUCT<
        originalPriceSet: STRUCT<
            shopMoney: STRUCT<
                amount: STRING,
                currencyCode: STRING
            >
        >,
        discountAllocations: ARRAY<STRUCT<
            allocatedAmountSet: STRUCT<
                shopMoney: STRUCT<
                    amount: STRING,
                    currencyCode: STRING
                >
            >,
            discountApplication: STRUCT<
                __typename: STRING,
                index: INT,
                allocationMethod: STRING,
                code: STRING
            >
        >>,
        taxLines: ARRAY<STRUCT<
            price: STRING
        >>,
        carrierIdentifier: STRING
    >,

    -- ======================
    -- Order-level tax lines
    -- ======================
    taxLines ARRAY<STRUCT<
        price: STRING,
        title: STRING
    >>,

    -- ======================
    -- Line items
    -- ======================
    lineItems STRUCT<
        edges: ARRAY<STRUCT<
            node: STRUCT<
                id: STRING,
                title: STRING,
                quantity: INT,
                sku: STRING,
                product: STRUCT<
                    id: STRING,
                    productType: STRING
                >,
                variant: STRUCT<
                    id: STRING,
                    sku: STRING,
                    price: STRING
                >,
                originalTotalSet: STRUCT<
                    shopMoney: STRUCT<
                        amount: STRING,
                        currencyCode: STRING
                    >
                >,
                discountedTotalSet: STRUCT<
                    shopMoney: STRUCT<
                        amount: STRING,
                        currencyCode: STRING
                    >
                >,
                discountAllocations: ARRAY<STRUCT<
                    allocatedAmountSet: STRUCT<
                        shopMoney: STRUCT<
                            amount: STRING,
                            currencyCode: STRING
                        >
                    >,
                    discountApplication: STRUCT<
                        __typename: STRING,
                        index: INT,
                        allocationMethod: STRING,
                        code: STRING
                    >
                >>
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
