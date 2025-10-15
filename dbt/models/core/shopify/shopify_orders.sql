{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

WITH deduped AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rn
    FROM {{ source('shopify', 'orders_raw') }}
    {% if is_incremental() %}
    WHERE updatedAt > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
  )
  WHERE rn = 1
)

SELECT
  id AS order_id,
  name AS order_name,
  createdAt AS created_at,
  updatedAt AS updated_at,
  displayFinancialStatus AS financial_status,

  CAST(currentSubtotalPriceSet.shopMoney.amount AS DOUBLE) AS subtotal,
  CAST(currentTotalDiscountsSet.shopMoney.amount AS DOUBLE) AS discounts_total,
  CAST(currentTotalPriceSet.shopMoney.amount AS DOUBLE) AS total_price,
  CAST(totalTaxSet.shopMoney.amount AS DOUBLE) AS total_tax,

  customer.id AS customer_id,
  customer.email AS customer_email,
  customer.firstName AS customer_first_name,
  customer.lastName AS customer_last_name,

  channelInformation.channelDefinition.channelName AS channel_name,
  channelInformation.channelDefinition.subChannelName AS sub_channel_name,

  shippingAddress.city AS shipping_city,
  shippingAddress.countryCode AS shipping_country_code,
  CAST(shippingLine.originalPriceSet.shopMoney.amount AS DOUBLE) AS shipping_cost

FROM deduped
