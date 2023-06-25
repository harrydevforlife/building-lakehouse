WITH daily_sales_products AS (
  SELECT 
    CAST(order_purchase_timestamp AS DATE) AS daily
    , product_id
    , ROUND(SUM(CAST(payment_value AS FLOAT)), 2) AS sales
    , COUNT(DISTINCT(order_id)) AS bills
  FROM {{ref("fact_sales")}}
  WHERE order_status = 'delivered'
  GROUP BY 
    CAST(order_purchase_timestamp AS DATE)
    , product_id
), daily_sales_categories AS (
  SELECT
    ts.daily
    , DATE_FORMAT(ts.daily, 'y-MM') AS monthly
    , p.product_category_name_english AS category
    , ts.sales
    , ts.bills
    , (ts.sales / ts.bills) AS values_per_bills
  FROM daily_sales_products ts
  JOIN {{ref("dim_products")}} p
  ON ts.product_id = p.product_id
)
SELECT
  monthly
  , category
  , SUM(sales) AS total_sales
  , SUM(bills) AS total_bills
  , (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills
FROM daily_sales_categories
GROUP BY 
  monthly
  , category