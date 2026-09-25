-- =============================================================
-- AMER Trade Orders Medallion Pipeline
-- Bronze (external Delta tables) → Silver (cleaned) → Gold (aggregated)
--
-- Bronze source tables (already ingested from StarRocks):
--   popmart.default.dwd_trd_amer_buy2s_order_full_d  (81M POS line items)
--   popmart.default.dws_amer_trade_order_1d           (daily aggregates)
-- =============================================================

-- =============================================================
-- SILVER LAYER — Cleaned and enriched materialized views
-- =============================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_order_line_items
COMMENT 'Cleaned POS order line items with decimal precision fixed (÷100), business-friendly column names, and extracted date parts. Reads from bronze dwd_trd_amer_buy2s_order_full_d.'
AS
SELECT
  -- Keys and identifiers
  posno                                            AS pos_terminal_id,
  flowno                                           AS transaction_id,
  itemno                                           AS line_item_number,
  DATE(rcvtime)                                    AS transaction_date,
  rcvtime                                          AS transaction_timestamp,
  YEAR(rcvtime)                                    AS txn_year,
  MONTH(rcvtime)                                   AS txn_month,
  DAYOFWEEK(rcvtime)                               AS txn_dow,

  -- Product
  gid                                              AS product_group_id,
  gdcode                                           AS product_code,

  -- Quantities (decimal precision fix: ÷100)
  CAST(iqty  / 100.0 AS DECIMAL(18,2))             AS initial_quantity,
  CAST(qty   / 100.0 AS DECIMAL(18,2))             AS quantity,

  -- Pricing (decimal precision fix: ÷100)
  CAST(rtlprc      / 100.0 AS DECIMAL(18,2))       AS retail_price,
  CAST(pfurtlprc   / 100.0 AS DECIMAL(18,2))       AS pfu_retail_price,
  CAST(stdtotal    / 100.0 AS DECIMAL(18,2))       AS standard_total,
  CAST(scrprice    / 100.0 AS DECIMAL(18,2))       AS screen_price,
  CAST(scrtotal    / 100.0 AS DECIMAL(18,2))       AS screen_total,
  CAST(favamt      / 100.0 AS DECIMAL(18,2))       AS discount_amount,
  CAST(realamt     / 100.0 AS DECIMAL(18,2))       AS actual_amount,
  CAST(listprice   / 100.0 AS DECIMAL(18,2))       AS list_price,
  CAST(tokenprc    / 100.0 AS DECIMAL(18,2))       AS token_price,
  CAST(tokentotal  / 100.0 AS DECIMAL(18,2))       AS token_total,
  CAST(tareweight  / 100.0 AS DECIMAL(18,2))       AS tare_weight,

  -- Tax (decimal precision fix: ÷100)
  CAST(saletax     / 100.0 AS DECIMAL(18,2))       AS sales_tax,
  CAST(tax         / 100.0 AS DECIMAL(18,2))       AS tax_amount,
  saletaxtype                                      AS sales_tax_type,

  -- Invoice and dealer amounts (decimal precision fix: ÷100)
  CAST(iamt        / 100.0 AS DECIMAL(18,2))       AS invoice_amount,
  CAST(itax        / 100.0 AS DECIMAL(18,2))       AS invoice_tax,

  -- Source-currency equivalents (decimal precision fix: ÷100)
  CAST(srcrtlprc     / 100.0 AS DECIMAL(18,2))     AS src_retail_price,
  CAST(srcpfurtlprc  / 100.0 AS DECIMAL(18,2))     AS src_pfu_retail_price,
  CAST(srcstdtotal   / 100.0 AS DECIMAL(18,2))     AS src_standard_total,
  CAST(srcscrprice   / 100.0 AS DECIMAL(18,2))     AS src_screen_price,
  CAST(srcscrtotal   / 100.0 AS DECIMAL(18,2))     AS src_screen_total,
  CAST(srcfavamt     / 100.0 AS DECIMAL(18,2))     AS src_discount_amount,
  CAST(srcrealamt    / 100.0 AS DECIMAL(18,2))     AS src_actual_amount,
  CAST(srclistprice  / 100.0 AS DECIMAL(18,2))     AS src_list_price,

  -- Operations
  wrh                                              AS warehouse_id,
  dealer                                           AS dealer_id,
  assistant                                        AS assistant_id,
  assistantcode                                    AS assistant_code,
  invno                                            AS invoice_number,
  ordflag                                          AS order_flag,
  vbnum                                            AS vb_number,
  codetype                                         AS code_type,
  retailitemno                                     AS retail_item_number,

  -- Descriptive
  remark,
  tastetags                                        AS taste_tags,
  killedreason                                     AS killed_reason,
  outidentcode                                     AS out_ident_code,
  srcitemno                                        AS src_item_number,
  inputtime                                        AS input_time,

  -- Dates
  sdrptdate                                        AS sd_report_date,
  etl_time

FROM popmart.default.dwd_trd_amer_buy2s_order_full_d;


CREATE OR REFRESH MATERIALIZED VIEW silver_trade_order_daily
COMMENT 'Daily trade order summary enriched with net orders, net revenue, refund rate, and cancel rate. Reads from bronze dws_amer_trade_order_1d.'
AS
SELECT
  stat_date,
  country_code,
  channel,
  pay_currency,
  order_cnt,
  user_cnt,
  paid_order_cnt,
  refund_order_cnt,
  cancel_order_cnt,
  pay_amount_sum,
  refund_amount_sum,
  actual_pay_amount_sum,
  promotion_amount_sum,
  coupon_amount_sum,
  freight_amount_sum,
  tax_amount_sum,
  avg_order_amount,
  etl_update_time,
  -- Computed columns
  paid_order_cnt - refund_order_cnt                                    AS net_order_cnt,
  actual_pay_amount_sum - refund_amount_sum                            AS net_revenue,
  ROUND(refund_order_cnt  / NULLIF(paid_order_cnt, 0), 4)              AS refund_rate,
  ROUND(cancel_order_cnt  / NULLIF(order_cnt, 0), 4)                   AS cancel_rate
FROM popmart.default.dws_amer_trade_order_1d;


-- =============================================================
-- GOLD LAYER — Business aggregates for dashboards and Genie
-- =============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales_kpi (
  transaction_date      DATE           COMMENT 'Calendar date of the transactions',
  total_transactions    BIGINT         COMMENT 'Number of distinct POS transactions on this day',
  total_items_sold      DECIMAL(28,2)  COMMENT 'Total quantity of items sold across all transactions',
  gross_revenue         DECIMAL(28,2)  COMMENT 'Sum of actual amounts paid (local currency, precision-corrected)',
  total_discounts       DECIMAL(28,2)  COMMENT 'Sum of all discount amounts applied',
  total_tax             DECIMAL(28,2)  COMMENT 'Sum of sales tax collected',
  unique_products_sold  BIGINT         COMMENT 'Count of distinct product codes sold',
  active_terminals      BIGINT         COMMENT 'Count of distinct POS terminals with at least one sale',
  avg_transaction_value DECIMAL(29,2)  COMMENT 'Average revenue per transaction (gross_revenue / total_transactions)'
)
COMMENT 'Daily sales KPI summary for Pop Mart AMER region. One row per day.'
AS
SELECT
  transaction_date,
  COUNT(DISTINCT transaction_id)                                                          AS total_transactions,
  SUM(quantity)                                                                           AS total_items_sold,
  SUM(actual_amount)                                                                      AS gross_revenue,
  SUM(discount_amount)                                                                    AS total_discounts,
  SUM(sales_tax)                                                                          AS total_tax,
  COUNT(DISTINCT product_code)                                                            AS unique_products_sold,
  COUNT(DISTINCT pos_terminal_id)                                                         AS active_terminals,
  ROUND(SUM(actual_amount) / NULLIF(COUNT(DISTINCT transaction_id), 0), 2)                AS avg_transaction_value
FROM silver_order_line_items
GROUP BY transaction_date;


CREATE OR REFRESH MATERIALIZED VIEW gold_product_performance (
  product_code      STRING         COMMENT 'Unique product SKU code',
  product_group_id  INT            COMMENT 'Product group/category identifier',
  total_line_items  BIGINT         COMMENT 'Total number of order line items containing this product',
  total_qty_sold    DECIMAL(28,2)  COMMENT 'Total quantity sold across all transactions',
  total_revenue     DECIMAL(28,2)  COMMENT 'Lifetime revenue from this product (sum of actual_amount)',
  total_discounts   DECIMAL(28,2)  COMMENT 'Lifetime discounts applied to this product',
  avg_retail_price  DECIMAL(19,2)  COMMENT 'Average listed retail price for this product',
  transaction_count BIGINT         COMMENT 'Number of distinct transactions that include this product',
  first_sale_date   DATE           COMMENT 'Date of the earliest recorded sale for this product',
  last_sale_date    DATE           COMMENT 'Date of the most recent recorded sale for this product'
)
COMMENT 'Product-level lifetime performance metrics for Pop Mart AMER.'
AS
SELECT
  product_code,
  product_group_id,
  COUNT(*)                            AS total_line_items,
  SUM(quantity)                       AS total_qty_sold,
  SUM(actual_amount)                  AS total_revenue,
  SUM(discount_amount)                AS total_discounts,
  ROUND(AVG(retail_price), 2)         AS avg_retail_price,
  COUNT(DISTINCT transaction_id)      AS transaction_count,
  MIN(transaction_date)               AS first_sale_date,
  MAX(transaction_date)               AS last_sale_date
FROM silver_order_line_items
GROUP BY product_code, product_group_id;


CREATE OR REFRESH MATERIALIZED VIEW gold_channel_country_daily (
  stat_date             DATE    COMMENT 'Calendar date',
  country_code          STRING  COMMENT 'ISO country code (US, CA, MX, FR)',
  channel               STRING  COMMENT 'Raw channel value from source system',
  channel_name          STRING  COMMENT 'Human-readable channel name (empty mapped to Direct/Other)',
  pay_currency          STRING  COMMENT 'Payment currency code (USD, CAD, MXN)',
  order_cnt             BIGINT  COMMENT 'Total order count',
  user_cnt              BIGINT  COMMENT 'Distinct user count',
  paid_order_cnt        BIGINT  COMMENT 'Number of paid orders',
  refund_order_cnt      BIGINT  COMMENT 'Number of refunded orders',
  cancel_order_cnt      BIGINT  COMMENT 'Number of cancelled orders',
  net_order_cnt         BIGINT  COMMENT 'Paid orders minus refunded orders',
  pay_amount_sum        DOUBLE  COMMENT 'Gross payment amount sum',
  refund_amount_sum     DOUBLE  COMMENT 'Total refund amount',
  net_revenue           DOUBLE  COMMENT 'Actual pay amount minus refund amount',
  actual_pay_amount_sum DOUBLE  COMMENT 'Actual pay amount after adjustments',
  promotion_amount_sum  DOUBLE  COMMENT 'Total promotion discounts applied',
  coupon_amount_sum     DOUBLE  COMMENT 'Total coupon discounts applied',
  freight_amount_sum    DOUBLE  COMMENT 'Total freight/shipping charges',
  tax_amount_sum        DOUBLE  COMMENT 'Total tax amount collected',
  avg_order_amount      DOUBLE  COMMENT 'Average order amount',
  refund_rate           DOUBLE  COMMENT 'Ratio of refunded to paid orders',
  cancel_rate           DOUBLE  COMMENT 'Ratio of cancelled to total orders',
  prev_day_net_revenue  DOUBLE  COMMENT 'Previous day net revenue for same country+channel (for DoD comparison)',
  revenue_change_pct    DOUBLE  COMMENT 'Day-over-day net revenue change percentage'
)
COMMENT 'Daily revenue and order metrics by country and sales channel for Pop Mart AMER.'
AS
WITH enriched AS (
  SELECT
    stat_date,
    country_code,
    channel,
    CASE WHEN channel = '' THEN 'Direct/Other' ELSE channel END AS channel_name,
    pay_currency,
    order_cnt,
    user_cnt,
    paid_order_cnt,
    refund_order_cnt,
    cancel_order_cnt,
    net_order_cnt,
    pay_amount_sum,
    refund_amount_sum,
    net_revenue,
    actual_pay_amount_sum,
    promotion_amount_sum,
    coupon_amount_sum,
    freight_amount_sum,
    tax_amount_sum,
    avg_order_amount,
    refund_rate,
    cancel_rate
  FROM silver_trade_order_daily
)
SELECT
  *,
  LAG(net_revenue) OVER (PARTITION BY country_code, channel ORDER BY stat_date)   AS prev_day_net_revenue,
  ROUND(
    (net_revenue - LAG(net_revenue) OVER (PARTITION BY country_code, channel ORDER BY stat_date))
    / NULLIF(ABS(LAG(net_revenue) OVER (PARTITION BY country_code, channel ORDER BY stat_date)), 0)
  , 4)                                                                            AS revenue_change_pct
FROM enriched;
