-- ============================================================================
-- Pop Mart — SDP (Lakeflow Spark Declarative Pipeline) medallion
-- Reads Lakeflow Connect ingested tables (popmart) -> SILVER -> GOLD
-- Publishes Silver & Gold to : ${var.catalog}.${var.schema}
-- Reads Bronze source tables from: ${source_catalog}.${source_schema}
-- (source_catalog / source_schema are pipeline configuration values — see databricks.yml)
-- ============================================================================

-- ============================ SILVER — dimensions ==========================
CREATE OR REFRESH MATERIALIZED VIEW silver_dim_ip (
  CONSTRAINT valid_ip_id      EXPECT (ip_id IS NOT NULL)                                    ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_ip_name    EXPECT (ip_name IS NOT NULL AND length(trim(ip_name)) > 0),
  CONSTRAINT valid_origin     EXPECT (origin IN ('original','licensed')),
  CONSTRAINT plausible_launch EXPECT (launch_year IS NULL OR launch_year BETWEEN 1990 AND year(current_date()) + 1)
)
COMMENT 'Cleaned IP/brand dimension' AS
SELECT ip_id, ip_name, artist_name, origin, launch_year
FROM ${source_catalog}.${source_schema}.ip_brands;

CREATE OR REFRESH MATERIALIZED VIEW silver_dim_product (
  CONSTRAINT valid_product_id    EXPECT (product_id IS NOT NULL)                             ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_sku           EXPECT (sku_code IS NOT NULL AND length(trim(sku_code)) > 0),
  CONSTRAINT valid_product_type  EXPECT (product_type IN ('blind_box','mega_400','mega_1000','plush_pendant','plush_doll','accessory','blocks')),
  CONSTRAINT non_negative_price  EXPECT (retail_price IS NULL OR retail_price >= 0),
  CONSTRAINT non_negative_cost   EXPECT (standard_cost IS NULL OR standard_cost >= 0),
  CONSTRAINT non_negative_margin EXPECT (unit_margin >= 0),
  CONSTRAINT valid_secret_ratio  EXPECT (secret_ratio IS NULL OR secret_ratio BETWEEN 0 AND 1)
)
COMMENT 'Conformed product dimension (ecom catalog + retail cost)' AS
SELECT
  p.product_id, p.sku_code, p.figure_name AS product_name,
  s.series_id, s.series_name, s.secret_ratio,
  b.ip_id, b.ip_name, b.artist_name, b.origin,
  p.product_type, CAST(p.is_secret AS BOOLEAN) AS is_secret,
  p.unit_price AS retail_price, c.standard_cost,
  ROUND(p.unit_price - COALESCE(c.standard_cost, 0), 2) AS unit_margin,
  (p.product_type = 'blind_box') AS is_blind_box
FROM ${source_catalog}.${source_schema}.products p
LEFT JOIN ${source_catalog}.${source_schema}.product_series s ON p.series_id = s.series_id
LEFT JOIN ${source_catalog}.${source_schema}.ip_brands b ON s.ip_id = b.ip_id
LEFT JOIN ${source_catalog}.${source_schema}.product_catalog c ON p.product_id = c.product_id;

CREATE OR REFRESH MATERIALIZED VIEW silver_dim_member (
  CONSTRAINT valid_member_id     EXPECT (member_id IS NOT NULL)                              ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email         EXPECT (email IS NULL OR email LIKE '%@%.%'),
  CONSTRAINT valid_tier          EXPECT (membership_tier IN ('Rookie','Silver','Gold','Black Card')),
  CONSTRAINT non_negative_points EXPECT (points_balance IS NULL OR points_balance >= 0),
  CONSTRAINT signup_not_future   EXPECT (signup_date IS NULL OR signup_date <= current_date())
)
COMMENT 'Cleaned member dimension — standardized country, typed flags' AS
SELECT
  member_id, first_name, last_name, email, phone, city,
  country AS country_raw,
  CASE
    WHEN upper(trim(country)) IN ('US','USA','U.S.','UNITED STATES') THEN 'United States'
    WHEN upper(trim(country)) IN ('SG','SGP','SINGAPORE') THEN 'Singapore'
    WHEN upper(trim(country)) IN ('UK','U.K.','UNITED KINGDOM') THEN 'United Kingdom'
    WHEN upper(trim(country)) IN ('KOREA','KR','S.KOREA','SOUTH KOREA') THEN 'South Korea'
    ELSE initcap(trim(country))
  END AS country,
  signup_date, membership_tier, points_balance, birthday,
  CAST(marketing_opt_in AS BOOLEAN) AS marketing_opt_in
FROM ${source_catalog}.${source_schema}.members;

CREATE OR REFRESH MATERIALIZED VIEW silver_dim_store (
  CONSTRAINT valid_store_id   EXPECT (store_id IS NOT NULL)                                   ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_store_type EXPECT (store_type IN ('flagship','standard','pop_bakery','roboshop_hub')),
  CONSTRAINT valid_status     EXPECT (status IN ('active','closed','maintenance','inactive')),
  CONSTRAINT valid_latitude   EXPECT (latitude IS NULL OR latitude BETWEEN -90 AND 90),
  CONSTRAINT valid_longitude  EXPECT (longitude IS NULL OR longitude BETWEEN -180 AND 180),
  CONSTRAINT positive_area    EXPECT (floor_area_sqm IS NULL OR floor_area_sqm > 0)
)
COMMENT 'Store dimension' AS
SELECT store_id, store_name, store_type, country, region, city,
       latitude, longitude, open_date, floor_area_sqm, status
FROM ${source_catalog}.${source_schema}.stores;

-- ============================ SILVER — facts ===============================
CREATE OR REFRESH MATERIALIZED VIEW silver_fact_online_sales (
  CONSTRAINT valid_order_item_id   EXPECT (order_item_id IS NOT NULL)                        ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_id        EXPECT (order_id IS NOT NULL)                             ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id      EXPECT (product_id IS NOT NULL)                           ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity        EXPECT (quantity > 0)                                     ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount          EXPECT (line_total >= 0)                                  ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_price    EXPECT (unit_price >= 0),
  CONSTRAINT valid_order_status    EXPECT (order_status IN ('created','paid','packed','shipped','delivered','cancelled','refunded')),
  CONSTRAINT order_date_not_future EXPECT (order_date <= current_date())
)
COMMENT 'Online order lines (orders x order_items)' AS
SELECT
  oi.order_item_id, oi.order_id, o.member_id, CAST(o.order_datetime AS TIMESTAMP) AS order_datetime,
  CAST(o.order_datetime AS DATE) AS order_date,
  o.channel, o.ship_country, o.order_status,
  oi.product_id, oi.quantity, oi.unit_price, oi.line_total, o.currency
FROM ${source_catalog}.${source_schema}.order_items oi
JOIN ${source_catalog}.${source_schema}.orders o ON oi.order_id = o.order_id;

CREATE OR REFRESH MATERIALIZED VIEW silver_fact_pos_sales (
  CONSTRAINT valid_txn_id        EXPECT (txn_id IS NOT NULL)                                 ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_product_id    EXPECT (product_id IS NOT NULL)                             ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity      EXPECT (quantity > 0)                                       ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_amount EXPECT (sale_amount >= 0)                                   ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_price  EXPECT (unit_price >= 0),
  CONSTRAINT valid_payment_type  EXPECT (payment_type IS NULL OR payment_type IN ('cash','card','mobile')),
  CONSTRAINT valid_channel       EXPECT (channel IN ('store','roboshop'))
)
COMMENT 'In-store & roboshop POS lines' AS
SELECT
  txn_id, store_id, machine_id, product_id, member_id,
  quantity, unit_price, sale_amount, currency,
  CAST(txn_timestamp AS TIMESTAMP) AS txn_timestamp, CAST(txn_timestamp AS DATE) AS txn_date, payment_type, employee_id,
  CASE WHEN machine_id IS NOT NULL THEN 'roboshop' ELSE 'store' END AS channel
FROM ${source_catalog}.${source_schema}.pos_transactions;

CREATE OR REFRESH MATERIALIZED VIEW silver_fact_sales (
  CONSTRAINT valid_sale_id        EXPECT (sale_id IS NOT NULL)                               ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_channel        EXPECT (channel IN ('online','store','roboshop'))          ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id     EXPECT (product_id IS NOT NULL)                            ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity       EXPECT (quantity > 0)                                      ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_amount  EXPECT (gross_amount >= 0)                                 ON VIOLATION DROP ROW,
  CONSTRAINT valid_sale_date      EXPECT (sale_date IS NOT NULL),
  CONSTRAINT sale_date_not_future EXPECT (sale_date <= current_date())
)
COMMENT 'UNIFIED omnichannel sales fact (online + store + roboshop)' AS
SELECT
  CONCAT('ONL-', CAST(order_item_id AS STRING)) AS sale_id,
  'online' AS channel, order_datetime AS sale_ts, order_date AS sale_date,
  product_id, member_id,
  CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS machine_id,
  quantity, line_total AS gross_amount, currency, ship_country AS country
FROM silver_fact_online_sales
WHERE order_status <> 'cancelled'
UNION ALL
SELECT
  CONCAT('POS-', CAST(p.txn_id AS STRING)) AS sale_id,
  p.channel, p.txn_timestamp AS sale_ts, p.txn_date AS sale_date,
  p.product_id, p.member_id, p.store_id, p.machine_id,
  p.quantity, p.sale_amount AS gross_amount, p.currency, st.country
FROM silver_fact_pos_sales p
LEFT JOIN silver_dim_store st ON p.store_id = st.store_id;

CREATE OR REFRESH MATERIALIZED VIEW silver_fact_pop_draw (
  CONSTRAINT valid_draw_id        EXPECT (draw_id IS NOT NULL)                               ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_product_id     EXPECT (drawn_product_id IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_price   EXPECT (price_paid >= 0)                                   ON VIOLATION DROP ROW,
  CONSTRAINT valid_secret_ratio   EXPECT (secret_ratio IS NULL OR secret_ratio BETWEEN 0 AND 1),
  CONSTRAINT draw_date_not_future EXPECT (draw_date <= current_date())
)
COMMENT 'Online blind-box draws with IP/series + typed secret-hit flag' AS
SELECT
  pd.draw_id, pd.member_id, pd.series_id, b.ip_name, s.series_name, s.secret_ratio,
  pd.drawn_product_id, CAST(pd.is_secret_hit AS BOOLEAN) AS is_secret_hit,
  pd.price_paid, CAST(pd.draw_datetime AS TIMESTAMP) AS draw_datetime, CAST(pd.draw_datetime AS DATE) AS draw_date
FROM ${source_catalog}.${source_schema}.pop_draw pd
LEFT JOIN ${source_catalog}.${source_schema}.product_series s ON pd.series_id = s.series_id
LEFT JOIN ${source_catalog}.${source_schema}.ip_brands b ON s.ip_id = b.ip_id;

CREATE OR REFRESH MATERIALIZED VIEW silver_fact_inventory (
  CONSTRAINT valid_inventory_id   EXPECT (inventory_id IS NOT NULL)                          ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_store_id       EXPECT (store_id IS NOT NULL)                              ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id     EXPECT (product_id IS NOT NULL)                            ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_qty     EXPECT (qty_on_hand >= 0),
  CONSTRAINT non_negative_reorder EXPECT (reorder_point IS NULL OR reorder_point >= 0)
)
COMMENT 'Store inventory with stockout / reorder flags' AS
SELECT inventory_id, store_id, product_id, qty_on_hand, reorder_point, last_restock_date,
       (qty_on_hand = 0) AS is_stockout,
       (qty_on_hand < reorder_point) AS is_below_reorder
FROM ${source_catalog}.${source_schema}.store_inventory;

CREATE OR REFRESH MATERIALIZED VIEW silver_fact_supply (
  CONSTRAINT valid_po_id          EXPECT (po_id IS NOT NULL)                                 ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_product_id     EXPECT (product_id IS NOT NULL)                            ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity       EXPECT (quantity > 0)                                      ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_cost    EXPECT (unit_cost IS NULL OR unit_cost >= 0),
  CONSTRAINT valid_status         EXPECT (status IN ('draft','sent','confirmed','in_transit','received','closed')),
  CONSTRAINT non_negative_lead    EXPECT (actual_lead_days IS NULL OR actual_lead_days >= 0),
  CONSTRAINT expected_after_order EXPECT (expected_date IS NULL OR expected_date >= order_date)
)
COMMENT 'Purchase-order lines with supplier + lead-time' AS
SELECT
  po.po_id, po.supplier_id, sup.supplier_name, sup.country AS supplier_country, sup.category,
  li.product_id, li.quantity, li.unit_cost, po.order_date, po.expected_date, po.received_date, po.status,
  DATEDIFF(po.received_date, po.order_date) AS actual_lead_days,
  sup.lead_time_days AS expected_lead_days
FROM ${source_catalog}.${source_schema}.po_line_items li
JOIN ${source_catalog}.${source_schema}.purchase_orders po ON li.po_id = po.po_id
JOIN ${source_catalog}.${source_schema}.suppliers sup ON po.supplier_id = sup.supplier_id;

-- ============================ GOLD — marts =================================
CREATE OR REFRESH MATERIALIZED VIEW gold_sales_daily
COMMENT 'Daily revenue/units by channel, country, IP, product type' AS
SELECT f.sale_date, f.channel, f.country, d.ip_name, d.product_type,
       SUM(f.quantity) AS units, ROUND(SUM(f.gross_amount), 2) AS revenue,
       COUNT(DISTINCT f.sale_id) AS line_count
FROM silver_fact_sales f
LEFT JOIN silver_dim_product d ON f.product_id = d.product_id
GROUP BY f.sale_date, f.channel, f.country, d.ip_name, d.product_type;

CREATE OR REFRESH MATERIALIZED VIEW gold_ip_performance
COMMENT 'Revenue/units by IP across all channels' AS
SELECT d.ip_name, d.artist_name, d.origin,
       ROUND(SUM(f.gross_amount), 2) AS total_revenue,
       SUM(f.quantity) AS total_units,
       COUNT(DISTINCT f.sale_id) AS total_lines,
       ROUND(SUM(CASE WHEN f.channel = 'online' THEN f.gross_amount ELSE 0 END), 2) AS online_revenue,
       ROUND(SUM(CASE WHEN f.channel <> 'online' THEN f.gross_amount ELSE 0 END), 2) AS offline_revenue,
       COUNT(DISTINCT d.product_id) AS num_products
FROM silver_fact_sales f
JOIN silver_dim_product d ON f.product_id = d.product_id
GROUP BY d.ip_name, d.artist_name, d.origin;

CREATE OR REFRESH MATERIALIZED VIEW gold_secret_hit_rates
COMMENT 'Blind-box secret-figure hit rates by series' AS
SELECT ip_name, series_name, secret_ratio AS expected_ratio,
       COUNT(*) AS total_draws,
       SUM(CAST(is_secret_hit AS INT)) AS secret_hits,
       ROUND(SUM(CAST(is_secret_hit AS INT)) / COUNT(*), 5) AS actual_hit_rate
FROM silver_fact_pop_draw
GROUP BY ip_name, series_name, secret_ratio;

CREATE OR REFRESH MATERIALIZED VIEW gold_omnichannel_customer
COMMENT 'Per-member online vs offline spend + omnichannel flag' AS
WITH online AS (
  SELECT member_id, SUM(gross_amount) AS online_spend, COUNT(DISTINCT sale_id) AS online_lines
  FROM silver_fact_sales WHERE channel = 'online' AND member_id IS NOT NULL GROUP BY member_id),
offline AS (
  SELECT member_id, SUM(gross_amount) AS offline_spend, COUNT(DISTINCT sale_id) AS offline_lines
  FROM silver_fact_sales WHERE channel <> 'online' AND member_id IS NOT NULL GROUP BY member_id)
SELECT m.member_id, m.membership_tier, m.country, m.city,
       ROUND(COALESCE(o.online_spend, 0), 2) AS online_spend,
       ROUND(COALESCE(f.offline_spend, 0), 2) AS offline_spend,
       ROUND(COALESCE(o.online_spend, 0) + COALESCE(f.offline_spend, 0), 2) AS total_spend,
       (o.member_id IS NOT NULL AND f.member_id IS NOT NULL) AS is_omnichannel
FROM silver_dim_member m
LEFT JOIN online o ON m.member_id = o.member_id
LEFT JOIN offline f ON m.member_id = f.member_id;

CREATE OR REFRESH MATERIALIZED VIEW gold_member_tier_summary
COMMENT 'Membership-tier LTV + omnichannel penetration' AS
SELECT membership_tier,
       COUNT(*) AS num_members,
       ROUND(AVG(total_spend), 2) AS avg_ltv,
       ROUND(SUM(total_spend), 2) AS total_revenue,
       ROUND(100.0 * SUM(CASE WHEN is_omnichannel THEN 1 ELSE 0 END) / COUNT(*), 1) AS omnichannel_pct
FROM gold_omnichannel_customer
GROUP BY membership_tier;

CREATE OR REFRESH MATERIALIZED VIEW gold_store_performance
COMMENT 'Revenue/units/basket by store' AS
SELECT st.store_id, st.store_name, st.store_type, st.country, st.city,
       ROUND(SUM(p.sale_amount), 2) AS revenue, SUM(p.quantity) AS units,
       COUNT(DISTINCT p.txn_id) AS txns,
       ROUND(SUM(p.sale_amount) / NULLIF(COUNT(DISTINCT p.txn_id), 0), 2) AS avg_basket
FROM silver_fact_pos_sales p
JOIN silver_dim_store st ON p.store_id = st.store_id
GROUP BY st.store_id, st.store_name, st.store_type, st.country, st.city;

CREATE OR REFRESH MATERIALIZED VIEW gold_inventory_health
COMMENT 'Inventory health by store' AS
SELECT st.store_id, st.store_name, st.store_type, st.country,
       COUNT(*) AS num_skus,
       SUM(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) AS stockouts,
       SUM(CASE WHEN i.is_below_reorder THEN 1 ELSE 0 END) AS below_reorder,
       ROUND(100.0 * SUM(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) / COUNT(*), 1) AS stockout_rate
FROM silver_fact_inventory i
JOIN silver_dim_store st ON i.store_id = st.store_id
GROUP BY st.store_id, st.store_name, st.store_type, st.country;

CREATE OR REFRESH MATERIALIZED VIEW gold_supply_chain_supplier
COMMENT 'Supplier lead-time & spend' AS
SELECT supplier_name, supplier_country, category,
       COUNT(DISTINCT po_id) AS num_pos,
       ROUND(SUM(quantity * unit_cost), 2) AS total_cost,
       ROUND(AVG(actual_lead_days), 1) AS avg_actual_lead_days,
       ROUND(AVG(expected_lead_days), 1) AS avg_expected_lead_days
FROM silver_fact_supply
GROUP BY supplier_name, supplier_country, category;
