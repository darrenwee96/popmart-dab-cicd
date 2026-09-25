# Databricks notebook source
# ============================================================================
# Pop Mart Medallion Pipeline — Post-Run Report Notebook
#
# Runs automatically after each DLT pipeline refresh (Task 2 of the job).
# Validates Gold-layer table row counts and prints data-quality metrics
# from system tables.
# ============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC # 📊 Pop Mart — Post-Pipeline Report
# MAGIC
# MAGIC This notebook runs after each **Lakeflow Declarative Pipeline** refresh.
# MAGIC
# MAGIC It checks:
# MAGIC - ✅ Gold-layer table row counts
# MAGIC - 🔍 Data-quality expectation pass/fail rates (via `system.lakeflow`)

# COMMAND ----------

# Widget parameters are injected by the Lakeflow Job
dbutils.widgets.text("catalog", "main",              "Unity Catalog")
dbutils.widgets.text("schema",  "popmart_medallion", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"▶ Reporting on:  {catalog}.{schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer — Row Counts

# COMMAND ----------

gold_tables = [
    "gold_sales_daily",
    "gold_ip_performance",
    "gold_secret_hit_rates",
    "gold_omnichannel_customer",
    "gold_member_tier_summary",
    "gold_store_performance",
    "gold_inventory_health",
    "gold_supply_chain_supplier",
]

print(f"{'Table':<45} {'Row Count':>12}")
print("─" * 59)

all_ok = True
for table in gold_tables:
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    try:
        count = spark.table(fq).count()
        status = "✅" if count > 0 else "⚠️  EMPTY"
        if count == 0:
            all_ok = False
        print(f"{table:<45} {count:>12,}  {status}")
    except Exception as e:
        all_ok = False
        print(f"{table:<45} {'ERROR':>12}  ❌ {e}")

print()
print("✅ All Gold tables populated." if all_ok else "⚠️  Some tables have issues — check above.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality — Expectation Metrics (last 24 h)

# COMMAND ----------

dq_sql = f"""
SELECT
  exp.dataset                                              AS table_name,
  exp.name                                                 AS expectation,
  SUM(exp.passed_records)                                  AS passed_rows,
  SUM(exp.failed_records)                                  AS failed_rows,
  ROUND(
    100.0 * SUM(exp.passed_records)
    / NULLIF(SUM(exp.passed_records) + SUM(exp.failed_records), 0),
    2
  )                                                        AS pass_rate_pct
FROM   system.lakeflow.pipeline_event_logs
LATERAL VIEW EXPLODE(details:flow_progress.data_quality.expectations) AS exp
WHERE  pipeline_id = (
         SELECT pipeline_id
         FROM   system.lakeflow.pipelines
         WHERE  name LIKE '%popmart_medallion%'
         ORDER  BY created_at DESC
         LIMIT  1
       )
  AND  event_type = 'flow_progress'
  AND  timestamp  >= current_timestamp() - INTERVAL 1 DAY
GROUP  BY exp.dataset, exp.name
ORDER  BY exp.dataset, exp.name
"""

try:
    dq_df = spark.sql(dq_sql)
    display(dq_df)

    low_pass = dq_df.filter("pass_rate_pct < 99").count()
    if low_pass > 0:
        print(f"⚠️  {low_pass} expectation(s) below 99% pass rate — review above.")
    else:
        print("✅ All expectations at ≥ 99% pass rate.")

except Exception as e:
    print(f"ℹ️  DQ metrics unavailable (pipeline may not have run yet): {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"Pipeline report complete for {catalog}.{schema}")
print("Job run complete ✅")
