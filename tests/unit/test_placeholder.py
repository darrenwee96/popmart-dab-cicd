"""
Unit tests for the AMER Trade Orders medallion bundle.

Run with:
    pytest tests/unit -v

Add tests here for:
  - Python transformation helpers (once extracted from SQL/notebooks)
  - Schema validation utilities
  - Business-logic edge cases (e.g. decimal precision ÷100, refund rate calc)
"""


def test_pipeline_sql_file_exists():
    """Verify the DLT SQL source file is present in the repo."""
    from pathlib import Path
    sql_path = Path(__file__).parent.parent.parent / "src" / "transformations" / "amer_trade_orders_medallion.sql"
    assert sql_path.exists(), f"Pipeline SQL not found at {sql_path}"


def test_pipeline_sql_has_gold_views():
    """Verify the pipeline SQL contains the expected Gold-layer view definitions."""
    from pathlib import Path
    sql = (Path(__file__).parent.parent.parent / "src" / "transformations" / "amer_trade_orders_medallion.sql").read_text()
    assert "gold_daily_sales_kpi" in sql
    assert "gold_product_performance" in sql
    assert "gold_channel_country_daily" in sql


def test_pipeline_sql_has_silver_views():
    """Verify the pipeline SQL contains the expected Silver-layer view definitions."""
    from pathlib import Path
    sql = (Path(__file__).parent.parent.parent / "src" / "transformations" / "amer_trade_orders_medallion.sql").read_text()
    assert "silver_order_line_items" in sql
    assert "silver_trade_order_daily" in sql


def test_pipeline_sql_has_decimal_precision_fix():
    """Verify the ÷100 decimal precision correction is applied to key monetary fields."""
    from pathlib import Path
    sql = (Path(__file__).parent.parent.parent / "src" / "transformations" / "amer_trade_orders_medallion.sql").read_text()
    assert "/ 100.0" in sql


def test_pipeline_sql_source_tables():
    """Verify the expected Bronze source tables are referenced."""
    from pathlib import Path
    sql = (Path(__file__).parent.parent.parent / "src" / "transformations" / "amer_trade_orders_medallion.sql").read_text()
    assert "dwd_trd_amer_buy2s_order_full_d" in sql
    assert "dws_amer_trade_order_1d" in sql
