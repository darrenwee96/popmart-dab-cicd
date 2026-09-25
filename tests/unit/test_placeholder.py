"""
Unit tests for the Pop Mart medallion bundle.

Run with:
    pytest tests/unit -v

Add tests here for:
  - Python transformation helpers (once extracted from SQL/notebooks)
  - Schema validation utilities
  - Business-logic edge cases (e.g. margin calculations, tier thresholds)
"""


def test_pipeline_sql_file_exists():
    """Verify the DLT SQL source file is present in the repo."""
    from pathlib import Path
    sql_path = Path(__file__).parent.parent.parent / "src" / "transformations" / "medallion_pipeline.sql"
    assert sql_path.exists(), f"Pipeline SQL not found at {sql_path}"


def test_pipeline_sql_has_gold_views():
    """Verify the pipeline SQL contains Gold-layer view definitions."""
    from pathlib import Path
    sql = (Path(__file__).parent.parent.parent / "src" / "transformations" / "medallion_pipeline.sql").read_text()
    assert "gold_sales_daily" in sql
    assert "gold_ip_performance" in sql
    assert "gold_omnichannel_customer" in sql


def test_pipeline_sql_has_constraints():
    """Verify data-quality CONSTRAINT blocks exist in Silver views."""
    from pathlib import Path
    sql = (Path(__file__).parent.parent.parent / "src" / "transformations" / "medallion_pipeline.sql").read_text()
    assert "CONSTRAINT" in sql
    assert "EXPECT" in sql
