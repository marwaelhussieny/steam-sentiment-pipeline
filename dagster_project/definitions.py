"""
Dagster asset definitions for the Steam review sentiment pipeline.

raw_steam_reviews -> scored_steam_reviews -> dbt models (staging + marts)
"""
import subprocess
import sys
from pathlib import Path

import dagster as dg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

DBT_PROJECT_DIR = str(Path(__file__).resolve().parent.parent / "dbt" / "steam_sentiment")


@dg.asset(group_name="raw")
def raw_steam_reviews(context: dg.AssetExecutionContext) -> None:
    """Ingests the raw Steam reviews CSV into Snowflake via dlt."""
    from ingest import run_ingestion

    run_ingestion(destination="snowflake")
    context.add_output_metadata({"status": "loaded to raw.steam_reviews_raw"})


@dg.asset(group_name="scored", deps=[raw_steam_reviews])
def scored_steam_reviews(context: dg.AssetExecutionContext) -> None:
    """Runs the HuggingFace sentiment model over raw reviews and writes scored results."""
    import dlt
    import pandas as pd

    from data_quality import run_data_quality_checks
    from sentiment import score_sentiment

    pipeline_obj = dlt.pipeline(pipeline_name="steam_sentiment_ingest", destination="snowflake", dataset_name="raw")
    with pipeline_obj.sql_client() as client:
        raw_rows = client.execute_sql("SELECT * FROM raw.steam_reviews_raw")

    df = pd.DataFrame(raw_rows)
    scored = score_sentiment(df)
    run_data_quality_checks(scored)

    out_pipeline = dlt.pipeline(pipeline_name="steam_sentiment_scored", destination="snowflake", dataset_name="scored")
    out_pipeline.run(scored.to_dict("records"), table_name="steam_reviews_scored", write_disposition="replace")

    context.add_output_metadata({"rows_scored": len(scored)})


@dg.asset(group_name="marts", deps=[scored_steam_reviews])
def dbt_marts(context: dg.AssetExecutionContext) -> None:
    """Runs dbt to build the staging and mart models on top of scored reviews."""
    result = subprocess.run(
        ["dbt", "build", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError("dbt build failed - see logs above")


sentiment_pipeline_assets = [raw_steam_reviews, scored_steam_reviews, dbt_marts]

defs = dg.Definitions(assets=sentiment_pipeline_assets)
