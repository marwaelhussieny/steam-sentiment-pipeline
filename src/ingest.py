"""
Ingestion layer using dlt (data load tool).

Reads the raw Steam reviews CSV and loads it into Snowflake as-is, with dlt
automatically inferring and versioning the schema. dlt's incremental load
support means re-running this with new review data appends only new rows
rather than reloading everything.

Why dlt here instead of a hand-rolled loader: schema inference, automatic
retries on transient network failures, and built-in schema evolution
tracking (if Steam ever added/removed a review field, dlt would surface that
as a schema change rather than silently dropping or misaligning columns).
"""
from __future__ import annotations

import logging
from pathlib import Path

import dlt
import pandas as pd

logger = logging.getLogger(__name__)

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "steam_reviews.csv"


@dlt.resource(name="steam_reviews_raw", write_disposition="merge", primary_key="review_id")
def steam_reviews_source(path: Path = DATA_PATH):
    df = pd.read_csv(path)

    # Source CSV has no natural primary key - synthesize one from a stable
    # hash of the fields that together uniquely identify a review, so
    # re-running ingestion is idempotent (merge, not duplicate).
    # fillna first: some review text fields are null, and mixing NaN floats
    # into a string join blows up even after astype(str) in some pandas versions.
    key_cols = df[["title", "date_posted", "review"]].fillna("").astype(str)
    df["review_id"] = pd.util.hash_pandas_object(
        key_cols.agg("|".join, axis=1)
    ).astype(str)

    logger.info("Loaded %d raw reviews from %s", len(df), path)
    yield df.to_dict("records")


def run_ingestion(destination: str = "snowflake") -> None:
    pipeline = dlt.pipeline(
        pipeline_name="steam_sentiment_ingest",
        destination=destination,
        dataset_name="raw",
    )
    info = pipeline.run(steam_reviews_source())
    logger.info("Ingestion complete: %s", info)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    dest = sys.argv[1] if len(sys.argv) > 1 else "snowflake"
    run_ingestion(destination=dest)
