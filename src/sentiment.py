"""
Sentiment scoring layer.

Reads raw reviews, runs a pretrained DistilBERT sentiment classifier over
the review text in batches, and writes the scored results back.

Model: distilbert-base-uncased-finetuned-sst-2-english - a small, fast,
well-established binary sentiment model (POSITIVE/NEGATIVE + confidence),
good enough for review-sentiment classification without needing to
fine-tune anything ourselves.
"""
from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"
BATCH_SIZE = 32
MAX_REVIEW_CHARS = 2000  # DistilBERT truncates at 512 tokens anyway; cap input size for speed


def _load_model():
    from transformers import pipeline

    logger.info("Loading sentiment model: %s", MODEL_NAME)
    return pipeline("sentiment-analysis", model=MODEL_NAME, truncation=True)


def score_sentiment(df: pd.DataFrame, text_column: str = "review") -> pd.DataFrame:
    """Adds sentiment_label and sentiment_score columns to df."""
    df = df.copy()

    # Empty/null reviews can't be scored - flag them rather than passing an
    # empty string into the model, which would produce a meaningless score.
    has_text = df[text_column].fillna("").str.strip() != ""

    texts = df.loc[has_text, text_column].fillna("").str.slice(0, MAX_REVIEW_CHARS).tolist()

    if texts:
        classifier = _load_model()
        results = []
        for i in range(0, len(texts), BATCH_SIZE):
            batch = texts[i : i + BATCH_SIZE]
            results.extend(classifier(batch))
            logger.info("Scored %d/%d reviews", min(i + BATCH_SIZE, len(texts)), len(texts))

        df.loc[has_text, "sentiment_label"] = [r["label"] for r in results]
        df.loc[has_text, "sentiment_score"] = [r["score"] for r in results]

    df.loc[~has_text, "sentiment_label"] = None
    df.loc[~has_text, "sentiment_score"] = None

    n_scored = has_text.sum()
    logger.info("Sentiment scoring complete: %d/%d reviews scored, %d had no text", n_scored, len(df), len(df) - n_scored)
    return df


if __name__ == "__main__":
    import dlt

    logging.basicConfig(level=logging.INFO)

    pipeline_obj = dlt.pipeline(pipeline_name="steam_sentiment_ingest", destination="snowflake", dataset_name="raw")
    with pipeline_obj.sql_client() as client:
        raw_df = client.execute_sql("SELECT * FROM raw.steam_reviews_raw")

    scored_df = score_sentiment(pd.DataFrame(raw_df))

    out_pipeline = dlt.pipeline(pipeline_name="steam_sentiment_scored", destination="snowflake", dataset_name="scored")
    out_pipeline.run(scored_df.to_dict("records"), table_name="steam_reviews_scored", write_disposition="replace")
    logger.info("Wrote scored reviews to Snowflake scored.steam_reviews_scored")
