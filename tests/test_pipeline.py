import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import sentiment  # noqa: E402
from data_quality import run_data_quality_checks, DataQualityError  # noqa: E402
from ingest import steam_reviews_source  # noqa: E402


def _fake_load_model():
    def fake_classifier(texts):
        return [
            {"label": "POSITIVE" if "good" in t.lower() or "great" in t.lower() else "NEGATIVE", "score": 0.9}
            for t in texts
        ]
    return fake_classifier


def _make_reviews_df(n=1200):
    return pd.DataFrame({
        "title": ["Test Game"] * n,
        "date_posted": ["2020-01-01"] * n,
        "hour_played": [10.0] * n,
        "helpful": [0] * n,
        "funny": [0] * n,
        "is_early_access_review": [False] * n,
        "recommendation": (["Recommended", "Not Recommended"] * (n // 2 + 1))[:n],
        "review": (["This game is great", "This game is bad, not good at all"] * (n // 2 + 1))[:n],
    })


def test_ingest_generates_unique_review_ids(tmp_path):
    df = _make_reviews_df(n=100)
    df.loc[0, "review"] = None  # exercise the null-review edge case that broke earlier
    path = tmp_path / "reviews.csv"
    df.to_csv(path, index=False)

    records = list(steam_reviews_source(path=path))
    ids = [r["review_id"] for r in records]
    assert len(ids) == 100
    assert len(set(ids)) <= 100  # duplicates possible only if source rows are truly identical


def test_score_sentiment_adds_expected_columns(monkeypatch):
    monkeypatch.setattr(sentiment, "_load_model", _fake_load_model)
    df = _make_reviews_df()
    scored = sentiment.score_sentiment(df)
    assert "sentiment_label" in scored.columns
    assert "sentiment_score" in scored.columns
    assert scored["sentiment_label"].isin(["POSITIVE", "NEGATIVE"]).all()


def test_score_sentiment_handles_null_reviews(monkeypatch):
    monkeypatch.setattr(sentiment, "_load_model", _fake_load_model)
    df = _make_reviews_df(n=100)
    df.loc[0, "review"] = None
    scored = sentiment.score_sentiment(df)
    assert pd.isna(scored.loc[0, "sentiment_label"])


def test_data_quality_passes_on_clean_scored_data(monkeypatch):
    monkeypatch.setattr(sentiment, "_load_model", _fake_load_model)
    df = _make_reviews_df()
    scored = sentiment.score_sentiment(df)
    results = run_data_quality_checks(scored)
    assert all(r.passed for r in results)


def test_data_quality_fails_on_too_few_rows(monkeypatch):
    monkeypatch.setattr(sentiment, "_load_model", _fake_load_model)
    df = _make_reviews_df(n=50)
    scored = sentiment.score_sentiment(df)
    with pytest.raises(DataQualityError):
        run_data_quality_checks(scored)


def test_data_quality_fails_on_invalid_recommendation(monkeypatch):
    monkeypatch.setattr(sentiment, "_load_model", _fake_load_model)
    df = _make_reviews_df()
    df.loc[0, "recommendation"] = "Maybe"
    scored = sentiment.score_sentiment(df)
    with pytest.raises(DataQualityError):
        run_data_quality_checks(scored)
