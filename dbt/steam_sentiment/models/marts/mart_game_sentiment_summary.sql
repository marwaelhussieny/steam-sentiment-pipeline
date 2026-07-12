with reviews as (
    select * from {{ ref('fct_reviews') }}
),

summary as (
    select
        game_title,
        count(*) as total_reviews,
        sum(case when recommendation = 'Recommended' then 1 else 0 end) as recommended_count,
        sum(case when sentiment_label = 'POSITIVE' then 1 else 0 end) as positive_sentiment_count,
        sum(case when sentiment_label = 'NEGATIVE' then 1 else 0 end) as negative_sentiment_count,
        round(avg(sentiment_score), 3) as avg_sentiment_confidence,
        round(avg(hours_played), 1) as avg_hours_played,
        sum(case when sentiment_recommendation_mismatch then 1 else 0 end) as mismatch_count
    from reviews
    group by game_title
)

select
    *,
    round(100.0 * recommended_count / nullif(total_reviews, 0), 1) as pct_recommended,
    round(100.0 * positive_sentiment_count / nullif(total_reviews, 0), 1) as pct_positive_sentiment
from summary
order by total_reviews desc
