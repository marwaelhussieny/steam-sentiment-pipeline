with staged as (
    select * from {{ ref('stg_steam_reviews') }}
),

final as (
    select
        review_id,
        game_title,
        review_date,
        hours_played,
        helpful_votes,
        funny_votes,
        is_early_access_review,
        recommendation,
        sentiment_label,
        sentiment_score,
        -- Flags a mismatch between Steam's own binary recommendation and the
        -- model's sentiment read of the review text - useful for finding
        -- sarcastic reviews or ones where the text doesn't match the vote.
        case
            when recommendation = 'Recommended' and sentiment_label = 'NEGATIVE' then true
            when recommendation = 'Not Recommended' and sentiment_label = 'POSITIVE' then true
            else false
        end as sentiment_recommendation_mismatch
    from staged
    where review_text is not null
)

select * from final
