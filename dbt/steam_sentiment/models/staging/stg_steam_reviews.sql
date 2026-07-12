with source as (
    select * from {{ source('scored', 'steam_reviews_scored') }}
),

renamed as (
    select
        review_id,
        title as game_title,
        cast(date_posted as date) as review_date,
        cast(hour_played as float) as hours_played,
        cast(helpful as int) as helpful_votes,
        cast(funny as int) as funny_votes,
        cast(is_early_access_review as boolean) as is_early_access_review,
        recommendation,
        review as review_text,
        sentiment_label,
        cast(sentiment_score as float) as sentiment_score
    from source
)

select * from renamed
