with base as (
    select
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(view_count)::numeric(18,2) as avg_views
    from {{ ref('stg_telegram_messages') }}
    group by 1
),

typed as (
    select
        channel_name,
        case
            when channel_name like '%pharma%' then 'Pharmaceutical'
            when channel_name like '%cosmetic%' or channel_name like '%lobelia%' then 'Cosmetics'
            else 'Medical'
        end as channel_type,
        first_post_date,
        last_post_date,
        total_posts,
        avg_views
    from base
)

select
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} as channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
from typed;
