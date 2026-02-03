with msg as (
    select
        message_id,
        channel_name,
        message_ts::date as full_date,
        message_text,
        message_length,
        view_count,
        forward_count,
        has_image
    from {{ ref('stg_telegram_messages') }}
),

ch as (
    select channel_key, channel_name
    from {{ ref('dim_channels') }}
),

dt as (
    select date_key, full_date
    from {{ ref('dim_dates') }}
)

select
    msg.message_id,
    ch.channel_key,
    dt.date_key,
    msg.message_text,
    msg.message_length,
    msg.view_count,
    msg.forward_count,
    msg.has_image
from msg
join ch on msg.channel_name = ch.channel_name
join dt on msg.full_date = dt.full_date
