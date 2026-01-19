with msg as (
    select * from {{ ref('stg_telegram_messages') }}
),

ch as (
    select channel_key, channel_name
    from {{ ref('dim_channels') }}
)

select
    msg.message_id,
    ch.channel_key,
    cast(to_char(msg.message_date, 'YYYYMMDD') as int) as date_key,
    msg.message_text,
    msg.message_length,
    msg.view_count,
    msg.forward_count,
    msg.has_image
from msg
join ch using (channel_name);
