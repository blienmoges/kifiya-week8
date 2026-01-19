with src as (
    select
        message_id,
        channel_name,
        message_date,
        message_text,
        has_media,
        image_path,
        views,
        forwards
    from {{ source('raw', 'telegram_messages') }}
),

clean as (
    select
        cast(message_id as bigint) as message_id,
        lower(trim(channel_name)) as channel_name,
        cast(message_date as timestamptz) as message_ts,
        cast(message_date as date) as message_date,
        nullif(trim(message_text), '') as message_text,
        coalesce(cast(has_media as boolean), false) as has_media,
        nullif(trim(image_path), '') as image_path,
        coalesce(cast(views as bigint), 0) as view_count,
        coalesce(cast(forwards as bigint), 0) as forward_count,
        case
            when image_path is not null and length(trim(image_path)) > 0 then true
            else false
        end as has_image,
        case
            when message_text is null then 0
            else length(message_text)
        end as message_length
    from src
)

select *
from clean
where message_ts is not null
  and (message_text is not null or has_image = true);
