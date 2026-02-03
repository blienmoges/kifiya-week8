with src as (
    select
        message_id::bigint as message_id,
        trim(lower(channel_name)) as channel_name,
        message_date::timestamptz as message_ts,
        coalesce(message_text, '') as message_text,
        coalesce(has_media, false) as has_media,
        nullif(image_path, '') as image_path,
        coalesce(views, 0)::bigint as view_count,
        coalesce(forwards, 0)::bigint as forward_count,
        case
            when image_path is not null and image_path <> '' then true
            else false
        end as has_image
    from {{ source('raw', 'telegram_messages') }}
    where message_id is not null
      and message_date is not null
)

select
    *,
    length(message_text) as message_length
from src
