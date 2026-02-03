with det as (
    select
        message_id,
        detected_class,
        confidence_score,
        image_category
    from {{ ref('stg_yolo_detections') }}
),

msg as (
    select
        message_id,
        channel_key,
        date_key,
        view_count
    from {{ ref('fct_messages') }}
)

select
    det.message_id,
    msg.channel_key,
    msg.date_key,
    det.detected_class,
    det.confidence_score,
    det.image_category,
    msg.view_count
from det
join msg using (message_id)
