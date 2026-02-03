select
    run_ts::timestamptz as run_ts,
    trim(lower(channel_name)) as channel_name,
    message_id::bigint as message_id,
    image_path,
    detected_class,
    confidence_score::numeric as confidence_score,
    bbox_xyxy,
    image_category
from {{ source('raw', 'yolo_detections') }}
where message_id is not null
