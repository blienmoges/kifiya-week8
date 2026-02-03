select *
from {{ ref('stg_telegram_messages') }}
where view_count < 0
   or forward_count < 0
