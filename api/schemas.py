from pydantic import BaseModel
from typing import Optional

class Message(BaseModel):
    message_id: int
    channel_key: str
    date_key: int
    message_text: Optional[str]
    message_length: int
    view_count: int
    forward_count: int
    has_image: bool
