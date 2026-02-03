from typing import Optional, List
from pydantic import BaseModel, Field


class TopProductItem(BaseModel):
    term: str = Field(..., description="Extracted product/term token from message_text")
    mentions: int = Field(..., ge=0, description="Number of messages mentioning the term")


class TopProductsResponse(BaseModel):
    limit: int
    results: List[TopProductItem]


class ChannelActivityItem(BaseModel):
    date: str = Field(..., description="YYYY-MM-DD")
    posts: int = Field(..., ge=0)
    avg_views: Optional[float] = None


class ChannelActivityResponse(BaseModel):
    channel_name: str
    daily: List[ChannelActivityItem]


class MessageSearchItem(BaseModel):
    message_id: int
    channel_name: str
    message_date: str
    views: int
    forwards: int
    has_image: bool
    message_text: str


class MessageSearchResponse(BaseModel):
    query: str
    limit: int
    results: List[MessageSearchItem]


class VisualContentItem(BaseModel):
    channel_name: str
    posts_with_images: int
    total_posts: int
    image_pct: float


class VisualContentResponse(BaseModel):
    results: List[VisualContentItem]
