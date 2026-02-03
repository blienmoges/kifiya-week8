from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from api.database import get_engine
from api.schemas import (
    TopProductsResponse, TopProductItem,
    ChannelActivityResponse, ChannelActivityItem,
    MessageSearchResponse, MessageSearchItem,
    VisualContentResponse, VisualContentItem
)

app = FastAPI(
    title="Medical Telegram Analytical API",
    version="1.0.0",
    description="Analytical API over transformed Telegram data (dbt marts in Postgres)."
)

# Create engine on startup (safer with reload)
engine: Engine | None = None


@app.on_event("startup")
def startup_event():
    global engine
    engine = get_engine()


@app.get("/health")
def health():
    return {"status": "ok"}


# 1) Top Products (basic token frequency)
@app.get("/api/reports/top-products", response_model=TopProductsResponse)
def top_products(limit: int = Query(10, ge=1, le=100)):
    """
    Returns most frequent tokens from message_text across all channels.
    Note: This is a basic approach (split by whitespace + cleanup).
    """
    if engine is None:
        raise HTTPException(status_code=500, detail="Database engine not initialized")

    sql = text("""
        with cleaned as (
            select coalesce(message_text, '') as txt
            from analytics.fct_messages
        ),
        tokens as (
            select
                lower(regexp_replace(token, '[^a-z0-9]+', '', 'g')) as term
            from cleaned,
                 unnest(regexp_split_to_array(txt, '\\s+')) as token
        )
        select term, count(*) as mentions
        from tokens
        where term is not null
          and term <> ''
          and length(term) >= 4
        group by term
        order by mentions desc
        limit :limit;
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit}).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    results = [TopProductItem(term=r[0], mentions=int(r[1])) for r in rows]
    return TopProductsResponse(limit=limit, results=results)


# 2) Channel Activity
@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse)
def channel_activity(channel_name: str):
    """
    Daily post counts and average views for a specific channel.
    """
    if engine is None:
        raise HTTPException(status_code=500, detail="Database engine not initialized")

    sql = text("""
        select
            d.full_date::text as date,
            count(*) as posts,
            avg(m.view_count)::float as avg_views
        from analytics.fct_messages m
        join analytics.dim_channels c on m.channel_key = c.channel_key
        join analytics.dim_dates d on m.date_key = d.date_key
        where c.channel_name = :channel_name
        group by d.full_date
        order by d.full_date;
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"channel_name": channel_name.strip().lower()}).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    if not rows:
        raise HTTPException(status_code=404, detail=f"Channel not found: {channel_name}")

    daily = [
        ChannelActivityItem(
            date=r[0],
            posts=int(r[1]),
            avg_views=float(r[2]) if r[2] is not None else None
        )
        for r in rows
    ]
    return ChannelActivityResponse(channel_name=channel_name.strip().lower(), daily=daily)


# 3) Message Search
@app.get("/api/search/messages", response_model=MessageSearchResponse)
def search_messages(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=200)
):
    """
    Search messages containing a keyword (case-insensitive).
    """
    if engine is None:
        raise HTTPException(status_code=500, detail="Database engine not initialized")

    sql = text("""
        select
            m.message_id,
            c.channel_name,
            d.full_date::text as message_date,
            m.view_count,
            m.forward_count,
            m.has_image,
            m.message_text
        from analytics.fct_messages m
        join analytics.dim_channels c on m.channel_key = c.channel_key
        join analytics.dim_dates d on m.date_key = d.date_key
        where m.message_text ilike :pattern
        order by d.full_date desc, m.view_count desc
        limit :limit;
    """)

    pattern = f"%{query}%"

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"pattern": pattern, "limit": limit}).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    results = [
        MessageSearchItem(
            message_id=int(r[0]),
            channel_name=r[1],
            message_date=r[2],
            views=int(r[3]) if r[3] is not None else 0,
            forwards=int(r[4]) if r[4] is not None else 0,
            has_image=bool(r[5]),
            message_text=r[6] or ""
        )
        for r in rows
    ]

    return MessageSearchResponse(query=query, limit=limit, results=results)


# 4) Visual Content Stats
@app.get("/api/reports/visual-content", response_model=VisualContentResponse)
def visual_content():
    """
    Stats about image usage across channels.
    """
    if engine is None:
        raise HTTPException(status_code=500, detail="Database engine not initialized")

    sql = text("""
        select
            c.channel_name,
            sum(case when m.has_image then 1 else 0 end) as posts_with_images,
            count(*) as total_posts,
            (sum(case when m.has_image then 1 else 0 end)::numeric / nullif(count(*),0))::float as image_rate
        from analytics.fct_messages m
        join analytics.dim_channels c on m.channel_key = c.channel_key
        group by c.channel_name
        order by posts_with_images desc;
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    results = []
    for r in rows:
        image_pct = round((float(r[3]) * 100.0) if r[3] is not None else 0.0, 2)
        results.append(
            VisualContentItem(
                channel_name=r[0],
                posts_with_images=int(r[1]) if r[1] is not None else 0,
                total_posts=int(r[2]) if r[2] is not None else 0,
                image_pct=image_pct
            )
        )

    return VisualContentResponse(results=results)
