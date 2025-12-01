# collector_app.py
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import psycopg2
import json

KAFKA_BROKER = "localhost:9092"
EVENT_TOPIC = "userevents"

app = FastAPI()

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------- PostgreSQL connection helper ----------
def get_db():
    conn = psycopg2.connect(
        host="localhost",
        database="clickstream_db",
        user="postgres",
        password="xyz"   # your password
    )
    return conn

# ---------- Pydantic Model for /track ----------
class TrackEvent(BaseModel):
    user_id: str
    event: str
    content_id: str
    category: str
    timestamp: str


# ---------- 1) Event tracking endpoint ----------
@app.post("/track")
async def track(event: TrackEvent):
    event_dict = event.dict()
    producer.send(EVENT_TOPIC, event_dict)
    producer.flush()
    print("Event sent to Kafka:", event_dict)
    return {"status": "received", "data": event_dict}


# ---------- 2) Feed endpoint ----------
@app.get("/feed")
def get_feed(user_id: str, limit: int = 20):
    conn = get_db()
    cur = conn.cursor()

    # Get top 3 preferred categories
    cur.execute("""
        SELECT category
        FROM user_interest
        WHERE user_id = %s
        ORDER BY score DESC
        LIMIT 3
    """, (user_id,))
    rows = cur.fetchall()

    if not rows:
        # If no personalization yet → return general feed
        cur.execute("""
            SELECT content_id, title, category, media_url, like_count
            FROM content
            ORDER BY like_count DESC, created_at DESC
            LIMIT %s
        """, (limit,))
    else:
        top_categories = tuple(r[0] for r in rows)
        cur.execute(f"""
            SELECT content_id, title, category, media_url, like_count
            FROM content
            WHERE category IN %s
            ORDER BY like_count DESC, created_at DESC
            LIMIT %s
        """, (top_categories, limit))

    content_rows = cur.fetchall()
    cur.close()
    conn.close()

    feed = [
        {
            "content_id": r[0],
            "title": r[1],
            "category": r[2],
            "media_url": r[3],
            "like_count": r[4]
        }
        for r in content_rows
    ]

    return {"user_id": user_id, "feed": feed}
