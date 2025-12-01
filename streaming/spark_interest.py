from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType

import psycopg2
import json

# 1. Spark Session
spark = SparkSession.builder \
    .appName("ClickstreamInterestUpdater") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "user_events"

# 2. Schema of events
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event", StringType()),
    StructField("content_id", StringType()),
    StructField("category", StringType()),
    StructField("timestamp", StringType())
])

# 3. Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) AS json_str")

df_parsed = df_json.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

# 4. Event weights (like Instagram-style)
df_weighted = df_parsed.withColumn(
    "weight",
    when(col("event") == "like", 3.0)
    .when(col("event") == "comment", 4.0)
    .when(col("event") == "share", 5.0)
    .otherwise(1.0)   # default for 'view' etc.
)

# 5. Aggregate per user+category in each micro-batch
df_agg = df_weighted.groupBy("user_id", "category").sum("weight") \
    .withColumnRenamed("sum(weight)", "delta_score")

def upsert_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    conn = psycopg2.connect(
        host="localhost",
        database="clickstream_db",
        user="postgres",
        password="xyz"  # your password
    )
    cur = conn.cursor()

    for row in rows:
        user_id = row["user_id"]
        category = row["category"]
        delta = float(row["delta_score"])

        cur.execute("""
            INSERT INTO user_interest (user_id, category, score)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, category)
            DO UPDATE SET score = user_interest.score + EXCLUDED.score;
        """, (user_id, category, delta))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Upserted {len(rows)} rows into user_interest")

query = df_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(upsert_batch) \
    .option("checkpointLocation", "file:///C:/tmp/clickstream_interest_checkpoint") \
    .start()

query.awaitTermination()
