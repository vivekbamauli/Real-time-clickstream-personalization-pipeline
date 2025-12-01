from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2

# 1. Spark Session
spark = SparkSession.builder \
    .appName("ClickstreamInterestUpdater") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka config
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "userevents"

# Schema of incoming events
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event", StringType()),
    StructField("content_id", StringType()),
    StructField("category", StringType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")

parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# Assign weights based on event type
weighted_df = parsed_df.withColumn(
    "weight",
    when(col("event") == "like", 3.0)
    .when(col("event") == "view", 1.0)
    .otherwise(1.0)
)

agg_df = weighted_df.groupBy("user_id", "category").sum("weight") \
    .withColumnRenamed("sum(weight)", "score_inc")


def update_postgres(batch_df, batch_id):
    rows = batch_df.collect()
    conn = psycopg2.connect(
        host="localhost",
        database="clickstream_db",
        user="postgres",
        password="XYZ"
    )
    cur = conn.cursor()

    for row in rows:
        cur.execute("""
            INSERT INTO user_interest (user_id, category, score)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, category)
            DO UPDATE SET score = user_interest.score + EXCLUDED.score;
        """, (row["user_id"], row["category"], float(row["score_inc"])))

    conn.commit()
    cur.close()
    conn.close()
    print("Updated interest scores:", rows)


query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(update_postgres) \
    .option("checkpointLocation", "C:/tmp/clickstream_checkpoint") \
    .start()

query.awaitTermination()
