from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, ArrayType

# Snowflake connection details
sfOptions = {
    "sfURL": "vdelgvk-yob19107.snowflakecomputing.com",
    "sfUser": "SNOWASHISH",
    "sfDatabase": "TECH_INTEL_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "TECH_WH",
    "sfRole": "ACCOUNTADMIN",
    "pem_private_key": open("/home/ashishunix/snowflake_keys/rsa_key_pkcs8.pem").read().replace("\n", "").replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", ""),
    "streaming_stage": "MY_INT_STAGE"
    #"tempDir": "file:///tmp"
}

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaToSnowflake") \
    .getOrCreate()

# Define schema for incoming Kafka JSON
schema = StructType() \
    .add("event_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("headline", StringType()) \
    .add("url", StringType()) \
    .add("source", StringType()) \
    .add("company", ArrayType(StringType())) \
    .add("topic", StringType()) \
    .add("sentiment", StringType()) \
    .add("risk_score", StringType())

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "enriched-tech-news") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse Kafka value as JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")


# ✅ FINAL: Write parsed Kafka data to Snowflake

from pyspark.sql.functions import expr, col, to_timestamp, to_json

parsed_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp"))

deduped_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["event_id"]) \
    .select(
        to_timestamp(col("timestamp")).alias("TIMESTAMP"),
        col("event_id").alias("EVENT_ID"),
        col("headline").alias("HEADLINE"),
        col("url").alias("URL"),
        col("source").alias("SOURCE"),
        to_json(col("company")).alias("COMPANY"),
        col("topic").alias("TOPIC"),
        col("sentiment").alias("SENTIMENT"),
        col("risk_score").alias("RISK_SCORE")
    )

'''
deduped_df = parsed_df.select(
    to_timestamp(col("timestamp")).alias("TIMESTAMP"),
    col("event_id").alias("EVENT_ID"),
    col("headline").alias("HEADLINE"),
    col("url").alias("URL"),
    col("source").alias("SOURCE"),
    to_json(col("company")).alias("COMPANY"),
    col("topic").alias("TOPIC"),
    col("sentiment").alias("SENTIMENT"),
    col("risk_score").alias("RISK_SCORE")
#)
).dropDuplicates(["event_id"])
'''

query = deduped_df.writeStream \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "TECH_NEWS_EVENTS") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/snowflake_checkpoint") \
    .start()



query.awaitTermination()
