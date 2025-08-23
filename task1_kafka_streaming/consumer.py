import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# --- Set Spark's log level to WARN to hide messy INFO logs ---
logging.getLogger("py4j").setLevel(logging.WARN)
logging.getLogger("pyspark").setLevel(logging.WARN)

# This schema must exactly match your CSV headers
SCHEMA = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("Record number", StringType(), True),
    StructField("Average Water Speed", StringType(), True),
    StructField("Average Water Direction", StringType(), True),
    StructField("Chlorophyll", StringType(), True),
    StructField("Chlorophyll [quality]", StringType(), True),
    StructField("Temperature", StringType(), True),
    StructField("Temperature [quality]", StringType(), True),
    StructField("Dissolved Oxygen", StringType(), True),
    StructField("Dissolved Oxygen [quality]", StringType(), True),
    StructField("Dissolved Oxygen (%Saturation)", StringType(), True),
    StructField("Dissolved Oxygen (%Saturation) [quality]", StringType(), True),
    StructField("pH", StringType(), True),
    StructField("pH [quality]", StringType(), True),
    StructField("Salinity", StringType(), True),
    StructField("Salinity [quality]", StringType(), True),
    StructField("Specific Conductance", StringType(), True),
    StructField("Specific Conductance [quality]", StringType(), True),
    StructField("Turbidity", StringType(), True),
    StructField("Turbidity [quality]", StringType(), True),
])

KAFKA_TOPIC = 'waterQuality'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
HDFS_PARQUET_PATH = 'hdfs://localhost:9000/user/student/water_quality_raw'
HDFS_CHECKPOINT_PATH = 'hdfs://localhost:9000/user/student/checkpoints_raw'


def create_spark_session():
    """Creates a Spark session."""
    return (
        SparkSession.builder
        .appName("KafkaToHDFSStreaming")
        .getOrCreate()
    )

# --- This is our custom function to print each batch ---
def print_batch_in_kv_format(batch_df, batch_id):
    """
    Takes a micro-batch DataFrame, collects the data, and prints each record
    in a clean key-value format.
    """
    if batch_df.count() > 0:
        print(f"--- Batch {batch_id} ---")
        # Collect the data to the driver. This is safe for small micro-batches.
        records = batch_df.collect()
        for record in records:
            # Convert the Spark Row to a Python dictionary
            record_dict = record.asDict()
            for key, value in record_dict.items():
                print(f"{key}: {value}")
            print("----------------------------------------") # Separator for records

def process_stream(spark):
    """Reads from Kafka and runs two parallel streams: one to HDFS and one for printing."""
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        df_json = df_stream.selectExpr("CAST(value AS STRING)")
        df_parsed = df_json.withColumn("data", from_json(col("value"), SCHEMA)).select("data.*")
        
        print("--- Kafka Consumer is now RUNNING ---")
        print("Pipeline is active. Press Ctrl+C to stop.")
        
        # --- STREAM 1: Write to HDFS (Silent) ---
        hdfs_query = (
            df_parsed.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", HDFS_PARQUET_PATH)
            .option("checkpointLocation", HDFS_CHECKPOINT_PATH)
            .trigger(processingTime='3 seconds')
            .start()
        )

        # --- STREAM 2: Write to Console using our custom function ---
        console_query = (
            df_parsed.writeStream
            .outputMode("append")
            .foreachBatch(print_batch_in_kv_format) # Use our custom function here
            .trigger(processingTime='3 seconds')
            .start()
        )

        console_query.awaitTermination()
        hdfs_query.awaitTermination()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nStopping the streaming queries...")
        spark.stop()

if __name__ == "__main__":
    spark_session = create_spark_session()
    process_stream(spark_session)
