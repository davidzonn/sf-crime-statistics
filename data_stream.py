import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

BOOTSTRAP_SERVERS = "localhost:9092"
SPARK_MASTER = "spark://192.168.2.20:7077"
TOPIC_NAME = "com.davidzonn.sf-crimes"

schema = StructType([
    StructField("disposition", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
])

def run_spark_job(spark):

    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
          .option("subscribe", TOPIC_NAME)
          .option("maxOffsetsPerTrigger", 100)
          .option("stopGracefullyOnShutdown", "true")
          .option("spark.default.parallelism", 4)
          .option("spark.streaming.kafka.maxRatePerPartition", 100)
          .load()
          )
    # Show schema for the incoming resources for checks
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    distinct_table = service_table.select(psf.col("original_crime_type_name").alias("crime_name"), "disposition")

    agg_df = distinct_table.groupBy("crime_name", "disposition").count()

    radio_code_json_filepath = "res/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = (agg_df
                  .join(radio_code_df, "disposition")
                  .writeStream
                  .outputMode("complete")
                  .format("console")
                  .start())

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = (SparkSession
        .builder
        .master(SPARK_MASTER)
        .config("spark.ui.port", 3000)
        .appName("KafkaSparkStructuredStreaming")
        .getOrCreate())

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
