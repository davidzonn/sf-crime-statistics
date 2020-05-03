import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

BOOTSTRAP_SERVERS = "localhost:9092"
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
          .option("startingOffsets", "earliest")
          .option("maxOffsetsPerTrigger", 100)
          .option("stopGracefullyOnShutdown", "true")
          .load()
          )
    # Show schema for the incoming resources for checks
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    distinct_table = service_table.select(psf.col("original_crime_type_name").alias("crime_name"), "disposition")

    # count the number of original crime type
    agg_df = distinct_table.groupBy("crime_name").count()

    query = (agg_df.writeStream
             .outputMode("complete")
             .format("console")
             .start())


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # # TODO get the right radio code json path
    # radio_code_json_filepath = ""
    # radio_code_df = spark.read.json(radio_code_json_filepath)
    #
    # # clean up your data so that the column names match on radio_code_df and agg_df
    # # we will want to join on the disposition code
    #
    # # TODO rename disposition_code column to disposition
    # radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    #
    # # TODO join on disposition column
    # join_query = agg_df.pass
    #
    #
    # join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
