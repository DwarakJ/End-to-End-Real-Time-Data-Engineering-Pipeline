import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

pg_properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}


def initialize_spark_session(app_name):
    """
    Initialize the Spark Session with provided configurations.
    
    :param app_name: Name of the spark application.
    :return: Spark session object or None if there's an error.
    """
    try:
        spark = SparkSession \
                .builder \
                .appName(app_name) \
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session initialized successfully')
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None


def get_streaming_dataframe(spark, brokers, topic):
    """
    Get a streaming dataframe from Kafka.
    
    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .option("delimiter", ",") \
            .option("startingOffsets", "earliest") \
            .load()
        logger.info("Streaming dataframe fetched successfully")
        return df

    except Exception as e:
        logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None


def transform_streaming_data(df):
    """
    Transform the initial dataframe to get the final structure.
    
    :param df: Initial dataframe with raw data.
    :return: Transformed dataframe.
    """
    schema = StructType([
        StructField("name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("city", StringType(), False),
        StructField("nation", StringType(), False),
        StructField("zip", IntegerType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
        StructField("email", StringType(), False)
    ])

    return (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

def load_streaming_data_to_postgres(df, jdbc_url, table_name, mode):
    """
    Load the transformed data to a PostgreSQL table.
    
    :param df: Transformed dataframe.
    :param jdbc_url: JDBC URL for the PostgreSQL database.
    :param table_name: Name of the table to write the data to.
    :param mode: Mode of writing data to the table.
    :return: None
    """
    
    logger.info("Loading streaming data to PostgreSQL...")
    query = df.writeStream \
    .foreachBatch(lambda df, _: df.write.jdbc(jdbc_url, table_name, mode=mode, properties=pg_properties)) \
    .outputMode("update") \
    .start()

    query.awaitTermination()


def main():
    app_name = "SparkStructuredStreamingToPostgres"
    if spark := initialize_spark_session(app_name):
        brokers = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
        topic = "names_topic"

        if df := get_streaming_dataframe(spark, brokers, topic):
            transformed_df = transform_streaming_data(df)
            load_streaming_data_to_postgres(transformed_df, "jdbc:postgresql://postgres_db:5432/customer_db", "retail.customers", "append")


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()
