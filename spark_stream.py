import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, TimestampType, IntegerType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table_weather(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.weather (
        time TIMESTAMP PRIMARY KEY,
        cloud_base FLOAT,
        cloud_ceiling FLOAT,
        cloud_cover INT,
        dew_point INT,
        freezing_rain_intensity INT,
        humidity INT,
        precipitation_probability INT,
        pressure_surface_level FLOAT,
        rain_intensity INT,
        sleet_intensity INT,
        snow_intensity INT,
        temperature INT,
        temperature_apparent FLOAT,
        uv_health_concern INT,
        uv_index INT,
        visibility FLOAT,
        weather_code INT,
        wind_direction INT,
        wind_gust FLOAT,
        wind_speed FLOAT,
        latitude FLOAT,
        longitude FLOAT);
    """)

    print("Table Weather created successfully!")

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("time", TimestampType(), False),
        StructField("cloud_base", FloatType()),
        StructField("cloud_ceiling", FloatType()),
        StructField("cloud_cover", IntegerType()),
        StructField("dew_point", IntegerType()),
        StructField("freezing_rain_intensity", IntegerType()),
        StructField("humidity", IntegerType()),
        StructField("precipitation_probability", IntegerType()),
        StructField("pressure_surface_level", FloatType()),
        StructField("rain_intensity", IntegerType()),
        StructField("sleet_intensity", IntegerType()),
        StructField("snow_intensity", IntegerType()),
        StructField("temperature", IntegerType()),
        StructField("temperature_apparent", FloatType()),
        StructField("uv_health_concern", IntegerType()),
        StructField("uv_index", IntegerType()),
        StructField("visibility", FloatType()),
        StructField("weather_code", IntegerType()),
        StructField("wind_direction", IntegerType()),
        StructField("wind_gust", FloatType()),
        StructField("wind_speed", FloatType()),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.5.0,"
                                            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                                           "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0,"
                                        #    "org.apache.commons:commons-pool2:1.5.4,"
                                           "org.apache.kafka:kafka-clients:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        # s_conn = SparkSession.builder \
        #     .appName('SparkDataStreaming') \
        #     .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        #                                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0,"
        #                                 #    "org.apache.commons:commons-pool2:1.5.4,"
        #                                    "org.apache.kafka:kafka-clients:3.5.0") \
        #     .config('spark.jars', "$SPARK_HOME/jars/postgresql-42.7.2.jar") \
        #     .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
            # .option('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'weather') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None




if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        query1 = selection_df.selectExpr("*").writeStream.format("console").start()
        
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table_weather(session)

            logging.info("Streaming is being started...")
            
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpointWeather')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'weather')
                                .start())
        # streaming_query = (selection_df.writeStream.format("jdbc-streaming").option('driver', 'org.postgresql.Driver')
        #                     .option('url', 'jdbc:postgresql://localhost:5432/airflow')
        #                     .option('checkpointLocation', '/tmp/checkpoint1')
        #                     .option('table', 'locations')
        #                     .start())

            streaming_query.awaitTermination()
            query1.awaitTermination()
    
