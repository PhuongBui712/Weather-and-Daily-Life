import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, TimestampType, IntegerType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

def create_table_weather(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.weather (
        time TIMESTAMP PRIMARY KEY,
        cloud_base FLOAT,
        cloud_ceiling FLOAT,
        cloud_cover FLOAT,
        dew_point FLOAT,
        freezing_rain_intensity INT,
        humidity FLOAT,
        precipitation_probability INT,
        pressure_surface_level FLOAT,
        rain_intensity INT,
        sleet_intensity INT,
        snow_intensity INT,
        temperature FLOAT,
        temperature_apparent FLOAT,
        uv_health_concern INT,
        uv_index INT,
        visibility INT,
        weather_code INT,
        wind_direction FLOAT,
        wind_gust FLOAT,
        wind_speed FLOAT,
        latitude FLOAT,
        longitude FLOAT);
    """)

def create_table_live_service(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.live_service (
        datetime TEXT PRIMARY KEY,
        usual_popularity INT,
        place_id TEXT,
        name TEXT,
        rating FLOAT,
        rating_n INT,
        current_popularity FLOAT,
        monday LIST<INT>,
        tuesday LIST<INT>,
        wednesday LIST<INT>,
        thursday LIST<INT>,
        friday LIST<INT>,
        saturday LIST<INT>,
        sunday LIST<INT>);
    """)

def create_live_service_df_from_kafka(spark_df):
    schema = StructType([
        StructField("datetime", StringType(), False),
        StructField("usual_popularity", IntegerType()),
        StructField("place_id", StringType()),
        StructField("name", StringType()),
        StructField("rating", FloatType()),
        StructField("rating_n", IntegerType()),
        StructField("current_popularity", FloatType()),
        StructField("monday", ArrayType(IntegerType())),
        StructField("tuesday", ArrayType(IntegerType())),
        StructField("wednesday", ArrayType(IntegerType())),
        StructField("thursday", ArrayType(IntegerType())),
        StructField("friday", ArrayType(IntegerType())),
        StructField("saturday", ArrayType(IntegerType())),
        StructField("sunday", ArrayType(IntegerType())),
    ])

    live_service_df = spark_df.selectExpr("CAST(value AS STRING)", "topic").where("topic = 'live_service'") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return live_service_df

def create_weather_df_from_kafka(spark_df):
    schema = StructType([
        StructField("time", TimestampType(), False),
        StructField("cloud_base", FloatType()),
        StructField("cloud_ceiling", FloatType()),
        StructField("cloud_cover", FloatType()),
        StructField("dew_point", FloatType()),
        StructField("freezing_rain_intensity", IntegerType()),
        StructField("humidity", FloatType()),
        StructField("precipitation_probability", IntegerType()),
        StructField("pressure_surface_level", FloatType()),
        StructField("rain_intensity", IntegerType()),
        StructField("sleet_intensity", IntegerType()),
        StructField("snow_intensity", IntegerType()),
        StructField("temperature", FloatType()),
        StructField("temperature_apparent", FloatType()),
        StructField("uv_health_concern", IntegerType()),
        StructField("uv_index", IntegerType()),
        StructField("visibility", IntegerType()),
        StructField("weather_code", IntegerType()),
        StructField("wind_direction", FloatType()),
        StructField("wind_gust", FloatType()),
        StructField("wind_speed", FloatType()),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
    ])

    weather_df = spark_df.selectExpr("CAST(value AS STRING)", "topic").where("topic = 'weather'") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return weather_df

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
            .option('subscribe', 'weather,live_service') \
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
        weather_df = create_weather_df_from_kafka(spark_df)
        live_service_df = create_live_service_df_from_kafka(spark_df)
        
        # query1 = weather_df.selectExpr("*").writeStream.outputMode("append").format("console").start()
        # query2 = live_service_df.selectExpr("*").writeStream.outputMode("append").format("console").start()
        
        
        session = create_cassandra_connection()
        # query1.awaitTermination()
        # query2.awaitTermination()

        if session is not None:
            create_keyspace(session)
            create_table_weather(session)
            create_table_live_service(session)

            logging.info("Streaming is being started...")
            
            streaming_query1 = (weather_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/weatherCp')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'weather')
                                .start())
            
            streaming_query2 = (live_service_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/liveserviceCp')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'live_service')
                                .start())
            
            streaming_query1.awaitTermination()
            streaming_query2.awaitTermination()



    
