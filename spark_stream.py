import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType


# def create_keyspace(session):
#     session.execute("""
#         CREATE KEYSPACE IF NOT EXISTS `spark_streams`
#         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
#     """)

#     print("Keyspace created successfully!")

    # location['location_id'] = str(uuid.uuid4())
    # location['latitude'] = res['location']['lat']
    # location['longitude'] = res['location']['lon']
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.locations (
        location_id TEXT PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT);
    """)

    print("Table created successfully!")


# def insert_data(session, **kwargs):
#     print("inserting data...")

#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')

#     try:
#         session.execute("""
#             INSERT INTO spark_streams.users( id, first_name, last_name, gender, address, 
#                 post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, ( user_id, first_name, last_name, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {first_name} {last_name}")

#     except Exception as e:
#         logging.error(f'could not insert data due to {e}')


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
            .option('subscribe', 'locations') \
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


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("location_id", StringType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),

    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        query1 = selection_df.selectExpr("*").writeStream.outputMode("append").format("console").start()
        

        # session = create_cassandra_connection()

        # if session is not None:
        #     # create_keyspace(session)
        #     create_table(session)

        #     logging.info("Streaming is being started...")

        # streaming_query = (selection_df.writeStream.format("jdbc-streaming").option('driver', 'org.postgresql.Driver')
        #                     .option('url', 'jdbc:postgresql://localhost:5432/airflow')
        #                     .option('checkpointLocation', '/tmp/checkpoint1')
        #                     .option('table', 'locations')
        #                     .start())

        streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint1')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'locations')
                               .start())


        streaming_query.awaitTermination()
        query1.awaitTermination()
    
