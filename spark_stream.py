import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster

# Configure logging to see informative messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_connection():
    """Creates and configures the SparkSession."""
    try:
        # Configuration is now handled by spark-submit for better reliability
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session: {e}")
        return None

def setup_cassandra(session):
    """Creates the keyspace and table in Cassandra."""
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace 'spark_streams' created or already exists.")
        
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
        logging.info("Table 'created_users' created or already exists.")
    except Exception as e:
        logging.error(f"Could not set up Cassandra keyspace/table: {e}")
        raise

def connect_to_kafka(spark_conn):
    """Connects to Kafka and returns a streaming DataFrame."""
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka streaming DataFrame created successfully.")
        return spark_df
    except Exception as e:
        # This will now print a very clear error if the JAR is missing
        logging.error(f"Kafka DataFrame could not be created: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    """Transforms the Kafka streaming DataFrame."""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if not spark_conn:
        logging.critical("Failed to create Spark session. Exiting.")
        exit(1)

    # Setup Cassandra first using a direct driver connection
    try:
        cluster = Cluster(['cassandra'])
        cas_session = cluster.connect()
        setup_cassandra(cas_session)
        cas_session.shutdown()
    except Exception as e:
        logging.critical(f"Failed to setup Cassandra. Exiting. Error: {e}")
        exit(1)

    spark_df = connect_to_kafka(spark_conn)
    if not spark_df:
        logging.critical("Failed to connect to Kafka. Exiting.")
        exit(1)
        
    selection_df = create_selection_df_from_kafka(spark_df)

    logging.info("Starting the streaming query to Cassandra...")
    
    streaming_query = (selection_df.writeStream
                       .format("cassandra")
                       .option('checkpointLocation', '/tmp/checkpoint')
                       .option('keyspace', 'spark_streams')
                       .option('table', 'created_users')
                       .start())

    streaming_query.awaitTermination()