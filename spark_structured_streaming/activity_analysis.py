from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# You only need to install pyspark, other required packages are mentioned below and will be downloaded, installed during
## runtime

# Be careful to check the version of Scala in each package, it's often 2.12 or 2.13
packages = [
        'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1',
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
        'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1',
        'org.apache.kafka:kafka-clients:2.1.1',
        'org.apache.commons:commons-pool2:2.8.0'
    ]

BOOTSTRAP_SERVER = 'localhost:9092'

# Change topic name
TOPIC = 'rate-events'

# Create spark session
spark = SparkSession.builder \
        .master('local') \
        .appName('kafka-example') \
        .config('spark.jars.packages',','.join(packages)) \
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Create streaming dataframe
streaming_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers',BOOTSTRAP_SERVER) \
    .option('subscribe', TOPIC) \
    .option('startingOffsets', 'latest') \
    .load()

# Create streaming query
streaming_query = streaming_df \
        .select(streaming_df.value.cast('string').alias('parsed_value')) \
        .withColumn('movieId', expr("get_json_object(parsed_value, '$.movieId')")) \
        .withColumn('userId', expr("get_json_object(parsed_value, '$.userId')")) \
        .withColumn('action', expr("get_json_object(parsed_value, '$.action')")) \
        .withColumn('timestamp', to_timestamp(expr("get_json_object(parsed_value, '$.timestamp')"))) \
        .withColumn('date', to_date('timestamp', 'yyyy-MM-dd')) \
        .withColumn('time', date_format('timestamp', 'HH:mm:ss')) \
        .withColumn('partition', col('date')) \
        .select('userId', 'movieId','action','time','date', 'partition')


# To track your progress, access localhost:4040/ for more information


# Set your own path and checkpointLocation
# path is where your file be sinked into
# checkpointLocation is where Spark save metadata, progress infomation, it can help recovering from failure
# This is Spark Structured Streaming, so that each checkpoint accept only one schema, if you want to use another schema,
## just change your checkpointLocation

# The data is partitioned by date, so that all the records which in a 'yyyy-MM-dd' date will be saved in a same folder named
# 'parition=yyyy-MM-dd'

streaming_sink = streaming_query \
    .writeStream \
    .outputMode('append') \
    .format('csv') \
    .partitionBy('partition') \
    .option('path', '/spark/activity/sink2/') \
    .option('checkpointLocation','/spark/checkpoint2/') \
    .start() \
    .awaitTermination()