#consumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Define schema based on your data
schema = StructType() \
    .add("Month", StringType()) \
    .add("Incident Number", StringType()) \
    .add("Date of Incident", StringType()) \
    .add("Number of Victims under 18", IntegerType()) \
    .add("Number of Victims over 18", IntegerType()) \
    .add("Number of Offenders under 18", IntegerType()) \
    .add("Number of Offenders over 18", IntegerType()) \
    .add("Race/Ethnicity of Offenders", StringType()) \
    .add("Offense(s)", StringType()) \
    .add("Offense Location", StringType()) \
    .add("Bias", StringType()) \
    .add("Zip Code", StringType()) \
    .add("APD Sector", StringType()) \
    .add("Council District", IntegerType())

# Create Spark Session with S3 and Kafka configs
spark = SparkSession.builder \
    .appName("KafkaCSVConsumer") \
    .config("spark.hadoop.fs.s3a.access.key", "YourAccessKey") \
    .config("spark.hadoop.fs.s3a.secret.key", "YourSecretKey") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                                    "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

# Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092") \
    .option("subscribe", "YourBucketName") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="YourAccessKey" password="YourSecretKey') \
    .load()

# Parse the value as JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write to S3 as Parquet
query = df_parsed.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://hatecrimes/checkpoints/TransformedData/") \
    .option("path", "s3a://hatecrimes/TransformedData/") \
    .start()
# yet some changes need to be done, some transformation here 
query.awaitTermination()
