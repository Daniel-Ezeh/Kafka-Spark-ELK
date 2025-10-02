from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKER = "localhost:29092,localhost:39092,localhost:49092"
KAFKA_BROKER= "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "finance_transactions"
AGGREGATE_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'   
CHECKPOINT_DIR = "/mnt/spark-checkpoints"
STATE_DIR = "/mnt/spark-state"


spark = (SparkSession.builder
            .appName("FinancialTransactionsProcessor")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
            .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
            .config('spark.sql.streaming.stateStore.stateStoreDir', STATE_DIR)
            .config('spark.sql.shuffle.partitions', 20)  # Adjust based on your cluster
         ).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_time", LongType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("is_international", StringType(), True),
    StructField("currency", StringType(), True)
])


kafka_stream = (spark.readStream
                .format("kafka")
                .option('kafka.bootstrap.servers', KAFKA_BROKER)
                .option('subscribe', SOURCE_TOPIC)
                .option('startingOffsets', 'earliest')
                ).load()

txns_df = (kafka_stream
          .selectExpr("CAST(value AS STRING) as json_str")
          .select(from_json(col("json_str"), transaction_schema).alias("data"))
          .select("data.*")
         )

txns_df = txns_df.withColumn('transaction_time_stamp', (col('transaction_time') / 1000).cast('timestamp'))


agg_df = txns_df.groupBy('merchant_id')\
            .agg(
                sum('amount').alias('total_amount'),
                count('*').alias('transaction_count')
            )

agg_query = agg_df \
            .withColumn('key', col('merchant_id').cast('string')) \
            .withColumn("value", to_json(struct(
            col("merchant_id"),
            col("total_amount"),
            col("transaction_count")
        ))).selectExpr("key", "value") \
            .writeStream \
            .format('kafka') \
            .outputMode('update') \
            .option('kafka.bootstrap.servers', KAFKA_BROKER) \
            .option('topic', AGGREGATE_TOPIC) \
            .option('checkpointLocation', f"{CHECKPOINT_DIR}/aggs") \
            .start().awaitTermination()
        





