### Project Implementation

To get the version of the spark and scala in the docker environment, ssh into any of the spark node (either mastoer or worker) and enter `spark-shell --version`



docker exec -u root -it kafka-spark-elk-spark-master-1 \
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    jobs/spark_producer.py