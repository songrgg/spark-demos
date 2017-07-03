Spark Streaming+Kafka+Cassandra
=====

### Install Spark

### Install Kafka

### Install Cassandra

### Develop In Scala

#### Submit application to the spark

```shell
$ ./bin/spark-submit --class "DirectKafkaWordCount" --master spark://{spark_host}:{spark_port} --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 sparkdemos_2.11-1.0.jar
```
