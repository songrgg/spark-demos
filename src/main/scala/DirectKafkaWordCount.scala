import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by songrgg on 17-6-7.
  */

object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = "accesslog".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    messages.map(_._2)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .saveToCassandra("streaming_test", "words", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
