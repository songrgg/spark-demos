import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class AccessLog(
                      app_type: String,
                      bytes_in: Int,
                      bytes_out: Int,
                      host: String,
                      latency: Int,
                      method: String,
                      path: String,
                      referer: String,
                      remote_ip: String,
                      response_code: Int,
                      status: Int,
                      time: String,
                      topic: String,
                      `type`: String,
                      uri: String,
                      user_agent: String,
                      user_id: Long,
                      trace_id: String
                    )

object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k, v) => k.name -> v })
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json: String)(implicit m: Manifest[V]) = fromJson[Map[String, V]](json)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }
}

/**
  * Created by songrgg on 17-7-3.
  */
class AccessLogParser {
  def parser(json: String): AccessLog = {
    JsonUtil.fromJson[AccessLog](json)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AccessLogParser")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = "accesslog".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    messages.map(_._2)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .saveToCassandra("streaming_test", "words", SomeColumns("word", "count"))

    messages.map(_._2)
      .map(parser)
      .saveToCassandra("accesslog", "accesslog", SomeColumns(
        "app_type",
        "bytes_in",
        "bytes_out",
        "host",
        "latency",
        "method",
        "path",
        "referer",
        "remote_ip",
        "response_code",
        "status",
        "time",
        "topic",
        "type",
        "uri",
        "user_agent",
        "user_id",
        "trace_id"
      ))

    ssc.start()
    ssc.awaitTermination()
  }

}
