import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by songrgg on 17-7-3.
  *
  * The access log is formatted as below, we use spark to process the log file line by line, and flush them into cassandra.
  * {
  * "app_type":null,
  * "bytes_in":0,
  * "bytes_out":0,
  * "host":"api-prod.wallstreetcn.com",
  * "latency": 0.069,
  * "latency_human":"69.755ms",
  * "level":"info",
  * "method":"GET",
  * "msg":"webaccess",
  * "path":"/apiv1/content/articles",
  * "referer":"https://wallstreetcn.com/articles/3016532",
  * "remote_ip": "123.123.123.123",
  * "response_code": 20000,
  * "status":200,
  * "time":"2017-06-21T05:41:05Z",
  * "topic":"user_activity",
  * "trace_id":null,
  * "type":"webaccess",
  * "uri":"/apiv1/content/articles",
  * "user_agent":"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
  * "user_id":1000000001
  * }
  */
object AccessLogParser {

  val r = scala.util.Random

  def parser(json: String): AccessLog = {
    val accesslog = JsonUtil.fromJson[AccessLog](json)

    if (accesslog.time.isEmpty) {
      return null
    }

    try {
      val d = DateTime.parse(accesslog.time)
      accesslog.copy(year = d.getYear, month = d.getMonthOfYear, day = d.getDayOfMonth, hour = d.getHourOfDay, trace_id = accesslog.trace_id + "_" + r.nextInt)
    } catch {
      case pe: java.text.ParseException =>
        accesslog.copy(year = 0, month = 0, day = 0, hour = 0, trace_id = accesslog.trace_id + "_" + r.nextInt)
      case e: Exception =>
        accesslog.copy(year = 0, month = 0, day = 0, hour = 0, trace_id = accesslog.trace_id + "_" + r.nextInt)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AccessLogParser")
    val sc = new SparkContext(sparkConf)
    val logData = sc.textFile("/home/songrgg/Desktop/ivankagateway.log", 2)
    logData
      .filter(line => line.contains("webaccess"))
      .map(parser)
      .saveToCassandra("accesslog", "accesslog", SomeColumns(
        "year",
        "month",
        "day",
        "hour",
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
    sc.stop()
  }

}
