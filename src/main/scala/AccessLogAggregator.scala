import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

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
object AccessLogAggregator {

  val r = scala.util.Random

  def parser(row: CassandraRow): Unit = {
    row.getInt("year")
    row.getInt("month")
    row.getInt("day")
    row.getInt("hour")
    row.getString("time")
    row.getInt("latency")
    row.getString("method")
    row.getString("path")
    row.getString("referer")
    row.getString("remote_ip")
    row.getInt("response_code")
    row.getInt("status")
    row.getString("uri")
    row.getString("user_agent")
    row.getLong("user_id")
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AccessLogAggregator")
    val sc = new SparkContext(sparkConf)
    val year = 2017
    val month = 6
    val day = 21
    val hour = 5

    val rdd = sc.cassandraTable("accesslog", "accesslog")
      .select(
        "year", "month", "day", "hour", "time", "trace_id", "bytes_in", "bytes_out", "host",
        "latency", "method", "path", "referer", "remote_ip", "response_code", "status", "topic",
        "type", "uri", "user_agent", "user_id"
      )
      .where(s"year = $year and month = $month and day = $day and hour = $hour")
      .sortBy((row) => row.getInt("latency"), ascending = true)
      .collect()

    val percentiles = Seq(100.0, 99.9, 99.5, 99.0, 95.0, 75.0, 50.0, 25.0, 5.0, 1.0)
    val total = rdd.count(x => true)
    val list = new ListBuffer[Int]
    println("total is: " + total)
    percentiles.foreach(percentile => {
      println("fetch " + percentile + "%, now index is: " + Math.floor(percentile * (total - 1) / 100.0).toInt)
      list += rdd.apply(Math.floor(percentile * (total - 1) / 100.0).toInt).getInt("latency")
    })
    println(list)

    case class ResponseTimePercentile(
                                       year: Int,
                                       month: Int,
                                       day: Int,
                                       hour: Int,
                                       method: String,
                                       perc100: Int,
                                       perc999: Int,
                                       perc995: Int,
                                       perc99: Int,
                                       perc95: Int,
                                       perc75: Int,
                                       perc50: Int,
                                       perc25: Int,
                                       perc5: Int,
                                       perc1: Int
                                     )
    sc.parallelize(Seq((
      year, month, day, hour, "GET", list.head, list.apply(1), list.apply(2), list.apply(3), list.apply(4), list.apply(5), list.apply(6), list.apply(7), list.apply(8), list.last
      ))).saveToCassandra("accesslog", "response_time_percentile", SomeColumns(
      "year", "month", "day", "hour", "method", "perc100", "perc999", "perc995", "perc99", "perc95", "perc75", "perc50", "perc25", "perc5", "perc1"
    ))
    sc.stop()
  }
}
