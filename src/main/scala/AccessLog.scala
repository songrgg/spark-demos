/**
  * Created by songrgg on 17-7-10.
  */
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
                      trace_id: String,
                      year: Int,
                      month: Int,
                      day: Int,
                      hour: Int
                    )