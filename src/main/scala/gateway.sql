CREATE KEYSPACE IF NOT EXISTS accesslog WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS accesslog(
   app_type text,
   bytes_in int,
   bytes_out int,
   host text,
   latency int,
   method text,
   path text,
   referer text,
   remote_ip text,
   response_code int,
   status int,
   time timestamp,
   topic text,
   type text,
   uri text,
   user_agent text,
   user_id bigint,
   trace_id text,
   PRIMARY KEY (user_id, time, trace_id)
) WITH CLUSTERING ORDER BY (time desc)