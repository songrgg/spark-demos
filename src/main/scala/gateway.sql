CREATE KEYSPACE IF NOT EXISTS accesslog WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS accesslog(
  year INT,
  month INT,
  day INT,
  hour INT,
  app_type TEXT,
  bytes_in INT,
  bytes_out INT,
  host TEXT,
  latency INT,
  method TEXT,
  path TEXT,
  referer TEXT,
  remote_ip TEXT,
  response_code INT,
  status INT,
  time TIMESTAMP,
  topic TEXT,
  type TEXT,
  uri TEXT,
  user_agent TEXT,
  user_id BIGINT,
  trace_id TEXT,
  PRIMARY KEY ((year, month, day, hour), time, trace_id)
) WITH CLUSTERING ORDER BY (time desc);


# User agent aggregated by hour.
CREATE TABLE IF NOT EXISTS user_agent_hourly(
  year INT,
  month INT,
  day INT,
  hour INT,
  userAgentRanks map<TEXT, bigint>,
  PRIMARY KEY ((year, month), day, hour)
) WITH CLUSTERING ORDER BY(day desc, hour desc);

INSERT INTO user_agent_hourly(year, month, day, hour, userAgentRanks) VALUES(2017, 10, 4, 1, { 'Safari': 1000000, 'Chrome': 10000000, 'All': 20000000 });
INSERT INTO user_agent_hourly(year, month, day, hour, userAgentRanks) VALUES(2017, 11, 1, 1, { 'Safari': 1000000, 'Chrome': 10000000, 'All': 20000000 });
INSERT INTO user_agent_hourly(year, month, day, hour, userAgentRanks) VALUES(2018, 1, 1, 1, { 'Safari': 1000000, 'Chrome': 10000000, 'All': 20000000 });
INSERT INTO user_agent_hourly(year, month, day, hour, userAgentRanks) VALUES(2018, 2, 1, 1, { 'Safari': 1000000, 'Chrome': 10000000, 'All': 20000000 });

# Response time percentile aggregated by hour.
CREATE TABLE IF NOT EXISTS response_time_percentile(
  year INT,
  month INT,
  day INT,
  hour INT,
  method TEXT,
  perc100 INT,
  perc999 INT,
  perc995 INT,
  perc99 INT,
  perc95 INT,
  perc75 INT,
  perc50 INT,
  perc25 INT,
  perc5 INT,
  perc1 INT,
  PRIMARY KEY ((year, month), day, hour, method)
) WITH CLUSTERING ORDER BY(day desc, hour desc);
