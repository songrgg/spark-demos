name := "SparkDemos"

version := "1.0"

scalaVersion := "2.11.11"

//resolvers += "Maven2" at "https://repo1.maven.org/maven2"
resolvers += "wallstreetcn" at "http://maven.wallstcn.com/nexus/content/groups/public/"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.1",
  "org.apache.spark" %% "spark-streaming" % "2.1.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2",
  "org.apache.spark" %% "spark-sql" % "2.1.1"
)
