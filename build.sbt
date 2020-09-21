name := "linkit-test"

version := "0.1"

scalaVersion := "2.10.5"

val hdpVersion = "2.6.4.0-91"
val sparkVersion = "2.2.2"
val hadoopVersion = "2.7.3"
val hbaseVersion = "1.1.2"
val phoenixVersion = "4.7.0"
val kafkaVersion = "0.10.1"


resolvers ++= Seq(
  "Hontonworks" at  "https://repo.hortonworks.com/content/repositories/releases/",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-common" % hbaseVersion,

  //kafka
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  //"org.json4s" %% "json4s-jackson" % "3.6.5",
  //log
  "log4j" % "log4j" % "1.2.17"

)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}