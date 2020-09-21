package linkittest

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

object KafkaStreamMain extends App with SparkConn {

  //Reading files from kafka topic
  val read = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER_PORT)
    .option("subscribe", KAFKA_TOPIC)
    .load()
    .selectExpr("CAST(value AS STRING)")


  val write = read.writeStream
    .format("json")
    .outputMode("append")
    .option("failOnDataLoss", "false")
    .option("path", WAREHOUSE_PATH + PATH_DEST)
    .option("checkpointLocation", WAREHOUSE_PATH + "checkpoint")
    .start()


  write.awaitTermination()

    /** SCHEMA
    "eventId", "string"
    "driverId", "string"
    "driverName", "string"
    "eventTime", "string"
    "eventType", "string")
    "latitudeColumn", "string"
    "longitudeColumn", "string"
    "routeId", "string"
    "routeName", "string"
    "truckId", "string"
    **/

}
