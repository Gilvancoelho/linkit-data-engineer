package linkittest

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkConn extends Serializable {

  val log: Logger = LogManager.getLogger(getClass)

  val sparkConfig = new SparkConf()


  sparkConfig.set("spark.broadcast.compress", "false")
  sparkConfig.set("spark.shuffle.compress", "false")
  sparkConfig.set("spark.shuffle.spill.compress", "false")

  SparkSession.builder()
    .master("local[2]").enableHiveSupport()
    .appName("DataEngTest").config(sparkConfig).getOrCreate()


}
