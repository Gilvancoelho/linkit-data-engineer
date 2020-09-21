package linkittest

/**
 * Created by Gilvan Coelho 2020-09-20
 * Project: linkittest data engineer test
 * This Trait will be provided SparkSession connection
 * for all other Apps
 */

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



/** this Trait have a common Connection
 * It is shared with all class that need to
 * connect with Spark
 */
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
