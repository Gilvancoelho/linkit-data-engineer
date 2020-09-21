package object linkittest {


  /**
   * Created by Gilvan Coelho 2020-09-20
   * Project: linkittest data engineer test
   * This package object will keep all variables related to connections
   * in order to keep simple change variables if environment change.
   * All variables declared here are acessible in entire Package
   */


  val WAREHOUSE = "hdfs://quickstart.cloudera:8020/apps/hive/warehouse"

  /** Here there are Hbase Variables */

  val DB_NAME = "linkittestdb"
  val TABLE_NAME_DD = "dangerous_driver"
  val TABLE_NAME_ED = "extra_driver"
  val SOURCE_FILE_DD= "files/data-hbase/dangerous_driver/dangerous-driver.csv"
  val SOURCE_FILE_ED= "files/data-hbase/extra-driver/extra-driver.csv"
  val DRIVER_ID = "80"
  val EVENT_ID = "4"
  val EVENT_TIME = " XXXXX"


  val CF_NAME = "cf1"
  val DEFAULT_ROWKEY = "rowkeyid"
  val ZK_HOST = "localhost"
  val ZK_PORT = "2181"
  val HBASE_HOST_PORT = "localhost:6000"

  /** Here there are HDFS Connections Variable */

  val HDFS_PATH = "hdfs://quickstart.cloudera:8020/linkittest/data-spark/"
  val HDFS_TMP = "hdfs://quickstart.cloudera:8020/tmp/"
  val SOURCE_PATH = "/tmp/data-spark/"
  val DBNAME = "linkittestdb"

  /** Here there are Kafka Connections Variable */

  val KAFKA_SERVER_PORT = "localhost:9092"
  val KAFKA_TOPIC = "linkittest_topic_driver"
  val PATH_DEST = "linkittestdata-hbase/dangerous-driver"
  val WAREHOUSE_PATH = "/linkittest/data-spark/"


}
