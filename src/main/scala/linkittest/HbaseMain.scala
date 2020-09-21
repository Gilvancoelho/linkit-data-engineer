package linkittest

/**
 * Created by Gilvan Coelho 2020-09-20
 * Project: linkit data engineer test
 * This package will do Hbase actions related
 */

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._


object HbaseMain extends App with SparkConn {

  val hbase = new HbaseTable


  // create a table
  hbase.createHbaseTable("dangerous_driver")

  log.info("***************  Start to Create a Dangerous_Drive Table and Load data *****************")

 // read csv file to dangerous-drive
  val dangerous_drive = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",").csv(SOURCE_FILE_DD)

 // Cast a timestamo column as string
  val dangerous_drive_str = hase.castColString(dangerous_drive,"eventTime" )

  // create a Dataframe and row key
  val df_row_key_dd = hbase.createDFRowkey( dangerous_drive_str, "driverId", "truckID", "eventID","eventTime")

  //Writing Dataframe row key into Hbase
  hbase.writeHbaseTable( df_row_key_dd, hbase.dangerous_drive_catalog)

  log.info("***************  Start to Read and Load Extra-drive data    *****************************")

  // read csv file to extra-driver
  val extra_driver = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",").csv(SOURCE_FILE_ED)

  // Cast a timestamo column as string
  val extra_driver_str = hbase.castColString(extra_driver,"eventTime" )

  // create a Dataframe and row key
  val df_row_key_ed = hbase.createDFRowkey( extra_driver_str, "driverId", "truckID", "eventID","eventTime")

  // Writing Dataframe row key into Hbase
  hbase.writeHbaseTable( df_row_key_ed, hbase.dangerous_drive_catalog)

  log.info("***************  Start to Get and change data based on rowkey    *****************************")

  // Getting data from Hbase to change
  val dangerous_drive_df = hbase.loadHbase(hbase.dangerous_drive_catalog)

   // filter row_key to get data
  val df_dangerous_drive = hbase.loadHbase(hbase.dangerous_drive_catalog).where(col("rowkeyid") === DRIVER_ID + "|" + EVENT_ID + "|" + EVENT_TIME)

  //Update data in Database
  val update_route = df_dangerous_drive.withColumn("routeName", when(lower(col("routeName"))
    .equalTo("Santa Clara to San Diego".toLowerCase), lit("Los Angeles to Santa Clara")))
  writeDfHabase(update_route, hbase.dangerous_drive_catalog)

}

class HbaseTable extends SparkConn {
  /** This method creae a Hbase table
   *
   *  This receive a table namne and check if exists
   *  if not table will be created
   */
  def createHbaseTable(tableName: String) = {
    val conn = getConnected()
    val admin = conn.getAdmin

    val table = new HTableDescriptor(TableName.valueOf(tableName))
    table.addFamily(new HColumnDescriptor(CF_NAME).setCompressionType(Algorithm.NONE))

    if (admin.tableExists(table.getTableName))

      log.warn("The Table [" + table.getTableName.getNameAsString + "] is already existed.")

    else {
      try {

        println("Creating new table... ")
        admin.createTable(table)
        println("Done.")
      } catch {

        case e: Exception => e.printStackTrace()

      } finally {

        conn.close()

      }

    }

  }
  /** This method connect to hbase
   * Its receive a HBASE-HOST-PORT information to
   * connect
   */
  def getConnected(): Connection = {
    val config = HBaseConfiguration.create()
    config.set("hbase.master", HBASE_HOST_PORT)
    config.setInt("timeout", 180000)
    ConnectionFactory.createConnection(config)
  }

  /** Write data to Hbase Table
   *
   *  This method receive a Dataframe and a Catalog and
   *  write this innformation to Hbase
   */
  def writeHbaseTable(df: DataFrame, catalog: String) = {
    //put.add(rk, column, value)

    if (hasColumn(df, DEFAULT_ROWKEY)) {
      log.info("*** WRITING INTO HBASE ***")
      df.write
        .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    } else {

      //throw an exception
    }

  }

  /** Load data to hbase
   *
   *  loadHbase receive a Catalog information and
   *  Load data into Hbase
   *  https://blog.cloudera.com/spark-hbase-dataframe-based-hbase-connector/
   */
  def loadHbase(catalog: String): DataFrame = {
    sparkSession.read.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  /** Cast Colunms to String
   * This method receive a Dataframe and a Column name and
   * transform this column to String
   */
  def castColString(dataFrame: DataFrame,columnName:String)={
    dataFrame.withColumn(columnName, dataFrame(columnName).cast("String") )
  }

  /** Create a Rowkey Column to a Dataframe
   * This method receive a Dataframe and receive a Columns List (3)
   * and create a rowkey column to this Dataframe
   */
  def createDFRowkey(df: DataFrame, firstColumn: String, secColumn: String, thirdColumn:String,forthColumn:String): DataFrame = {
    df.withColumn(DEFAULT_ROWKEY, concat(col(firstColumn), lit("|"), col(secColumn),lit("|"),col(thirdColumn),lit("|"),col(forthColumn)))
  }

  /** This method defines a Catalog to for the schema mapping
   * Based on this catalog information will be feed in a rowkey and cf.
   */
  def dangerous_drive_catalog = s"""{
                                  |"table":{"namespace":"default", "name":"dangerous_driver"},
                                  |"rowkey":"key",
                                  |"columns":{
                                  |"rowkeyid":{"cf":"rowkey", "col":"key", "type":"string"},
                                  |"eventId":{"cf":"${CF_NAME}", "col":"eventId", "type":"int"},
                                  |"driverId":{"cf":"${CF_NAME}", "col":"driverId", "type":"int"},
                                  |"driverName":{"cf":"${CF_NAME}", "col":"driverName", "type":"string"},
                                  |"eventTime":{"cf":"${CF_NAME}", "col":"eventTime", "type":"string"},
                                  |"eventType":{"cf":"${CF_NAME}", "col":"eventType", "type":"string"},
                                  |"latitudeColumn":{"cf":"${CF_NAME}", "col":"latitudeColumn", "type":"double"},
                                  |"longitudeColumn":{"cf":"${CF_NAME}", "col":"latitudeColumn", "type":"double"},
                                  |"routeId":{"cf":"${CF_NAME}", "col":"routeId", "type":"int"},
                                  |"routeName":{"cf":"${CF_NAME}", "col":"routeName", "type":"string"},
                                  |"truckId":{"cf":"${CF_NAME}", "col":"truckId", "type":"int"}
                                  |}
                                  |}""".stripMargin

}
