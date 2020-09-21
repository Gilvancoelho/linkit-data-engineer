package linkittest

import java.io.File
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.sum


/**
 * Created by Gilvan Coelho 2020-09-20
 * Project: linkittest data engineer
 * This is the App Main - Linktest Entry Point
 * Create a Pipeline App that do:
 * move data to hdfs -> create tables in hive -> populate tables in Hive -> generate query ->
 * create hbase tables -> populate hbase tables -> Stream Data from Kafka
 */

object HDFSHiveMain extends App with SparkConn {


  val movedata = new MoveData()

  val hive = new HiveTable()

  movedata.UploadHiveDir(SOURCE_PATH, HDFS_PATH)

  val files = movedata.getCSVFiles(new File(SOURCE_PATH))

  val df_files = movedata.getDataFramesMap(files)

  df_files.foreach(dfmap => {
    //create hive table with
    hive.createHiveTableForCSVList(dfmap._2, DBNAME, dfmap._1, HDFS_PATH, true)
  })


  val result_agregation = hive.getAgregatedData(DBNAME)

  result_agregation.show()

}


class MoveData extends SparkConn {
  //val log = Logger.getLogger(FileName.getClass)
  val hadoopConf = new Configuration()
  val hdfs =  FileSystem.get(new java.net.URI("hdfs://sandbox-hdp.hortonworks.com:8020"), hadoopConf)

  /**
   * This method copy files from a directory to a destination into HDFS.
   * * The destination folder have the same name of the database and file
   * * example: mywarehouse/mydatabase/filename/filename.csv
   *
   * @param srcPath - source path
   * @param destPath - destination path
   */


  def UploadHiveDir(srcPath:String, destPath:String): Unit = {

    log.info("*** Getting a list of files")
    val filesList = getListOfCSVFiles(new File(srcPath))

    log.info("*** Put files into HDFS")

    filesList.foreach( file => {
      val hdfsPath =  new Path ( destPath+"/" + removeFileExtensions(file.getName))
      try {
        hdfs.copyFromLocalFile( new Path(srcPath +"/"+ file.getName ),hdfsPath)

      }
      catch { case e : Throwable => { e.printStackTrace()} }
    })
  }


  /**
   *This method receive a java.io.File as parameter and return a Map [String, DataFrame]
   * containing a Dataframe for each file into a path
   *example: Map (filename -> filenameDF)
   *
   * @param files
   * @return
   */
  def getDataFramesMap(files: List[File]): Map[String, DataFrame] = {
    files.map(file =>
      (removeFileExtensions(file.getName),loadCsvAsDataFrame(file.getAbsolutePath))).toMap
  }

  def loadCsvAsDataFrame(path:String) ={
    sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("file:///" + path)
  }

  /**
   * The method return only csv files as a list from a directory.
   *
   * @param dir
   * @return List[File]
   */
  def getCSVFiles(dir: File): List[File] = {
    if (dir.exists()) dir.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv")).toList
    else List[File]()
  }

  /**
   * Remove the filename extension and special characters.
   *
   * @param file
   * @return => string containing a filename without extension
   */
  def removeFileExtensions(file:String):String = {
    if (file.contains("."))
      file.dropRight(file.length - file.lastIndexOf(".")).replaceAll("[\\\\/:*?\"<>|]", "")
    else file

  }
}

class HiveTable extends SparkConn {

  /**
   * Created by Gilvan Coelho 2020-09-20
   * Project: linkit data engineer test
   * This package will Create a Hive Table in order to keep
   */

  /**
   * This method create a table for csv file that have been moved to HDFS.
   * The hive table have
   * example: folder/dangerous_drivers.csv =>
   *
   * @param df - dataframe loaded from a csv file
   * @param dbname - name of database
   * @param tablename - table name
   * @param destPath - warehouse database folder
   *
   * @param hiveExternalTable - option to create Hive managed table
   */
  def createHiveTableForCSVList(df:DataFrame, dbname:String, tablename:String, destPath: String, formathive:String hiveExternalTable:Boolean): Unit ={

    val formattedDf = removeHyphen(df)
    val fullDestPath = destPath.concat(dbname).concat("/").concat(tablename)
    val fieldsStr = createFieldString(formattedDf)


    if(!sparkSession.catalog.tableExists(s"${dbname}.${tablename}")){
      //creating hive table
      var query =""

      if(hiveExternalTable){

        //At this point I would like to put this query into a external file facilitating changes and table configuration
        query = s"CREATE EXTERNAL TABLE IF NOT EXISTS  ${dbname}.${tablename} (${fieldsStr} ) " +
          s"STORED AS ${formathive} LOCATION '${fullDestPath}'"
      }else

        query = s"CREATE TABLE IF NOT EXISTS  ${dbname}.${tablename} (${fieldsStr} ) "

      sql(query)
    }
    //save to HDFS
    formattedDf.write.mode(SaveMode.Append).orc(fullDestPath)
  }

  /**
   * This method removes dash sign(-) from column name that is not supported by Hive
   *
   * @param df
   * @return
   */

  def removeHyphen(df: DataFrame): DataFrame = {
    val columnName = df.columns.toSeq.map(columnName => columnName.replace("-","_"))
    df.toDF(columnName:_*)
  }


  def createFieldString(df: DataFrame):String ={
    var fieldsStr = ""
    df.schema.fields.foreach(f => {
      fieldsStr += s" " +f.name +" " +f.dataType.typeName + ","
    }
    )

    //patch permit us slice a character in text/word: ex: in this case = ...int, ) to ...int )
    fieldsStr.patch(fieldsStr.lastIndexOf(','), "", 1).replace("integer","int")
  }

  def getTable(dbName:String, tableName:String): DataFrame ={
    sql(s"select * from $dbName.$tableName")
  }

  def getAgregatedData(dbName:String): Dataframe ={
    sql(s"select d.driveid, d.name, sum(hourslogged) as HOURS_LOGGED, sum(mileslogged) as MILES_LOGGED  from $dbName.drivers d left join $dbName.timesheet t on d.driverid = t.driverid " )

  }

}
