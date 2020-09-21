package linkittest

/**
 * Created by Gilvan Coelho 2020-09-20
 * Project: linkittest data engineer
 * This is the App Main - Linktest Entry Point
 * Create a Pipeline App that do:
 * move data to hdfs -> create tables in hive -> populate tables in Hive -> generate query ->
 * create hbase tables -> populate hbase tables -> Stream Data from Kafka
 */

import java.io.File

import linkittest.HbaseMain.log
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.sum



object HDFSHiveMain extends App with SparkConn {


  val movedata = new MoveData()

  val hive = new HiveTable()

  log.info("***************  Start to Moving Data    *****************************")


  movedata.UploadHiveDir(SOURCE_PATH, HDFS_PATH)

  val files = movedata.getCSVFiles(new File(SOURCE_PATH))

  val df_files = movedata.getDataFramesMap(files)

  log.info("***************  Start to Creae hive table    *****************************")


  df_files.foreach(dfmap => {
    //create hive table with
    hive.createHiveTableForCSVList(dfmap._2, DBNAME, dfmap._1, HDFS_PATH, true)
  })

  log.info("***************  Start doing agregatee Query  *****************************")

  val result_agregation = hive.getAgregatedData(DBNAME)

  result_agregation.show()

}


class MoveData extends SparkConn {
  //val log = Logger.getLogger(FileName.getClass)
  val hadoopConf = new Configuration()
  val hdfs =  FileSystem.get(new java.net.URI(DEFAULT_URI), hadoopConf)


  /**
   *This method receive a path source and path destination
   * and move files from one to another
   *
   */
  def UploadHiveDir(srcPath:String, destPath:String): Unit = {

    val filesList = getListOfCSVFiles(new File(srcPath))

    filesList.foreach( file => {
      val hdfsPath =  new Path ( destPath+"/" + removeFileExtensions(file.getName))
      try {
        hdfs.copyFromLocalFile( new Path(srcPath +"/"+ file.getName ),hdfsPath)

      }
      catch { case e : Throwable => { e.printStackTrace()} }
    })
  }


  /**
   *This method receive a java.io.File as parameter and return a
   * Map [String, DataFrame]
   * containing a Dataframe for each file into a path
   *
   */
  def getDataFramesMap(files: List[File]): Map[String, DataFrame] = {
    files.map(file =>
      (removeFileExtensions(file.getName),loadCsvAsDataFrame(file.getAbsolutePath))).toMap
  }

  /**
   *This method receive a Path and return
   * a dataframe read by Spark
   */
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
   * This method create a table for csv file that have been moved to HDFS.
   * The hive table have

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
   * This methods receive a dataframe
   * and returns other dataframe with a
   * columns anme adjusted replace hyphen to undescore
   *
   */
  def removeHyphen(df: DataFrame): DataFrame = {
    val columnName = df.columns.toSeq.map(columnName => columnName.replace("-","_"))
    df.toDF(columnName:_*)
  }

  /** This Method receive a Dataframe
   *  and generate as result a formated String
   *  with field name and datatypes.
   */
  def createFieldString(df: DataFrame):String ={
    var fieldsStr = ""
    df.schema.fields.foreach(f => {
      fieldsStr += s" " +f.name +" " +f.dataType.typeName + ","
    }
    )

    //patch permit us slice a character in text/word: ex: in this case = ...int, ) to ...int )
    fieldsStr.patch(fieldsStr.lastIndexOf(','), "", 1).replace("integer","int")
  }

  /** This Method send a Query to Hive Database
   *  and receive a Dataframe as result
   *  receive a database and tablename as parameter
   */
  def getTable(dbName:String, tableName:String): DataFrame ={
    sql(s"select * from $dbName.$tableName")
  }
  /** This Method send a Query to Hive Database
   *  and receive a Dataframe as result
   *  Agregated as required
   */
  def getAgregatedData(dbName:String): Dataframe ={
    sql(s"select d.driveid, d.name, sum(hourslogged) as HOURS_LOGGED, sum(mileslogged) as MILES_LOGGED  from $dbName.drivers d left join $dbName.timesheet t on d.driverid = t.driverid " )

  }

}
