package com.carrefour.ingestion.commons.util

import com.carrefour.ingestion.commons.context.{FileSystemContext, SparkSessionContext}
import com.carrefour.ingestion.commons.context.impl.{FileSystemContextImpl, SparkSessionContextImpl}
import com.carrefour.ingestion.commons.exception.FatalException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}


object SqlUtils {

  val fs: FileSystemContext = FileSystemContextImpl
  val spark: SparkSessionContext = SparkSessionContextImpl

  val Logger: Logger = LoggerFactory.getLogger(getClass)

  def sql(path: String, args: String*): Option[DataFrame] = {

    val is = getClass.getResourceAsStream(path)
    if (is == null) {
      throw new FatalException("No existe el fichero: " + path)
    }
    var query = scala.io.Source.fromInputStream(is).mkString

    var i = 1
    for (arg <- args) {
      query = query.replace("$" + i, arg)
      i = i + 1
    }

    Logger.info(s"Hive Query File: $path")
    Some(spark.getSparkSession().sql(query))
  }

  def setTableAsExternal(fullTableName: String): Unit = {
    val queryFile = "/hql/setTableAsExternal.hql"
    sql(queryFile, fullTableName)
  }

  def setTableAsInternal(fullTableName: String): Unit = {
    val queryFile = "/hql/setTableAsInternal.hql"
    sql(queryFile, fullTableName)
  }

  def dropPartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit = {
    Logger.info(s"Dropping partition: $fullTableName(year=$year, month=$month, day=$day)")
    val queryFile = "/hql/dropPartitionYearMonthDay.hql"
    sql(queryFile, fullTableName, year.toString, month.toString, day.toString)
  }

  def purgePartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit = {
    Logger.info(s"Purging partition: $fullTableName(year=$year, month=$month, day=$day)")
    val locationPath = getPartitionLocationYearMonthDay(fullTableName, year, month, day)
    Logger.info(s"Deleting partition folder: $locationPath")
    fs.getFileSystem().delete(new Path(locationPath), true)

    dropPartitionYearMonthDay(fullTableName, year, month, day)
  }

  def getPartitionLocationYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): String = {
    val queryFile = "/hql/describeFormattedPartitionYearMonthDay.hql"
    sql(queryFile, fullTableName, year.toString, month.toString, day.toString)
      .get
      .filter("col_name like 'Location%'")
      .first()
      .getAs[String]("data_type")
  }
}