package com.carrefour.ingestion.commons.util

import com.carrefour.ingestion.commons.exception.FatalException
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{LoggerFactory, Logger}


object SqlUtils {

  val Logger = LoggerFactory.getLogger(getClass)

  def sql(path: String, args: String*)(implicit sparkSession: SparkSession): Option[DataFrame] = {

    val is = getClass().getResourceAsStream(path)
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
    Some(sparkSession.sql(query))
  }

  def setTableAsExternal(fullTableName: String)(implicit sparkSession: SparkSession): Unit = {
    val queryFile="/hql/setTableAsExternal.hql"
    sql(queryFile, fullTableName)
  }

  def setTableAsInternal(fullTableName: String)(implicit sparkSession: SparkSession): Unit = {
    val queryFile="/hql/setTableAsInternal.hql"
    sql(queryFile, fullTableName)
  }

  def dropPartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int)(implicit sparkSession: SparkSession): Unit = {
    Logger.info(s"Dropping partition: ${fullTableName}(year=${year}, month=${month}, day=${day})")
    val queryFile="/hql/dropPartitionYearMonthDay.hql"
    sql(queryFile, fullTableName, year.toString, month.toString, day.toString)
  }

  def purgePartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int)(implicit sparkSession: SparkSession): Unit = {
    Logger.info(s"Purging partition: ${fullTableName}(year=${year}, month=${month}, day=${day})")
    val locationPath = getPartitionLocationYearMonthDay(fullTableName, year, month, day)
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    Logger.info(s"Deleting partition folder: $locationPath")
    fs.delete(new Path(locationPath), true)

    dropPartitionYearMonthDay(fullTableName, year, month, day)
  }

  def getPartitionLocationYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int)(implicit sparkSession: SparkSession): String = {
    val queryFile="/hql/describeFormattedPartitionYearMonthDay.hql"
    sql(queryFile, fullTableName, year.toString, month.toString, day.toString)
      .get
      .filter("col_name like 'Location%'")
      .first()
      .getAs[String]("data_type")
  }
}