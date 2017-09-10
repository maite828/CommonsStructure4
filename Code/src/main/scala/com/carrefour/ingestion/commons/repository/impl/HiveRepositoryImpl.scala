package com.carrefour.ingestion.commons.repository.impl

import com.carrefour.ingestion.commons.exception.FatalException
import com.carrefour.ingestion.commons.repository.HiveRepository
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

/**
  *
  */
object HiveRepositoryImpl extends HiveRepository{

  override def sql(path: String, args: String*): Option[DataFrame] = {
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

    //TODO Logger.info(s"Hive Query File: $path")
    Some(SparkSessionRepositoryImpl.getSparkSession().sql(query))
  }

  override def setTableAsExternal(fullTableName: String): Unit = {
    val queryFile = "/Code/src/main/resources/hql/setTableAsExternal.hql"
    sql(queryFile, fullTableName)
  }

  override def setTableAsInternal(fullTableName: String): Unit = {
    val queryFile = "/Code/src/main/resources/hql/setTableAsInternal.hql"
    sql(queryFile, fullTableName)
  }

  override def dropPartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit = {
    //    Logger.info(s"Dropping partition: $fullTableName(year=$year, month=$month, day=$day)")
    val queryFile = "/Code/src/main/resources/hql/dropPartitionYearMonthDay.hql"
    sql(queryFile, fullTableName, year.toString, month.toString, day.toString)
  }

  override def purgePartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit = {
    //    Logger.info(s"Purging partition: $fullTableName(year=$year, month=$month, day=$day)")
    val locationPath = getPartitionLocationYearMonthDay(fullTableName, year, month, day)
    //Logger.info(s"Deleting partition folder: $locationPath")
    FileSystemRepositoryImpl.getFileSystem().delete(new Path(locationPath), true)

    dropPartitionYearMonthDay(fullTableName, year, month, day)
  }

  override def getPartitionLocationYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): String = {
    val queryFile = "/Code/src/main/resources/hql/describeFormattedPartitionYearMonthDay.hql"
    sql(queryFile, fullTableName, year.toString, month.toString, day.toString)
      .get
      .filter("col_name like 'Location%'")
      .first()
      .getAs[String]("data_type")
  }
}
