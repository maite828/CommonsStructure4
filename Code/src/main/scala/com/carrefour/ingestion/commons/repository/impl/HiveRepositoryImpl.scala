package com.carrefour.ingestion.commons.repository.impl

import com.carrefour.ingestion.commons.core.Contexts
import com.carrefour.ingestion.commons.exception.logging.CommonsException
import com.carrefour.ingestion.commons.repository.manage.queries.{CommonsAlterQueries, CommonsDropQueries, CommonsLoadQueries, CommonsShowQueries}
import com.carrefour.ingestion.commons.repository.{FileSystemRepository, HiveRepository, SparkSessionRepository}
import com.carrefour.ingestion.commons.service.impl.ExtractServiceImpl._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

/**
  *
  */
object HiveRepositoryImpl extends HiveRepository with CommonsLoadQueries with CommonsAlterQueries  with CommonsDropQueries with CommonsShowQueries {

  private val spark: SparkSessionRepository = SparkSessionRepositoryImpl
  private val dfs: FileSystemRepository = FileSystemRepositoryImpl

  override def sql(query:String):DataFrame = {
    Contexts.spark.getSparkSession().sql(query)
  }

  /**
    *
    * @param fullTableName
    */
  override def setTableAsExternal(fullTableName: String): Unit = {

    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
    val msgError: String = s"$methodName > Error al intentar establecer la tabla como INTERNA"
    initLog(methodName)

    val query = sQuerySetTableAsExternal(fullTableName)
    infoLog(query)

    val result = spark.getSparkSession().sql(query)

    if (result != true) throw CommonsException(msgError)
    endLog(methodName)
  }

  /**
    *
    * @param fullTableName
    */
  override def setTableAsInternal(fullTableName: String): Unit = {

    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
    val msgError: String = s"$methodName > Error al intentar establecer la tabla como EXTERNA"
    initLog(methodName)

    val query = sQuerySetTableAsExternal(fullTableName)
    infoLog(query)

    val result = spark.getSparkSession().sql(query)
    warnLog(result.toString())

    if (result != true) throw CommonsException(msgError)
    endLog(methodName)

  }

  /**
    *
    * @param fullTableName
    * @param year
    * @param month
    * @param day
    */
  override def dropPartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit = {

    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
    val msgError: String = s"$methodName > Error al intentar borrar la partición"
    initLog(methodName)

    val query = sQueryDropPartitionYearMonthDay(fullTableName, year, month, day)
    infoLog(query)

    val result = spark.getSparkSession().sql(query)
    warnLog(result.toString())

    if (result != true) throw CommonsException(msgError)
    endLog(methodName)

  }

  /**
    *
    * @param fullTableName
    * @param year
    * @param month
    * @param day
    */
  override def purgePartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit = {
    //    Logger.info(s"Purging partition: $fullTableName(year=$year, month=$month, day=$day)")
    val locationPath = getPartitionLocationYearMonthDay(fullTableName, year, month, day)
    //Logger.info(s"Deleting partition folder: $locationPath")
    dfs.getFileSystem().delete(new Path(locationPath), true)

    dropPartitionYearMonthDay(fullTableName, year, month, day)
  }

  /**
    *
    * @param fullTableName
    * @param year
    * @param month
    * @param day
    * @return
    */
  override def getPartitionLocationYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): String = {

    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
    val msgError: String = s"$methodName > Error al intentar hacer el describe"
    initLog(methodName)

    val query = sQueryDescribeFormattedPartition(fullTableName, year, month, day)
    infoLog(query)

    val result = spark.getSparkSession().sql(query)
    warnLog(result.toString())

    if (result.count() != 0) throw CommonsException(msgError)
    endLog(methodName)

    result.toString()
  }
}
