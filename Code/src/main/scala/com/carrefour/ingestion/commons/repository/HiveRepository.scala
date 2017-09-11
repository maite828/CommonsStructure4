package com.carrefour.ingestion.commons.repository

import com.carrefour.ingestion.commons.controller.IngestionSettings
import org.apache.spark.sql.DataFrame

trait HiveRepository {

  def sqlMetadata(settings : IngestionSettings): Option[DataFrame]

//  def sqlFromFile(path: String, args: String*): Option[DataFrame]

  def setTableAsExternal(fullTableName: String): Unit

  def setTableAsInternal(fullTableName: String): Unit

  def dropPartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit

  def purgePartitionYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): Unit

  def getPartitionLocationYearMonthDay(fullTableName: String, year: Int, month: Int, day: Int): String

}
