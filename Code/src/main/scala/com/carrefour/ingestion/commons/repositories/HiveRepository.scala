package com.carrefour.ingestion.commons.repositories

import org.apache.spark.sql.DataFrame

trait HiveRepository {
  def sql(path: String, args: String*): Option[DataFrame]
}
