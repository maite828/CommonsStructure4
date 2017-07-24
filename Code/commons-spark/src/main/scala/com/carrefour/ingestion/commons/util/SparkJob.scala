package com.carrefour.ingestion.commons.util

import org.apache.spark.sql.SQLContext

trait SparkJob[T <: SparkJobSettings] {
  def run(settings: T)(implicit sqlContext: SQLContext): Unit
}

trait SparkJobSettings {

}