package com.carrefour.ingestion.commons.util

import org.apache.spark.sql.SparkSession

trait SparkJob[T <: SparkJobSettings] {
  def run(settings: T)(implicit sparkSession: SparkSession): Unit
}

trait SparkJobSettings {

}