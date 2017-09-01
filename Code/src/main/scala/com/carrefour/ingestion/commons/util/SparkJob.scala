package com.carrefour.ingestion.commons.util

trait SparkJob[T <: SparkJobSettings] {
  def run(settings: T): Unit
}

trait SparkJobSettings {

}