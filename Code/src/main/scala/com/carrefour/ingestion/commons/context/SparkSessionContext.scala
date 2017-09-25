package com.carrefour.ingestion.commons.context

import org.apache.spark.sql.SparkSession

/**
  * Interface to get SparkSession
  */
trait SparkSessionContext {

  def getSparkSession(appName: String): SparkSession

  def getSparkSession(): SparkSession
}
