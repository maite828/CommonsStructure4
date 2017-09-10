package com.carrefour.ingestion.commons.repository

import org.apache.spark.sql.SparkSession

trait SparkSessionRepository {

  def getSparkSession(appName: String): SparkSession

  def getSparkSession(): SparkSession
}
