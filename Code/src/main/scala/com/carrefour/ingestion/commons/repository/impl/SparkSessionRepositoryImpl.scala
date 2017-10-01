package com.carrefour.ingestion.commons.repository.impl

import com.carrefour.ingestion.commons.context.SparkSessionContext
import com.carrefour.ingestion.commons.core.Contexts
import com.carrefour.ingestion.commons.repository.SparkSessionRepository

/**
  *
  */
object SparkSessionRepositoryImpl extends SparkSessionContext with SparkSessionRepository{

  override def getSparkSession(appName: String) = Contexts.spark.getSparkSession(appName: String)

  override def getSparkSession() = Contexts.spark.getSparkSession()
}
