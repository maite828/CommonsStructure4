package com.carrefour.ingestion.commons.repository.impl

import com.carrefour.ingestion.commons.context.SparkSessionContext
import com.carrefour.ingestion.commons.context.impl.SparkSessionContextImpl
import com.carrefour.ingestion.commons.repository.SparkSessionRepository

/**
  *
  */
object SparkSessionRepositoryImpl extends SparkSessionContext with SparkSessionRepository{

  private val spark: SparkSessionContext =  SparkSessionContextImpl

  override def getSparkSession(appName: String) = spark.getSparkSession(appName: String)

  override def getSparkSession() = spark.getSparkSession()
}
