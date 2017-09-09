package com.carrefour.ingestion.commons.repository

import com.carrefour.ingestion.commons.context.SparkSessionContext
import com.carrefour.ingestion.commons.context.impl.SparkSessionContextImpl

/**
  *
  */
object SparkSessionRepository extends SparkSessionContext{

  private val fs: SparkSessionContext =  SparkSessionContextImpl

  override def getSparkSession(appName: String) = fs.getSparkSession(appName: String)

  override def getSparkSession() = fs.getSparkSession()
}
