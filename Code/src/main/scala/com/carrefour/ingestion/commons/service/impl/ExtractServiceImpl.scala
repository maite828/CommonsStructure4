package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.repository.{FileSystemRepository, HiveRepository, SparkSessionRepository}
import com.carrefour.ingestion.commons.service.ExtractService

/**
  *
  */
object ExtractServiceImpl extends ExtractService {

  private val fs = FileSystemRepository
  private val spark = SparkSessionRepository
  private val hive = HiveRepository

  def getSparkSession() = spark.getSparkSession()

  def getSparkSession(appName: String) = spark.getSparkSession(appName)
}
