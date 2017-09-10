package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.repository.impl.{FileSystemRepositoryImpl, HiveRepositoryImpl, SparkSessionRepositoryImpl}
import com.carrefour.ingestion.commons.repository.{FileSystemRepository, HiveRepository, SparkSessionRepository}
import com.carrefour.ingestion.commons.service.TransformService


object TransformServiceImpl extends TransformService {

  private val dfs: FileSystemRepository = FileSystemRepositoryImpl
  private val spark: SparkSessionRepository = SparkSessionRepositoryImpl
  private val hive: HiveRepository = HiveRepositoryImpl

  override def getSparkSession() = spark.getSparkSession()

  val dia: Int = 1
  val mes: Int = 2
  val ano: Int = 2017

  hive.dropPartitionYearMonthDay("ampliaciones", dia, mes, ano)

}
