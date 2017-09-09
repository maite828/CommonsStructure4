package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.repository.{FileSystemRepository, HiveRepository, SparkSessionRepository}
import com.carrefour.ingestion.commons.service.TransformService


object TransformServiceImpl extends TransformService {
  private val fs = FileSystemRepository
  private val spark = SparkSessionRepository
  private val hive = HiveRepository

  val dia: Int = 1
  val mes: Int = 2
  val ano: Int = 2017

  hive.dropPartitionYearMonthDay("ampliaciones",dia,mes,ano)

}
