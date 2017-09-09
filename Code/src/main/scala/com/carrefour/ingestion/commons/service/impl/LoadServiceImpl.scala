package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.repository.{FileSystemRepository, HiveRepository, SparkSessionRepository}
import com.carrefour.ingestion.commons.service.LoadService

/**
  *
  */
object LoadServiceImpl extends LoadService {

  private val fs = FileSystemRepository
  private val spark = SparkSessionRepository
  private val hive = HiveRepository

  def readFile(): Unit = {
    // Repository.hive
  }
}
