package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.repository.impl.{FileSystemRepositoryImpl, HiveRepositoryImpl, SparkSessionRepositoryImpl}
import com.carrefour.ingestion.commons.repository.{FileSystemRepository, HiveRepository, SparkSessionRepository}

/**
  * In principle private with the idea of ​​being able to delegate only in the methods that are decided
  */
object Repositories {

  val hive: HiveRepository = HiveRepositoryImpl
  val dfs: FileSystemRepository = FileSystemRepositoryImpl
  val spark: SparkSessionRepository = SparkSessionRepositoryImpl

}

