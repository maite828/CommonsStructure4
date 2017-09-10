package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.repository.impl.{FileSystemRepositoryImpl, HiveRepositoryImpl, SparkSessionRepositoryImpl}

/**
  *  In principle private with the idea of ​​being able to delegate only in the methods that are decided
  */
object Repositories {
  private val hive = HiveRepositoryImpl
  private val fileSystem = FileSystemRepositoryImpl
  private val spark = SparkSessionRepositoryImpl

}

