package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.repository.{FileSystemRepository, HiveRepository, SparkSessionRepository}

/**
  *  En principio privada con la idea de poder delegar solo en los métodos que se decida
  */
object Repositories {
  private val hiveService = HiveRepository
  private val fileSystem = FileSystemRepository
  private val spark = SparkSessionRepository
}

