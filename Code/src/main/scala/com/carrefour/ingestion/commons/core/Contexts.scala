package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.context.SparkSessionContext
import com.carrefour.ingestion.commons.context.impl.SparkSessionContextImpl
import com.carrefour.ingestion.commons.repository.FileSystemRepository
import com.carrefour.ingestion.commons.repository.impl.FileSystemRepositoryImpl

/**
  * In principle private with the idea of ​​being able to delegate only in the methods that are decided
  */
object Contexts {

  val dfs: FileSystemRepository = FileSystemRepositoryImpl
  val spark: SparkSessionContext = SparkSessionContextImpl
}

