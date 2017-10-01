package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.context.impl.{FileSystemContextImpl, SparkSessionContextImpl}
import com.carrefour.ingestion.commons.context.{FileSystemContext, SparkSessionContext}

/**
  * In principle private with the idea of ​​being able to delegate only in the methods that are decided
  */
object Contexts {

  val dfs: FileSystemContext = FileSystemContextImpl
  val spark: SparkSessionContext = SparkSessionContextImpl
}

