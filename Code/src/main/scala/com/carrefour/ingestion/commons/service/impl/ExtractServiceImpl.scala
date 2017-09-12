package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.core.Repositories
import com.carrefour.ingestion.commons.exception.logging.LazyLogging
import com.carrefour.ingestion.commons.service.ExtractService

/**
  * Antiguo Metadata loader
  */

object ExtractServiceImpl extends ExtractService with LazyLogging {

  val defaultNumPartitions = 8

  override def nameApp(app: String) = Repositories.spark.getSparkSession(app)


}



