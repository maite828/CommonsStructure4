package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.repositories.HiveRepository
import com.carrefour.ingestion.commons.repositories.impl.HiveRepositoryImpl


object Repository {
  val hive: HiveRepository = HiveRepositoryImpl


}
