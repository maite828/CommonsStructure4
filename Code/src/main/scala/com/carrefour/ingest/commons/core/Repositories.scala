package com.carrefour.ingest.commons.core

import com.carrefour.ingest.commons.repository.impl.HiveRepositoryImpl

object Repositories {
  val hiveService: HiveRepositoryImpl.type = HiveRepositoryImpl
}
