package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.service.LoadService
import com.carrefour.ingestion.commons.service.impl.LoadServiceImpl

object Services {
  val loadService:LoadService = LoadServiceImpl
}
