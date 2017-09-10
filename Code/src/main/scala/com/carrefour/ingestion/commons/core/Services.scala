package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.service.{ExtractService, LoadService, TransformService}
import com.carrefour.ingestion.commons.service.impl.{ExtractServiceImpl, LoadServiceImpl, TransformServiceImpl}


/**
  * In principle private with the idea of ​​being able to delegate only in the methods that are decided
  */
object Services {
  private val extractService: ExtractService = ExtractServiceImpl
  private val loadService: LoadService = LoadServiceImpl
  private val transformService: TransformService = TransformServiceImpl


}
