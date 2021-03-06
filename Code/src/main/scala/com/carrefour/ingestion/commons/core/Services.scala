package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.service.{ExtractService, LoadService, MetadataService, TransformService}
import com.carrefour.ingestion.commons.service.impl.{ExtractServiceImpl, LoadServiceImpl, MetadataServiceImpl, TransformServiceImpl}


/**
  * In principle private with the idea of ​​being able to delegate only in the methods that are decided
  */
object Services {

  val extractService: ExtractService = ExtractServiceImpl
  val loadService: LoadService = LoadServiceImpl
  val transformService: TransformService = TransformServiceImpl
  val metadataService: MetadataService = MetadataServiceImpl
}
