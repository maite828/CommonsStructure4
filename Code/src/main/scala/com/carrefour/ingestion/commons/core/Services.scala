package com.carrefour.ingestion.commons.core

import com.carrefour.ingestion.commons.service.impl.{ExtractServiceImpl, LoadServiceImpl, TransformServiceImpl}


/**
  * En principio privada con la idea de poder delegar solo en los métodos que se decida
  */
object Services {
  private val extractService: ExtractServiceImpl.type = ExtractServiceImpl
  private val loadService: LoadServiceImpl.type = LoadServiceImpl
  private val transformService: TransformServiceImpl.type = TransformServiceImpl


}
