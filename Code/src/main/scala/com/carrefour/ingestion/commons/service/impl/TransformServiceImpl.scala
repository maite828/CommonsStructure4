package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.core.{Repositories, Services}
import com.carrefour.ingestion.commons.service.TransformService


object TransformServiceImpl extends TransformService {

  Repositories.dfs
  Repositories.hive
  Repositories.spark

  Services.extractService
  Services.loadService

  val dia: Int = 1
  val mes: Int = 2
  val ano: Int = 2017

  Repositories.hive.dropPartitionYearMonthDay("ampliaciones", dia, mes, ano)

}
