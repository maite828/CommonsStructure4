package com.carrefour.ingestion.commons.service

import com.carrefour.ingestion.commons.bean.IngestionMetadata
import com.carrefour.ingestion.commons.controller.IngestionSettings
import org.apache.spark.sql.SparkSession

trait ExtractService {

  def nameApp(app:String): SparkSession

  def loadMetadata(settings: IngestionSettings): Array[IngestionMetadata]

}
