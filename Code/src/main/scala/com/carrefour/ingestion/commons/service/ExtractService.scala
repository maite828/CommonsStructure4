package com.carrefour.ingestion.commons.service

import org.apache.spark.sql.SparkSession

trait ExtractService {

  def nameApp(app:String): SparkSession

}
