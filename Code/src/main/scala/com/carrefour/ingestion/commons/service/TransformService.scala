package com.carrefour.ingestion.commons.service

import org.apache.spark.sql.SparkSession

trait TransformService {

  def getSparkSession(): SparkSession

}
