package com.carrefour.ingestion.commons.service

import com.carrefour.ingestion.commons.bean.{DelimitedFileType, IngestionMetadata}
import com.carrefour.ingestion.commons.controller.IngestionSettings
import com.carrefour.ingestion.commons.util.transform.{FieldInfo, TransformationInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait LoadService {

  def getSparkSession(): SparkSession

  def run(jobSettings: IngestionSettings): Unit

  def loadDelimitedFile(fileType: DelimitedFileType, settings: IngestionMetadata): Unit

  def extractFieldsInfo(input: RDD[String], tableName: String, transformations: Map[String, Map[String, TransformationInfo]]): Seq[FieldInfo]

}
