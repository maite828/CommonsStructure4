package com.carrefour.ingestion.commons.context.impl

import com.carrefour.ingestion.commons.context.SparkSessionContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Context - SparkSession
  */
 object SparkSessionContextImpl  extends SparkSessionContext {

  @transient private var spark: SparkSession = _

  override def getSparkSession(appName: String): SparkSession = {

    spark = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .config("spark.dynamicAllocation.enabled","false")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("hive.exec.max.dynamic.partitions", "100000")
      .config("hive.exec.max.dynamic.partitions.pernode", "20000")
      .config("spark.sql.parquet.compression.codec","snappy")
      .config("parquet.compression","snappy")
      .getOrCreate()

    spark
  }

  //Current SparkSession
  override def getSparkSession(): SparkSession = {
    spark
  }

}

