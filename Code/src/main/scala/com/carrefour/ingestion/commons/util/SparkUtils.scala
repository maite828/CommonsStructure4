package com.carrefour.ingestion.commons.commons.util

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def withHiveContext(appName: String)(job: SparkSession => Unit): Unit = {

    val sparkSession = SparkSession
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

    job(sparkSession)

    sparkSession.stop()
  }
}