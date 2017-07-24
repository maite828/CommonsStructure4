package com.carrefour.ingestion.commons.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SparkUtils {

  def withHiveContext(appName: String)(job: SQLContext => Unit): Unit = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val sqlContext: HiveContext = new HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    sqlContext.setConf("hive.exec.max.dynamic.partitions", "100000")
    sqlContext.setConf("hive.exec.max.dynamic.partitions.pernode", "20000")
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    sqlContext.setConf("parquet.compression","snappy")

    job(sqlContext)

    sqlContext.sparkContext.stop()
  }
}