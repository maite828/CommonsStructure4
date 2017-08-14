package com.carrefour.ingestion.commons.util

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SparkUtils {

  def withHiveContext(appName: String)(job: SparkSession => Unit): Unit = {
//    val sparkConf = new SparkConf().setAppName(appName)
//    val sc = new SparkContext(sparkConf)
//    val sqlContext: HiveContext = new HiveContext(sc)
//    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
//    sqlContext.setConf("hive.exec.max.dynamic.partitions", "100000")
//    sqlContext.setConf("hive.exec.max.dynamic.partitions.pernode", "20000")
//    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
//    sqlContext.setConf("parquet.compression", "snappy")
//
//    job(sqlContext)
//
//    sqlContext.sparkContext.stop()

    val sparkSession = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
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