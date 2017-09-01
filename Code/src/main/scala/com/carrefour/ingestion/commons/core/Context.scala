package com.carrefour.ingestion.commons.core

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Context - SparkSession
  */

object Context {

  //SparkSession
  @transient private var spark: SparkSession = _
  def getSparkSession(appName: String): SparkSession = {

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
  def getSparkSession(): SparkSession = {
    spark
  }

  //FileSystem
  private val dfs: FileSystem = FileSystem.get( spark.sparkContext.hadoopConfiguration)
  def getFileSystem(): FileSystem = {
    dfs
  }

  //Todo implemetar fileSystemLocal
//  private val fileSystemLocalocal: FileSystem

}

