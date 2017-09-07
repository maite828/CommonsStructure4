package com.carrefour.ingest.commons.core

import org.apache.hadoop.fs.FileSystem

/**
  *
  */
object FileSystemContext {

  private val dfs = FileSystem.get(Context.getSparkSession().sparkContext.hadoopConfiguration)
  def getFileSystem(): FileSystem = {
    dfs
  }
}
