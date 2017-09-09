package com.carrefour.ingestion.commons.context.impl

import com.carrefour.ingestion.commons.context.FileSystemContext
import org.apache.hadoop.fs.FileSystem

/**
  * Context - FileSystem
  */
object FileSystemContextImpl extends FileSystemContext{

  @transient private var dfs: FileSystem = _

  override def getFileSystem(): FileSystem = {
    dfs = FileSystem.get(SparkSessionContextImpl.getSparkSession().sparkContext.hadoopConfiguration)
    dfs
  }
}
