package com.carrefour.ingestion.commons.context

import org.apache.hadoop.fs.FileSystem

/**
  * Interface to get hadoop filesystem context
  */
trait FileSystemContext {

  def getFileSystem(): FileSystem
}
