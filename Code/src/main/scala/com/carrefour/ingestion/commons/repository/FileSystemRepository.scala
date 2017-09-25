package com.carrefour.ingestion.commons.repository

import org.apache.hadoop.fs.FileSystem

trait FileSystemRepository {

  def getFileSystem(): FileSystem
}
