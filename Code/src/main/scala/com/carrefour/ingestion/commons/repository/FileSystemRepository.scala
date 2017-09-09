package com.carrefour.ingestion.commons.repository

import com.carrefour.ingestion.commons.context.FileSystemContext
import com.carrefour.ingestion.commons.context.impl.FileSystemContextImpl

/**
  *
  */
object FileSystemRepository extends FileSystemContext {

  private val fs: FileSystemContext =  FileSystemContextImpl

  override def getFileSystem() = fs.getFileSystem()
}
