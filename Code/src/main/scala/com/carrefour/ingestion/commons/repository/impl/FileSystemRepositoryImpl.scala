package com.carrefour.ingestion.commons.repository.impl

import com.carrefour.ingestion.commons.context.FileSystemContext
import com.carrefour.ingestion.commons.context.impl.FileSystemContextImpl
import com.carrefour.ingestion.commons.repository.FileSystemRepository

/**
  *
  */
object FileSystemRepositoryImpl extends FileSystemContext with FileSystemRepository{

  private val dfs: FileSystemContext =  FileSystemContextImpl

  override def getFileSystem() = dfs.getFileSystem()
}
