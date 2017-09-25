package com.carrefour.ingestion.commons.repository.impl

import com.carrefour.ingestion.commons.context.FileSystemContext
import com.carrefour.ingestion.commons.core.Contexts
import com.carrefour.ingestion.commons.repository.FileSystemRepository

/**
  *
  */
object FileSystemRepositoryImpl extends FileSystemContext with FileSystemRepository{

  override def getFileSystem() = Contexts.dfs.getFileSystem()


}
