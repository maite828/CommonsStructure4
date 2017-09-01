package com.carrefour.ingestion.commons.repositories.impl

import com.carrefour.ingestion.commons.core.Context
import com.carrefour.ingestion.commons.repositories.FileSystemRepository

object FileSystemRepositoryImpl extends FileSystemRepository{
  override val dfs = Context.getFileSystem()
  val hive = Context.getFileSystem()
}
