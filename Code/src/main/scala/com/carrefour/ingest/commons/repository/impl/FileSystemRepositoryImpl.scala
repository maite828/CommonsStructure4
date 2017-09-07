package com.carrefour.ingest.commons.repository.impl

import com.carrefour.ingest.commons.core.FileSystemContext
import com.carrefour.ingest.commons.repository.FileSystemRepository

object FileSystemRepositoryImpl extends FileSystemRepository{
  override val dfs = FileSystemContext.getFileSystem()
}
