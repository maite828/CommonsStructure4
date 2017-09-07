package com.carrefour.ingest.commons.repository

import org.apache.hadoop.fs.FileSystem

trait FileSystemRepository {

  val dfs: FileSystem
}
