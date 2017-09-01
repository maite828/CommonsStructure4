package com.carrefour.ingestion.commons.repositories

import org.apache.hadoop.fs.FileSystem

trait FileSystemRepository {
  //Los métodos
  val dfs:FileSystem
}
