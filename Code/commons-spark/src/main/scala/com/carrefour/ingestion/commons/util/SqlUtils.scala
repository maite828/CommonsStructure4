package com.carrefour.ingestion.commons.util

import com.carrefour.ingestion.commons.exceptions.FatalException
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

object SqlUtils {

  val Logger = LoggerFactory.getLogger(getClass)

  def sql(path: String, args: String*)(implicit sqlContext: SQLContext): Option[DataFrame] = {

    val is = getClass().getResourceAsStream(path)
    if (is == null) {
      throw new FatalException("No existe el fichero: " + path)
    }
    var query = scala.io.Source.fromInputStream(is).mkString

    var i = 1
    for (arg <- args) {
      query = query.replace("$" + i, arg)
      i = i + 1
    }

    Logger.info(s"Hive Query File: $path")
    Logger.info(s"Hive Query: $query")
    Some(sqlContext.sql(query));
  }
}