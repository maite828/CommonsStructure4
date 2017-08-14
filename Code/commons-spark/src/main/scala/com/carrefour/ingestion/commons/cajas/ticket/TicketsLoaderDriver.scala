package com.carrefour.ingestion.commons.cajas.ticket

import com.carrefour.ingestion.commons.util.SparkUtils
import org.apache.spark.sql.SparkSession

object TicketsLoaderDriver {

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args, TicketsLoaderSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings =>
        SparkUtils.withHiveContext("Tickets loader") { implicit sparkSession => TicketsLoaderJob.run(settings) }
    }
  }
}