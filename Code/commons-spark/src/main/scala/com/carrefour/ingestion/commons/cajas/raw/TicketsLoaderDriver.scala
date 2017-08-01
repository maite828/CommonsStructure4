package com.carrefour.ingestion.commons.cajas.raw

import com.carrefour.ingestion.commons.util.SparkUtils

object TicketsLoaderDriver {

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args, TicketsLoaderSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings =>
        SparkUtils.withHiveContext("Tickets loader") { implicit sqlContext => TicketsLoaderJob.run(settings) }
    }
  }
}