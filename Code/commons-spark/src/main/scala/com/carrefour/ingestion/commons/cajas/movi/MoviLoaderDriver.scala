package com.carrefour.ingestion.commons.cajas.movi

import com.carrefour.ingestion.commons.util.SparkUtils

object MoviLoaderDriver {

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args, MoviLoaderSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings =>
        SparkUtils.withHiveContext("Movi loader") { implicit sqlContext => MoviLoaderJob.run(settings) }
    }
  }
}