package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.util.SparkUtils

object MercanciasLoaderDriver {

  //TODO Log

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args, RelationalLoaderJobSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        settings.businessunit = "Mercancias"
        SparkUtils.withHiveContext("Mercancias relational data loader") { implicit sqlContext => RelationalLoaderJob.run(settings) } }
    }
  }
}