package com.carrefour.ingestion.commons.relational.raw


import com.carrefour.ingestion.commons.relational.raw.{ArgsParser, RelationalLoaderJob, RelationalLoaderJobSettings}
import com.carrefour.ingestion.commons.util.SparkUtils

object MarketingLoaderDriver {

  //TODO Log

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args, RelationalLoaderJobSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        settings.businessunit = "Marketing"
        SparkUtils.withHiveContext("Marketing relational data loader") { implicit sqlContext => RelationalLoaderJob.run(settings) } }
    }
  }
}