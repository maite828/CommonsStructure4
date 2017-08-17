package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.loader.{IngestionSettingsLoader, FileLoader, IngestionSettings}
import com.carrefour.ingestion.commons.util.SparkUtils

/**
  * Specific loader driver for the ingestion of the "Marketing" entities
  */
object MarketingLoaderDriver {

  //TODO Log

  def main(args: Array[String]): Unit = {
    IngestionSettingsLoader.parse(args, IngestionSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        //FIXME Replace var in LoaderJobSettings case class
        settings.businessunit = "Marketing"
        SparkUtils.withHiveContext("Marketing relational data loader") { implicit sparkSession => FileLoader.run(settings) } }
    }
  }
}