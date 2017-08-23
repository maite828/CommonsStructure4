package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.loader.{FileLoader, IngestionSettings, IngestionSettingsLoader}
import com.carrefour.ingestion.commons.util.SparkUtils

object SsffLoaderDriver {

  //TODO Log

  def main(args: Array[String]): Unit = {
    IngestionSettingsLoader.parse(args, IngestionSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        //FIXME Replace var in LoaderJobSettings case class
        settings.businessunit = "Ssff"
        SparkUtils.withHiveContext("Ssff ssff data loader") { implicit sparkSession => FileLoader.run(settings) } }
    }
  }
}
