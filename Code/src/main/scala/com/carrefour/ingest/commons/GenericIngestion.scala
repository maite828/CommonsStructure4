package com.carrefour.ingest.commons

import com.carrefour.ingest.commons.controller.{FileLoader, IngestionSettings, IngestionSettingsLoader}
import com.carrefour.ingest.commons.core.Context

/**
 * Main
 *
 */

object GenericIngestion {

  //TODO Log

  def main(args: Array[String]): Unit = {


    IngestionSettingsLoader.parse(args, IngestionSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        settings.businessunit = "Ssff"
        Context.getSparkSession("Ssff ssff data loader")
        FileLoader.run(settings)
      }
    }
  }
}
