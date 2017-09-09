package com.carrefour.ingestion.commons

import com.carrefour.ingestion.commons.controller.{FileLoader, IngestionSettings, IngestionSettingsLoader}
import com.carrefour.ingestion.commons.service.impl.ExtractServiceImpl

/**
  * Main
  *
  */

object GenericIngestion {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    IngestionSettingsLoader.parse(args, IngestionSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        settings.businessunit = "Ssff"
        ExtractServiceImpl.getSparkSession("Ssff ssff data loader")
        FileLoader.run(settings)

      }
    }
  }
}
