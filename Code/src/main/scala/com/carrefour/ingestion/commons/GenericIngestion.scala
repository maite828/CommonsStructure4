package com.carrefour.ingestion.commons

import com.carrefour.ingestion.commons.controller.{IngestionSettings, IngestionSettingsLoader}

object GenericIngestion {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    IngestionSettingsLoader.parse(args, IngestionSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        settings.businessunit = "Ssff"
        IngestionSettingsLoader.startApp("Ingesting Financial Services")
        IngestionSettingsLoader.fileLoader(settings)
      }
    }
  }
}
