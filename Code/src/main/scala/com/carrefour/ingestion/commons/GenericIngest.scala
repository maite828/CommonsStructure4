package com.carrefour.ingestion.commons

import com.carrefour.ingestion.commons.controller.{FileLoader, IngestionSettings, IngestionSettingsLoader}
import com.carrefour.ingestion.commons.core.Context


/**
  *
  *
  */

object GenericIngest {

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
