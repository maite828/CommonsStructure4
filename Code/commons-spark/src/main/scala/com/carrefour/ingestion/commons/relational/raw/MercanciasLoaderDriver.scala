package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.Loader.JobSettingsLoader
import com.carrefour.ingestion.commons.Loader.ArgsParser
import com.carrefour.ingestion.commons.util.SparkUtils

object MercanciasLoaderDriver {

  //TODO Log

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args, JobSettingsLoader()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        //FIXME Replace var in LoaderJobSettiogs case class
        settings.businessunit = "Mercancias"
        SparkUtils.withHiveContext("Mercancias relational data loader") { implicit sqlContext => RelationalLoaderJob.run(settings) } }
    }
  }
}