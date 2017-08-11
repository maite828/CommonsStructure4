package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.loader.{ArgsParser, FileLoader, JobSettingsLoader}
import com.carrefour.ingestion.commons.util.SparkUtils

object MarketingLoaderDriver {

  //TODO Log

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args, JobSettingsLoader()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        //FIXME Replace var in LoaderJobSettiogs case class
        settings.businessunit = "Marketing"
        SparkUtils.withHiveContext("Marketing relational data loader") { implicit sqlContext => FileLoader.run(settings) } }
    }
  }
}