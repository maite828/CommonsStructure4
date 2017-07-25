package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.util.SparkJobSettings
import scopt.OptionParser

/**
 * Settings to load relational data.
 */
case class RelationalLoaderJobSettings(
  inputPath: String = "",
  outputDb: String = "", 
  outputTable: String = "",
  transformationsTable: String = "",
  numPartitions: Int = 0,
  format: RelationalFormats.RelationalFormat = RelationalFormats.GzFormat,
  header: Int = 0,
  fieldDelimiter: String = "",
  date: Int = 0,
  year: Int = 0,
  month: Int = 0,
  day: Int = 0,
  businessunit: String = "") extends SparkJobSettings

object RelationalFormats {
  sealed trait RelationalFormat
  case object TextFormat extends RelationalFormat
  case object GzFormat extends RelationalFormat
  case object ZipFormat extends RelationalFormat
}

/**
 * Parser for the relational data loader program. Method  {@link #parse} produces a {@link RelationalJobSettings} to configure the Spark job.
 */
object ArgsParser extends OptionParser[RelationalLoaderJobSettings]("RelationalLoaderDriver") {

  head("Relational data loader", "1.0")
  
  opt[String]('d', "date") required () valueName "<load date>" action { (value, config) =>
    val dateValue = value.toInt
    val yearValue=value.substring(0,4).toInt
    val monthValue=value.substring(4,6).toInt
    val dayValue=value.substring(6,8).toInt
    config.copy(date = dateValue, year = yearValue, month = monthValue, day = dayValue)
  } text "Loading date"

  opt[String]('b', "businessunit") required () valueName "<business unit>" action { (value, config) =>
    config.copy(businessunit = value)
  } text "Business Unit to load"

  opt[String]("file-format") valueName "<text|gz>" action { (value, config) =>
    value.toLowerCase match {
      case "text" => config.copy(format = RelationalFormats.TextFormat)
      case "gz" => config.copy(format = RelationalFormats.GzFormat)
      //FIXME support zip format
      //      case "zip" => config.copy(format = RelationalFormats.ZipFormat)
      case _ => throw new IllegalArgumentException(s"Unsupported file format: $value. Allowed formats are: text, gz")
    }
  } text "File format: text|gz"

  help("help") text "This help"
}
