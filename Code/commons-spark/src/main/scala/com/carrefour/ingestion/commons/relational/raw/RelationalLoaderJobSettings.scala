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
  tableSuffix: String = "",
  transformationsTable: String = "",
  numPartitions: Int = 0,
  format: RelationalFormats.RelationalFormat = RelationalFormats.GzFormat,
  header: Boolean = false,
  date: Int = 0,
  year: Int = 0,
  month: Int = 0,
  day: Int = 0) extends SparkJobSettings

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
  
  opt[String]("date") required () valueName "<load date>" action { (value, config) =>
    val dateValue = value.toInt
    val yearValue=value.substring(0,4).toInt
    val monthValue=value.substring(4,6).toInt
    val dayValue=value.substring(6,8).toInt
    config.copy(date = dateValue, year = yearValue, month = monthValue, day = dayValue)
  } text "Loading date"

  opt[String]('i', "input") required () valueName "<input path>" action { (value, config) =>
    config.copy(inputPath = value)
  } text "HDFS input path (directory with multiple files -multiple tables- or one file)"

  opt[String]('d', "output-db") required () valueName "<output database>" action { (value, config) =>
    config.copy(outputDb = value)
  } text "Output database"

  opt[String]('b', "output-table") valueName "<output table>" action { (value, config) =>
    config.copy(outputTable = value)
  } text "Output table. It will be ignored if input is a directory."

  opt[String]('s', "suffix") valueName "<file suffix>" action { (value, config) =>
    config.copy(tableSuffix = value)
  } text "File name suffix (before .extension), to be ignored when obtaining the table name from the file name. When output-table is specified this parameter is ignored."

  opt[String]('t', "transformations") valueName "<transformations table>" action { (value, config) =>
    config.copy(transformationsTable = value)
  } text "Field transformations table name"

  opt[String]('p', "partitions") valueName "<num partitions>" action { (value, config) =>
    val intValue = value.toInt
    if (intValue >= 0) config.copy(numPartitions = intValue) else config
  } text "Minimum number of RDD partitions to use for the input data. If 0, the original number of partitions will be used. Default value is 0."

  opt[String]("file-format") valueName "<text|gz>" action { (value, config) =>
    value.toLowerCase match {
      case "text" => config.copy(format = RelationalFormats.TextFormat)
      case "gz" => config.copy(format = RelationalFormats.GzFormat)
      //FIXME support zip format
      //      case "zip" => config.copy(format = RelationalFormats.ZipFormat)
      case _ => throw new IllegalArgumentException(s"Unsupported file format: $value. Allowed formats are: text, gz")
    }
  } text "File format: text|gz"

  opt[Unit]("with-header") action { (_, config) =>
    config.copy(header = true)
  } text "Files contain header"

  help("help") text "This help"
}
