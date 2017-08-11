package com.carrefour.ingestion.commons.loader

import com.carrefour.ingestion.commons.util.SparkJobSettings
import scopt.OptionParser

/**
 * Settings to load relational data.
 */
case class IngestionSettings(
  var businessunit: String = "",
  entity: String = "",
  transformationsTable: String = "",
  numPartitions: Int = 0,
  date: Int = 0,
  year: Int = 0,
  month: Int = 0,
  day: Int = 0
) extends SparkJobSettings

/**
 * Parser for the relational data loader program. Method  {@link #parse} produces a {@link JobSettings} to configure the Spark job.
 */
object IngestionSettingsLoader extends OptionParser[IngestionSettings]("IngestionJobSettings") {

  head("Relational data loader", "1.0")

  opt[String]('e',"entity") valueName "<entity to load>" action{ (value, config) =>
    val entityValue = value
    config.copy(entity = entityValue)
  } text "Entity to load. If not specified, the process will load all of the entities defined in the Hive parameters"
  
  opt[String]('d', "date") required () valueName "<load date>" action { (value, config) =>
    val dateValue = value.toInt
    val yearValue=value.substring(0,4).toInt
    val monthValue=value.substring(4,6).toInt
    val dayValue=value.substring(6,8).toInt
    config.copy(date = dateValue, year = yearValue, month = monthValue, day = dayValue)
  } text "Loading date"

  opt[String]('t', "transformations") required () valueName "<transformations_table>" action { (value, config) =>
    val transformations = value
    config.copy(transformationsTable = transformations)
  } text "Loading date"

  opt[String]('p', "partitions") valueName "<num partitions>" action { (value, config) =>
    val intValue = value.toInt
    if (intValue >= 0) config.copy(numPartitions = intValue) else config
  } text "Minimum number of RDD partitions to use for the input data. If 0, the original number of partitions will be used. Default value is 0."


//  opt[String]("file-format") valueName "<text|gz>" action { (value, config) =>
//    value.toLowerCase match {
//      case "text" => config.copy(format = FileFormats.TextFormat)
//      case "gz" => config.copy(format = FileFormats.GzFormat)
//      //FIXME support zip format
//      //      case "zip" => config.copy(format = RelationalFormats.ZipFormat)
//      case _ => throw new IllegalArgumentException(s"Unsupported file format: $value. Allowed formats are: text, gz")
//    }
//  } text "File format: text|gz"

  help("help") text "This help"
}
