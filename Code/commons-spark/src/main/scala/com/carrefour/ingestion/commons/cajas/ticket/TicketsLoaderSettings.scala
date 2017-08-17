package com.carrefour.ingestion.commons.cajas.ticket

import com.carrefour.ingestion.commons.util.SparkJobSettings

import scopt.OptionParser

case class TicketsLoaderSettings(groupSize: Int = -1, window: Int = 1, inputPath: String = "", outputDb: String = "", rowBuilderTable: String = "", transformationsTable: String = "", numPartitions: Int = 2000, format: TFormats.TFormat = TFormats.TarGzFormat) extends SparkJobSettings

/**
 * Parser for the tickets data loader program.
 * Method  {@link #parse} produces a {@link TicketsLoaderSettings} to configure the Spark job.
 */
object ArgsParser extends OptionParser[TicketsLoaderSettings]("TicketsLoaderDriver") {

  head("Tickets data loader", "1.0")

  opt[String]('g', "groupSize") valueName "<input path>" action { (value, config) =>
    config.copy(groupSize = value.toInt)
  } text "Size of the window to load (bytes)"

  opt[String]('w', "window") valueName "<input path>" action { (value, config) =>
    config.copy(window = value.toInt)
  } text "Size of the window to load (days)"

  opt[String]('i', "input") required () valueName "<input path>" action { (value, config) =>
    config.copy(inputPath = value)
  } text "HDFS input path (directory with multiple files or single file)"

  opt[String]('d', "output") required () valueName "<output database>" action { (value, config) =>
    config.copy(outputDb = value)
  } text "Output database"

  opt[String]('b', "builders") required () valueName "<row builders table>" action { (value, config) =>
    config.copy(rowBuilderTable = value)
  } text "Row builders table name: configures how to parse each record type"

  opt[String]('t', "transformations") valueName "<transformations table>" action { (value, config) =>
    config.copy(transformationsTable = value)
  } text "Field transformations table name: configures how to transform input values"

  opt[String]('p', "partitions") valueName "<num partitions>" action { (value, config) =>
    val intValue = value.toInt
    if (intValue > 0) config.copy(numPartitions = intValue) else config
  } text "Suggestion of the number of RDD partitions to use for the input data"

  opt[String]("file-format") valueName "<text|tar.gz>" action { (value, config) =>
    value.toLowerCase match {
      case "text" => config.copy(format = TFormats.TextFormat)
      case "tar.gz" => config.copy(format = TFormats.TarGzFormat)
      case _ => throw new IllegalArgumentException(s"Unsupported ticket format: $value. Allowed formats are: text, tar.gz")
    }
  } text "File format: text|tar.gz"

  help("help") text "This help"
}