package com.carrefour.ingestion.commons.cajas.movi

import com.carrefour.ingestion.commons.cajas.ticket.TFormats
import com.carrefour.ingestion.commons.util.SparkJobSettings
import scopt.OptionParser

case class MoviLoaderSettings(inputPath: String = "", outputDb: String = "", fieldsConfTable: String = "", transformationsTable: String = "", format: TFormats.TFormat = TFormats.TarGzFormat) extends SparkJobSettings

/**
 * Parser for the movi data loader program. Method  {@link #parse} produces a {@link MoviLoaderSettings} to configure the Spark job.
 */
object ArgsParser extends OptionParser[MoviLoaderSettings]("MoviLoaderDriver") {

  head("Tickets data loader", "1.0")

  opt[String]('i', "input") required () valueName "<input path>" action { (value, config) =>
    config.copy(inputPath = value)
  } text "HDFS input path (directory with multiple files or single file)"

  opt[String]('d', "output") required () valueName "<output database>" action { (value, config) =>
    config.copy(outputDb = value)
  } text "Output database"

  opt[String]('f', "fieldsconf") required () valueName "<fields configuration table>" action { (value, config) =>
    config.copy(fieldsConfTable = value)
  } text "Fields configuration table name: configures field names and positions for each record type"

  opt[String]('t', "transformations") valueName "<transformations table>" action { (value, config) =>
    config.copy(transformationsTable = value)
  } text "Field transformations table name: configures how to transform input values"

  opt[String]("file-format") valueName "<text|tar.gz>" action { (value, config) =>
    value.toLowerCase match {
      case "text" => config.copy(format = TFormats.TextFormat)
      case "tar.gz" => config.copy(format = TFormats.TarGzFormat)
      case _ => throw new IllegalArgumentException(s"Unsupported movi format: $value. Allowed formats are: text, tar.gz")
    }
  } text "File format: text|tar.gz"

  help("help") text "This help"
}