package com.carrefour.ingestion.commons.cajas.ticket

import java.util.regex.Pattern

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.carrefour.ingestion.commons.util.ExtractionUtils

/**
 * Allowed formats for t files (tickets and movi).
 */
object TFormats {
  sealed trait TFormat
  case object TextFormat extends TFormat
  case object TarGzFormat extends TFormat
}

/**
 * Utilities for t files
 */
object TUtils {
  private val NumPartitions = 200

  val TicketFileNamePattern = Pattern.compile("\\d{8}_\\d{4}_t\\d{6}t\\.\\d{3}")

  /**
   * Creates a RDD with the ticket files: (filename, content), according to the given configuration (path, format, partitions).
   * It will only include in the RDD those files that match TicketFileNamePattern.
   */
  def ticketFiles(settings: TicketsLoaderSettings)(implicit sc: SparkContext): RDD[(String, String)] = {
    tFiles(settings.inputPath, settings.format, TicketFileNamePattern, settings.numPartitions)
  }


  /**
   * Loads t files from the given path in the specified format. Filenames not matching the pattern will be filtered out.
   */
  private[this] def tFiles(inputPath: String, format: TFormats.TFormat, pattern: Pattern, numPartitions: Int = NumPartitions)(implicit sc: SparkContext): RDD[(String, String)] = {
    val tkFiles = format match {
      case TFormats.TarGzFormat => sc.binaryFiles(inputPath, numPartitions).
        flatMap { case (_, content) => ExtractionUtils.extractArchiveFiles(content, ExtractionUtils.TarArchive, Some(ExtractionUtils.GzipCompression), Some(pattern)).get }. //FIXME exception
        mapValues(ExtractionUtils.decode(_))
      case TFormats.TextFormat => sc.wholeTextFiles(inputPath, numPartitions)
    }
    tkFiles.map { case (filepath, content) => (filename(filepath), content) }.
      filter { case (filename, content) => pattern.matcher(filename).matches }
  }

  private[this] def filename(path: String): String = new Path(path).getName
}