package com.carrefour.ingestion.commons.cajas.ticket

import java.util.regex.Pattern

import com.carrefour.ingestion.commons.cajas.movi.MoviLoaderSettings
import com.carrefour.ingestion.commons.util.ExtractionUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

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

  val TicketFileNamePattern: Pattern = Pattern.compile("\\d{8}_\\d{4}_t\\d{6}t\\.\\d{3}")
  val MoviFileNamePattern: Pattern = Pattern.compile("\\d{8}_\\d{4}_movi\\.\\d{3}")

  /**
   * Creates a RDD with the ticket files: (filename, content), according to the given configuration (path, format, partitions).
   * It will only include in the RDD those files that match TicketFileNamePattern.
   */
  def ticketFiles(inputFiles: Array[String], settings: TicketsLoaderSettings)(implicit sc: SparkContext): RDD[(String, String)] = {
    inputFiles.map(inputFile => {
      tFiles(inputFile, settings.format, TicketFileNamePattern, settings.numPartitions)
    }).reduce((left, right) => left ++ right)
  }

  /**
    * Creates a RDD with the movi files: (filename, content), according to the given configuration (path, format).
    * It will only include in the RDD those files that match MoviFileNamePattern.
    */
  def moviFiles(settings: MoviLoaderSettings)(implicit sc: SparkContext): RDD[(String, String)] = {
    tFiles(settings.inputPath, settings.format, MoviFileNamePattern)
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
      filter { case (filename, _) => pattern.matcher(filename).matches }
  }

  private[this] def filename(path: String): String = new Path(path).getName

  /**
    * Method to get the names of the files to be ingested.
    * The files will be grouped in blocks of N days, where N is specified by the parameter settings.window
    *
    * @param settings Settings for the ingestion of the ticket entity. Needed for the input path and the size of the window in days.
    * @param sc SparkContext needed for accessing HDFS.
    * @return A List of Array's containing the file names, already grouped in blocks of days
    */
  def getFilesByDay(settings: TicketsLoaderSettings)(sc: SparkContext): List[Array[String]] = {
    val pattern =  """(.*\d{8})_(\d{4}).*""".r

    FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(settings.inputPath))
      .map(x => x.getPath.toString)
      .map(x => { val pattern(fecha,_) = x
        fecha})
      .distinct
      .map(_ + "*")
      .grouped(settings.window)
      .toList
  }

  /**
    * Method to get the names of the files to be ingested.
    * The files will be grouped in blocks of a maximum size N in bytes, where N is specified by the parameter settings.groupSize
    *
    * @param settings Settings for the ingestion of the ticket entity. Needed for the input path and the size of the window in days.
    * @param sc SparkContext needed for accessing HDFS.
    * @return A List of Array's containing the file names, already grouped in blocks of a maximum size
    */
  def getFilesBySize(settings: TicketsLoaderSettings)(sc: SparkContext): List[Array[String]] = {
    val fileList = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(settings.inputPath))
    val files: Array[(String, Long)] = fileList.map(x => Tuple2(x.getPath.toString, x.getLen))
    var group = 1
    val indexes = files.scanLeft(Tuple2(0,0))((prev, next) => {val value = prev._1 + next._2.toInt
      if(value > settings.groupSize){group = group+1
        Tuple2(next._2.toInt, group)} else Tuple2(value,group)})
      .map(x => x._2)
    val groups = files.map(_._1) zip indexes
    groups
      .groupBy(x => x._2)
      .values
      .toList
      .map(y => y.map(z => z._1))
  }
}