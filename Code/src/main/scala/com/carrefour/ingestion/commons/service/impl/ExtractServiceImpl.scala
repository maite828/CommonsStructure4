package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.bean._
import com.carrefour.ingestion.commons.controller.IngestionSettings
import com.carrefour.ingestion.commons.exception.FatalException
import com.carrefour.ingestion.commons.exception.logging.{CommonsException, LazyLogging}
import com.carrefour.ingestion.commons.repository.SparkSessionRepository
import com.carrefour.ingestion.commons.repository.impl.SparkSessionRepositoryImpl
import com.carrefour.ingestion.commons.service.ExtractService
import com.carrefour.ingestion.commons.service.manage.queries.CommonsLoadQueries
import org.apache.spark.sql.DataFrame

/**
  * Antiguo Metadata loader
  */

object ExtractServiceImpl extends ExtractService with CommonsLoadQueries with LazyLogging {

  private val spark: SparkSessionRepository = SparkSessionRepositoryImpl
  val defaultNumPartitions = 8

  override def nameApp(app: String) = spark.getSparkSession(app)

  /**
    * @param settings Basic configuration loaded through parameters in the command line.
    * @return - A dataframe with all the metadata associated to the businessunit specified in settings
    */
  override def loadMetadata(settings: IngestionSettings): Array[IngestionMetadata] = {

    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
    val msgError: String = s"$methodName > Error al realizar la carga de parámetria"
    initLog(methodName)

    val query = sQueryLoadMetadata_Entity(settings.businessunit, settings.entity)
    warnLog(query)
    val metadata: Option[DataFrame] = Option(spark.getSparkSession().sql(query))

    if (Option(metadata) isEmpty) throw CommonsException(msgError)
    endLog(methodName)

    metadata match {
      case Some(metadat) =>
        metadat.collect.map(row => {
          val table_name: String = row.getAs[String]("table_name")
          val schema_name: String = row.getAs[String]("schema_name")
          val parentpath: String = row.getAs[String]("parentpath")
          val filemask: String = row.getAs[String]("filemask")
          val fileformat_type: String = row.getAs[String]("fileformat_type")
          val fileformat_format: String = row.getAs[String]("fileformat_format")
          val fielddelimiter: String = row.getAs[String]("fielddelimiter")
          val linedelimiter: String = row.getAs[String]("linedelimiter")
          val endswithdelimiter: Boolean = row.getAs[Boolean]("endswithdelimiter")
          val headerlines: Int = row.getAs[Int]("headerlines")
          val datedefaultformat: String = row.getAs[String]("datedefaultformat")
          val enclosechar: String = row.getAs[String]("enclosechar")
          val escapechar: String = row.getAs[String]("escapechar")

          val numPartitions = if (settings.numPartitions == 0) defaultNumPartitions else settings.numPartitions

          val fileFormat = fileformat_format match {
            case "TEXT" => FileFormats.TextFormat
            case "GZ" => FileFormats.GzFormat
            case "TAR.GZ" => FileFormats.TarGzFormat
            case "ZIP" => FileFormats.ZipFormat
          }

          val fileType = fileformat_type match {
            case "DELIMITED" => DelimitedFileType(fileFormat, numPartitions, fielddelimiter, linedelimiter, endswithdelimiter, headerlines, datedefaultformat, enclosechar, escapechar)
            case "TICKET" => NoFileType()
            case "MOVI" => NoFileType()
          }

          //FIXME no mapear -> nuevo objeto con lo necesario
          IngestionMetadata(
            s"$parentpath/$filemask",
            schema_name,
            table_name,
            fileType,
            datedefaultformat,
            enclosechar,
            settings.date,
            settings.year,
            settings.month,
            settings.day,
            settings.businessunit,
            table_name
          )
        })
      case None => throw new FatalException("No se han devuelto datos de metadatos.")
    }
  }
}



