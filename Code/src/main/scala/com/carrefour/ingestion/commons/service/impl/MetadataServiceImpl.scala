package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.bean._
import com.carrefour.ingestion.commons.controller.IngestionSettings
import com.carrefour.ingestion.commons.core.Repositories
import com.carrefour.ingestion.commons.exception.FatalException
import com.carrefour.ingestion.commons.exception.logging.CommonsException
import com.carrefour.ingestion.commons.repository.impl.HiveRepositoryImpl.sQueryLoadMetadata_Entity
import com.carrefour.ingestion.commons.service.MetadataService
import com.carrefour.ingestion.commons.service.impl.ExtractServiceImpl.{defaultNumPartitions, endLog, infoLog, initLog}
import org.apache.spark.sql.DataFrame


object MetadataServiceImpl extends MetadataService {

  /**
    * @param settings Basic configuration loaded through parameters in the command line.
    * @return - A dataframe with all the metadata associated to the businessunit specified in settings
    */
  override def loadMetadata(settings: IngestionSettings): Array[IngestionMetadata] = {

    val metadata = sqlMetadata(settings)

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
          val partition_type: String = row.getAs[String]("partition_type")
          val partition_param: String = row.getAs[String]("partition_param")
          val partition_date_format: String = row.getAs[String]("partition_date_format")
          val partition_transform: String = row.getAs[String]("partition_transform")

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

          val partitionType = partition_type match {
            case "1" => ParameterPartitionType(settings.day.toString, settings.month.toString, settings.year.toString).getPartitionFields(filemask, partition_param, partition_date_format, partition_transform)
            case "2" => FileNamePartitionType(filemask, partition_param, partition_date_format, partition_transform)
          }

          //FIXME no mapear -> nuevo objeto con lo necesario
          IngestionMetadata(
            s"$parentpath/$filemask",
            schema_name,
            table_name,
            filemask,
            fileType,
            datedefaultformat,
            enclosechar,
            partitionType,
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

  /**
    *
    * @param settings
    * @return
    */
  def sqlMetadata(settings: IngestionSettings): Option[DataFrame] = {

    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
    val msgError: String = s"$methodName > Error al obtener la parámetria"
    initLog(methodName)

    val query = sQueryLoadMetadata_Entity(settings.businessunit, settings.entity)
    infoLog(query)

    val result = Repositories.hive.sql(query)

    //TODO Validar resultado
    if (result.isEmpty ) throw CommonsException(msgError)
    endLog(methodName)

    result
  }

}
