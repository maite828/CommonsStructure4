package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.exceptions.FatalException
import com.carrefour.ingestion.commons.util.SqlUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame


object RelationalFormats {
  sealed trait RelationalFormat
  case object TextFormat extends RelationalFormat
  case object GzFormat extends RelationalFormat
  case object ZipFormat extends RelationalFormat
}

/**
 * Parser for the relational data loader program. Method  {@link #parse} produces a {@link RelationalJobSettings}
  * to configure the Spark job.
 */
object IngestionMetadataLoader {

  def loadMetadata(businessunit: String)(implicit sqlContext: SQLContext): Array[RelationalLoaderJobSettings] = {

    val metadata = SqlUtils.sql("/hql/loadMetadata.hql")
    metadata match{
      case Some(metadata) =>
        metadata.map(row => {
          val businessunit_id: Int = row.getAs[Int]("businessunit_id")
          val businessunit_name: String = row.getAs[String]("businessunit_name")
          val table_id: Int = row.getAs[Int]("table_id")
          val table_name: String = row.getAs[String]("table_name")
          val schema_name: String = row.getAs[String]("schema_name")
          val storeformat: String = row.getAs[String]("storeformat")
          val compressiontype: String = row.getAs[String]("compressiontype")
          val transformationstable: String = row.getAs[String]("transformationstable")
          val transformationsschema: String = row.getAs[String]("transformationsschema")
          val file_id: Int = row.getAs[Int]("file_id")
          val file_name: String = row.getAs[String]("file_name")
          val parentpath: String = row.getAs[String]("parentpath")
          val filemask: String = row.getAs[String]("filemask")
          val fileformat_id: Int = row.getAs[Int]("fileformat_id")
          val fileformat_type: String = row.getAs[String]("fileformat_type")
          val fielddelimiter: String = row.getAs[String]("fielddelimiter")
          val linedelimiter: String = row.getAs[String]("linedelimiter")
          val endswithdelimiter: Boolean = row.getAs[Boolean]("endswithdelimiter")
          val headerlines: Int = row.getAs[Int]("headerlines")
          val datedefaultformat: String = row.getAs[String]("datedefaultformat")
          val enclosechar: String = row.getAs[String]("enclosechar")
          val escapechar: String = row.getAs[String]("escapechar")

          RelationalLoaderJobSettings(
            parentpath,
            schema_name,
            table_name,
            "",
            transformationsschema + "." + transformationstable,
            8,
            RelationalFormats.TextFormat,
            headerlines,
            1,
            2,
            3,
            4,
            "Mercancias"
          )
        }).collect()
      case None => throw new FatalException("No se han devuelto datos de metadatos.")
    }
  }
}
