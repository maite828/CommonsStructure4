package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.exceptions.FatalException
import com.carrefour.ingestion.commons.util.SqlUtils
import org.apache.spark.sql.SQLContext

object IngestionMetadata {

}

/**
 * Parser for the relational data loader program. Method  {@link #parse} produces a {@link RelationalJobSettings}
 * to configure the Spark job.
 */
object IngestionMetadataLoader {

  /**
    *
    * @param settings Basic configuration loaded through parameters in the command line.
    * @param sqlContext
    * @return - A dataframe with all the metadata associated to the businessunit specified in settings
    */

  def loadMetadata(settings: RelationalLoaderJobSettings)(implicit sqlContext: SQLContext): Array[RelationalLoaderJobSettings] = {

    val metadata = if (settings.entity == "")
      SqlUtils.sql("/hql/load_BU_Metadata.hql", settings.businessunit) else
      SqlUtils.sql("/hql/load_Entity_Metadata.hql", settings.businessunit, settings.entity)
    metadata match{
      case Some(metadata) =>
        metadata.collect.map(row => {
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

          //FIXME no mapear -> nuevo objeto con lo necesario
          RelationalLoaderJobSettings(
            s"${parentpath}/${filemask}",
            schema_name,
            table_name,
            transformationsschema + "." + transformationstable,
            8, // num_partitions
            RelationalFormats.TextFormat,
            headerlines,
            fielddelimiter,
            settings.date,
            settings.year,
            settings.month,
            settings.day,
            settings.businessunit
          )
        })
      case None => throw new FatalException("No se han devuelto datos de metadatos.")
    }
  }
}
