package com.carrefour.ingestion.commons.service.impl

import com.carrefour.ingestion.commons.bean.{DelimitedFileType, FileFormats, IngestionMetadata}
import com.carrefour.ingestion.commons.controller.IngestionSettings
import com.carrefour.ingestion.commons.core.{Repositories, Services}
import com.carrefour.ingestion.commons.exception.logging.LazyLogging
import com.carrefour.ingestion.commons.service.LoadService
import com.carrefour.ingestion.commons.service.transform.{FieldInfo, FieldTransformationUtil, TransformationInfo}
import com.carrefour.ingestion.commons.util.TreatmentDates
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}

/**
  * Antiguo File Loader
  */
object LoadServiceImpl extends LoadService with LazyLogging{

  /**
    * Main method for the ingestion of the files.
    *
    * @param jobSettings Previously loaded settings through the command line that will be used for the ingestion
    */

  override def run(jobSettings: IngestionSettings): Unit = {
    //Getting the metadata for the configuration of the load
    val metadata = Services.metadataService.loadMetadata(jobSettings)
    //Getting the transformations from the table specified in the settings
    //val transformations = FieldTransformationUtil.loadTransformations(jobSettings.transformationsTable)

    //Starting files load
    //The ingestion process will be run for each file found in the metadata configuration
    metadata.foreach(settings => {
      infoLog(s"Loading file ${settings.inputPath}")
      val path = new Path(settings.inputPath)
      val status = Repositories.dfs.getFileSystem().globStatus(path)
      //Checking whether the input path specified exists or not
      if (status == null) {
        errorLog(s"Invalid parameter ${settings.inputPath}. File doesnt exist.")
        throw new IllegalArgumentException(s"Invalid parameter ${settings.inputPath}. File doesnt exist.")
      }
      // A specific load process will be used depending on the file type
      settings.fileType match {
        case dft: DelimitedFileType => loadDelimitedFile(dft, settings)

      }
    })
  }

  override def loadDelimitedFile(fileType: DelimitedFileType, settings: IngestionMetadata): Unit = {

    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
    initLog(methodName)

    //val datepart:Array[Int] = TreatmentDates.getPartitionFields(settings)
    //warnLog(datepart(0).toString)

    val outputTable = s"${settings.outputDb}.${settings.outputTable}"
    infoLog(s"Processing file ${settings.inputPath} to $outputTable for date ${datepart(0)}${datepart(1)}${datepart(2)}")

    val input = fileType.fileFormat match {
      case FileFormats.TextFormat => Repositories.spark.getSparkSession().sparkContext.textFile(settings.inputPath, fileType.numPartitions)
      case FileFormats.GzFormat => Repositories.spark.getSparkSession().sparkContext.textFile(settings.inputPath)
      case f =>
        errorLog(s"Unsupported format $f. Supported formats are: text, gz.")
        throw new IllegalArgumentException(s"Unsupported format $f. Supported formats are: text, gz.")
    }

    //val fieldsInfo = extractFieldsInfo(input, outputTable, transformations)

    // Transformation and write
    //Logger.info(s"Inserting in table $outputTable with fields: ${fieldsInfo.map(_.field).mkString(",")}")
    try {
      val contentRaw = if (fileType.headerLines > 0)
      // discard header
        input.mapPartitionsWithIndex { (idx, iter) =>
          if (idx == 0) {
            iter.drop(fileType.headerLines)
          } else iter
        }
      else input

      val regs = (if (fileType.fileFormat == FileFormats.GzFormat) repartition(contentRaw, fileType.numPartitions) else contentRaw).
        // filter empty records
        filter(!_.isEmpty).
        // split fields
        map(_.split(fileType.fieldDelimiter, -1))


      val schema = Repositories.spark.getSparkSession().table(outputTable).schema
      // mark bad records and apply transformations

      val partFields = settings.partitionType.getPartitionFields(settings)

      val content = regs.
        map(fields => {
          //FIXME Test fields length properly
          Row(FieldTransformationUtil.applySchema(fields ++ Array(datepart(0), datepart(1),datepart(2)), schema, settings): _*)
        })
      endLog(methodName)
      infoLog(s"Writing in table $outputTable")
      val df = Repositories.spark.getSparkSession().createDataFrame(content, schema)
      df.repartition(8).write.mode(SaveMode.Overwrite).insertInto(outputTable)

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  /**
    * Builds the FieldInfo sequence from the given input RDD and field transformations specification.
    * Field names are taken from Hive Table Definition.
    */
  override def extractFieldsInfo(input: RDD[String], tableName: String, transformations: Map[String, Map[String, TransformationInfo]]): Seq[FieldInfo] = {
    val fieldNames = Repositories.spark.getSparkSession().table(tableName).schema.fields.map(_.name)
    FieldInfo.buildFieldsInfo(tableName.split("\\.").last, fieldNames, transformations)
  }

  /**
    * This method performs a repartition of the RDD if numPartitions > 0
    */
  def repartition[T](rdd: RDD[T], partitions: Int): RDD[T] = if (partitions > 0) rdd.repartition(partitions) else rdd

  /**
    * Validate RDD row format
    */
  def validateRowLength[T](row: Row, num_fields: Int): Boolean = {
    if (row.length == num_fields)
      true
    else
      false
  }


}
