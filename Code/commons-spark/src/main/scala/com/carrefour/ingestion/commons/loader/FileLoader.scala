package com.carrefour.ingestion.commons.loader

import com.carrefour.ingestion.commons.bean.{DelimitedFileType, FileFormats}
import com.carrefour.ingestion.commons.exception.RowFormatException
import com.carrefour.ingestion.commons.util.transform.{FieldInfo, FieldTransformationUtil, TransformationInfo}
import com.carrefour.ingestion.commons.util.{SparkJob, SqlUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Generic file loader job.
  */
object FileLoader extends SparkJob[IngestionSettings] {

  val Logger = LoggerFactory.getLogger(getClass)

  /**
    * Main method for the ingestion of the files.
    *
    * @param jobSettings Previously loaded settings through the command line that will be used for the ingestion
    * @param sparkSession
    */
  override def run(jobSettings: IngestionSettings)(implicit sparkSession: SparkSession): Unit = {
    //Getting the metadata for the configuration of the load
    val metadata = IngestionMetadataLoader.loadMetadata(jobSettings)
    //Getting the transformations from the table specified in the settings
    //val transformations = FieldTransformationUtil.loadTransformations(jobSettings.transformationsTable)

    //Starting files load
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    //The ingestion process will be run for each file found in the metadata configuration
    metadata.foreach( settings => {
      Logger.info(s"Loading file ${settings.inputPath}")
      val path = new Path(settings.inputPath)
      val status = fs.globStatus(path)
      //Checking whether the input path specified exists or not
      if(status == null){
        Logger.error(s"Invalid parameter ${settings.inputPath}. File doesnt exist.")
        throw new IllegalArgumentException(s"Invalid parameter ${settings.inputPath}. File doesnt exist.")
      }
      // A specific load process will be used depending on the file type
      settings.fileType match {
        case dft: DelimitedFileType => loadDelimitedFile ( dft, settings)

      }
    })
  }

  def loadDelimitedFile(fileType: DelimitedFileType, settings: IngestionMetadata)(implicit sparkSession: SparkSession): Unit = {
    val loadYear = settings.date.toString.substring(0,4).toInt
    val loadMonth = settings.date.toString.substring(4,6).toInt
    val loadDay = settings.date.toString.substring(6,8).toInt
    val outputTable = s"${settings.outputDb}.${settings.outputTable}"
    Logger.info(s"Processing file ${settings.inputPath} to $outputTable for date ${loadYear}${loadMonth}${loadDay}")
    val input = fileType.fileFormat match {
      case FileFormats.TextFormat => sparkSession.sparkContext.textFile(settings.inputPath, fileType.numPartitions)
      case FileFormats.GzFormat => sparkSession.sparkContext.textFile(settings.inputPath)
      //FIXME support zip format
      //      case RelationalFormats.ZipFormat => 
      //        sqlContext.sparkContext.hadoopFile(inputPath, classOf[ZipInputFormat], classOf[Text], classOf[Text],
      //        settings.numPartitions).map(pair => pair._2.toString)
      //      binaryFiles(inputPath, settings.numPartitions).
      //        flatMap(f => ExtractionUtils.extractArchiveFiles(f._2, ExtractionUtils.ZipArchive).get). //FIXME exception
      //        map(c => ExtractionUtils.decode(c._2)).
      //        flatMap(c => c.split(RecordSep))
      case f =>
        Logger.error(s"Unsupported format $f. Supported formats are: text, gz.")
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
        filter(!_.isEmpty()).
        // split fields
        map(_.split(fileType.fieldDelimiter, -1))


      val schema = sparkSession.table(outputTable).schema
      // mark bad records and apply transformations

      val content = regs.
        map(fields => {
          //FIXME Test fields length properly
          /*if (fields.length == fieldsInfo.length - 3) {
            val transField = (fields zip fieldsInfo).
              map {
                case (fieldValue, fieldInfo) => fieldInfo.transformation.transform(fieldValue, fieldInfo.transformationArgs: _*)
              }
            // add partition fields and generate Row

            Row(transField ++ Array(loadYear, loadMonth, loadDay) :_*)
            */

          Row(FieldTransformationUtil.applySchema(fields ++ Array(loadYear, loadMonth, loadDay), schema,settings) :_*)
        })

      Logger.info(s"Writing in table $outputTable")
      val df = sparkSession.createDataFrame(content, schema)
      df.repartition(8).write.mode(SaveMode.Overwrite).insertInto(outputTable)

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  /**
    * Builds the {@link FieldInfo} sequence from the given input RDD and field transformations specification.
    * Field names are taken from Hive Table Definition.
    */
  def extractFieldsInfo(input: RDD[String], tableName: String, transformations: Map[String, Map[String, TransformationInfo]])(implicit sparkSession: SparkSession): Seq[FieldInfo] = {
    val fieldNames = sparkSession.table(tableName).schema.fields.map(_.name)
    FieldInfo.buildFieldsInfo(tableName.split("\\.").last, fieldNames, transformations)
  }

  /**
    * Repartitions the RDD if numPartitions > 0
    */
  def repartition[T](rdd: RDD[T], partitions: Int): RDD[T] = if (partitions > 0) rdd.repartition(partitions) else rdd

  /**
    * Validate RDD row format
    */
  def validateRowLength[T](row: Row, num_fields:Int): Boolean = {
    if (row.length == num_fields)
      true
    else
      false
  }


}