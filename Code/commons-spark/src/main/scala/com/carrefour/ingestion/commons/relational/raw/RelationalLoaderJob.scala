package com.carrefour.ingestion.commons.relational.raw

import com.carrefour.ingestion.commons.util.SparkJob
import com.carrefour.ingestion.commons.util.transform.{FieldInfo, FieldTransformationUtil, TransformationInfo}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory

object RelationalLoaderJob extends SparkJob[RelationalLoaderJobSettings] {

  val Logger = LoggerFactory.getLogger(getClass)

  override def run(jobSettings: RelationalLoaderJobSettings)(implicit sqlContext: SQLContext): Unit = {
    //Getting the metadata for the configuration of the load
    val metadata = IngestionMetadataLoader.loadMetadata(jobSettings)
    //Getting the transformations from the table specified in the settings
    val transformations = FieldTransformationUtil.loadTransformations(jobSettings.transformationsTable)

    //Starting files load
    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    metadata.foreach( settings => {

      Logger.info(s"Loading file ${settings.inputPath}")
      val path = new Path(settings.inputPath)
      val status = fs.globStatus(path)
      //Checking whether the input path specified exists or not
      if(status == null){
        Logger.error(s"Invalid parameter ${settings.inputPath}. File doesnt exist.")
        throw new IllegalArgumentException(s"Invalid parameter ${settings.inputPath}. File doesnt exist.")
      }
      /*if (status.length == 0) {
        Logger.error(s"Invalid parameter ${settings.inputPath}. Path doesnt exist.")
        throw new IllegalArgumentException(s"Invalid parameter ${settings.inputPath}. Path doesnt exist.")
      }*/


      /*val (inputFiles: Seq[String], singleFile: Boolean) = if (fs.isDirectory(path)) {
        val it = fs.listFiles(path, false)
        var files: Seq[String] = Seq[String]()
        while (it.hasNext()) {
          files = it.next().getPath.toString() +: files
        }
        (files, false)
      } else (Seq(path.toString()), true)*/

      val fullOutputTable = s"${settings.outputDb}.${settings.outputTable}"

      loadFile(settings.inputPath, fullOutputTable, transformations)(settings, sqlContext)
    })
  }

  /**
    * Loads the file in the given path into a table in the output DB,
    * applying the given field transformations. Output table name will be given
    * by the file name and field names by the file header.
    *
    * @param inputPath Path of the file to be loaded.
    * @param outputTable Schema and name of the table where the data will be stored (schema.name).
    * @param transformations Map with the transformations to be applied to the fields.
    * @param settings Collection of the necessary settings for the table to be loaded.
    * @param sqlContext SQL Context used for loading the input file from HDFS
    */
  def loadFile(inputPath: String, outputTable: String, transformations: Map[String, Map[String, TransformationInfo]])(implicit settings: RelationalLoaderJobSettings, sqlContext: SQLContext): Unit = {
    val loadYear = settings.year
    val loadMonth = settings.month
    val loadDay = settings.day
    Logger.info(s"Processing file $inputPath to $outputTable for date ${loadYear}${loadMonth}${loadDay}")
    val input = settings.format match {
      case RelationalFormats.TextFormat => sqlContext.sparkContext.textFile(inputPath, settings.numPartitions)
      case RelationalFormats.GzFormat => sqlContext.sparkContext.textFile(inputPath)
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

    val fieldsInfo = extractFieldsInfo(input, outputTable, transformations)

    Logger.info(s"Inserting in table $outputTable with fields: ${fieldsInfo.map(_.field).mkString(",")}")
    Logger.info(s"Loaded transformations: ${transformations.mkString(",")}")
    Logger.info(s"Schema after extractFieldsInfo: ${fieldsInfo.map(_.transformation.getClass.getName).mkString(",")}")

    val contentRaw = if (settings.header > 0)
      // discard header  
      input.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 0) {
        iter.drop(settings.header)
      } else iter
    }
    else input

    val content = (if (settings.format == RelationalFormats.GzFormat) repartition(contentRaw, settings.numPartitions) else contentRaw).
      // filter empty records
      filter(!_.isEmpty()).
      // split fields
      map(_.split(settings.fieldDelimiter, -1)).
      // apply transformations
      map(fields => (fields zip fieldsInfo).
        map { case (fieldValue, fieldInfo) =>
          fieldInfo.transformation.transform(fieldValue, fieldInfo.transformationArgs: _*) }).
      // add partition fields
      map(x => x ++ Array(loadYear, loadMonth, loadDay)).
      // build row
      map(Row(_: _*))
    
    Logger.info(s"Dropping existing partition: year=${settings.year}, month=${settings.month}, day=${settings.day}") 
    sqlContext.sql(s"ALTER TABLE $outputTable DROP IF EXISTS PARTITION (year=${settings.year}, month=${settings.month}, day=${settings.day})")
    Logger.info(s"Writing in table $outputTable")
//    Logger.info(s"Content First Line:\n ${content.first()}")
//    Logger.info(s"Table Schema:\n ${schema} ")
//    Logger.info(s"Dataframe Schema:\n ${df.printSchema()} ")
    try {
      if (!content.isEmpty()) {
        val schema = sqlContext.table(outputTable).schema

        val df = sqlContext.createDataFrame(content, schema)
        df.write.insertInto(outputTable)
      }else{
        Logger.warn(s"Warning: File $inputPath is empty")
      }
    }
    catch{
      case e: Exception =>
        Logger.info(s"${content.first().schema}")
        e.printStackTrace()
        throw e
    }
  }

  /**
   * Builds the {@link FieldInfo} sequence from the given input RDD and field transformations specification.
   * Field names are taken from Hive Table Definition.
   */
  def extractFieldsInfo(input: RDD[String], tableName: String, transformations: Map[String, Map[String, TransformationInfo]])(implicit settings: RelationalLoaderJobSettings, sqlContext: SQLContext): Seq[FieldInfo] = {
    val fieldNames = sqlContext.table(tableName).schema.fields.map(_.name)
    FieldInfo.buildFieldsInfo(tableName.split("\\.").last, fieldNames, transformations)
  }

  /**
   * Repartitions the RDD if numPartitions > 0
   */
  def repartition[T](rdd: RDD[T], partitions: Int): RDD[T] = if (partitions > 0) rdd.repartition(partitions) else rdd

}