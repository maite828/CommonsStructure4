package com.carrefour.ingestion.commons.cajas.movi

import com.carrefour.ingestion.commons.cajas.movi.builder.{MoviRowBuilder, MoviRowBuilderUtil}
import com.carrefour.ingestion.commons.cajas.ticket.TUtils
import com.carrefour.ingestion.commons.util.SparkJob
import com.carrefour.ingestion.commons.util.transform.FieldTransformationUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.LoggerFactory

object MoviLoaderJob extends SparkJob[MoviLoaderSettings] {

  val Logger = LoggerFactory.getLogger(getClass)

  val RecordSep = "\\r?\\n"
  val CommonSpecificSep = ";"
  val FieldSep = ":"

  override def run(settings: MoviLoaderSettings)(implicit sparkSession: SparkSession): Unit = {

    val confFields = MoviRowBuilderUtil.loadFieldsConf(settings.fieldsConfTable)
    val transformations = FieldTransformationUtil.loadTransformations(settings.transformationsTable)

    // (moviInfo,(commonFields, specificFields))
    val moviLines: RDD[(MoviInfo, (Seq[String], Seq[String]))] = TUtils.moviFiles(settings)(sparkSession.sparkContext).
      flatMap { case (file, content) => content.split(RecordSep).map((MoviInfo(file), _)) }.
      mapValues(_.split(CommonSpecificSep, -1)).
      filter { case (moviInfo, line) => line.size > 1 || (line.size == 1 && !line.head.isEmpty) }. // discard empty lines
      filter {
        case (moviInfo, splits) =>
          if (splits.size > 2)
            Logger.warn(s"""Unparseable record $moviInfo with more than 2 parts separated by "$CommonSpecificSep" will be ignored: ${splits.mkString(CommonSpecificSep)}""")
          splits.size <= 2
      }.
      mapValues(splits => (splits(0).split(FieldSep, -1), if (splits.size == 2) splits(1).split(FieldSep, -1) else Seq.empty))

    val rowBuilder = sparkSession.sparkContext.broadcast(new MoviRowBuilder(confFields, transformations))

    val rows = moviLines.map {
      case (moviInfo, (commonFields, specificFields)) =>
        val r = rowBuilder.value.buildRow(moviInfo, commonFields, specificFields)
        if (r.isFailure) Logger.warn(s"""Unparseable record $moviInfo will be ignored: ${commonFields.mkString(FieldSep)}$CommonSpecificSep${specificFields.mkString(FieldSep)}""")
        r
    }.filter(_.isSuccess).map(_.get)

    Logger.debug(s"Schema with ${rowBuilder.value.getSchema.fields.size} fields: ${rowBuilder.value.getSchema.fields.map(sf => sf.name).mkString("||")}")
    val df = sparkSession.createDataFrame(rows, rowBuilder.value.getSchema)
    df.write.partitionBy(MoviRowBuilder.DatePartField).insertInto(s"${settings.outputDb}.${MoviRowBuilder.TableName}")

  }
}