package com.carrefour.ingestion.commons.cajas.raw

import com.carrefour.ingestion.commons.cajas.raw.builder.TicketRowBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import com.carrefour.ingestion.commons.util.SparkJob
import com.carrefour.ingestion.commons.util.transform.FieldTransformationUtil
import org.apache.spark.storage.StorageLevel

object TicketsLoaderJob extends SparkJob[TicketsLoaderSettings] {

  val Logger = LoggerFactory.getLogger(getClass)

  val RecordSep = "\\r?\\n"
  val FieldSep = ":"

  override def run(settings: TicketsLoaderSettings)(implicit sqlContext: SQLContext): Unit = {

    import sqlContext.implicits._
    val builders = sqlContext.table(settings.rowBuilderTable).map(r => BuilderSpec(r.getAs("recordtype"), r.getAs("builderclass"))).collect()

    val transformations = FieldTransformationUtil.loadTransformations(settings.transformationsTable)

    implicit val sc = sqlContext.sparkContext

    // (ticketInfo, recordType, recordFields)
    val tkLines: RDD[(TicketInfo, String, Array[String])] = TUtils.ticketFiles(settings).
      flatMap { case (filename, content) => content.split(RecordSep).map((TicketInfo(filename), _)) }.
      map { case (tkInfo, record) => (tkInfo, record.split(FieldSep, -1)) }.
      // discard empty lines
      filter { case (tkInfo, record) => record.size > 1 || (record.size == 1 && !record.isEmpty) }.
      map { case (tkInfo, fields) => (tkInfo, fields(0), fields.drop(1)) }

    tkLines.persist(StorageLevel.MEMORY_AND_DISK_SER)

    builders.foreach {
      case BuilderSpec(recordType, builderClass) =>
        val builder = sc.broadcast(TicketRowBuilder(builderClass, transformations))
        Logger.info(s"Processing ticket records of type $recordType to store at ${settings.outputDb}.${builder.value.tableName}")
        val tkRows = tkLines.filter(r => recordType.equals(r._2)).map(r => builder.value.buildRow(r._1, r._3))
        Logger.debug(s"Schema with ${builder.value.getSchema.fields.size} fields: ${builder.value.getSchema.fields.map(sf => sf.name).mkString("||")}")
        val tkDf = sqlContext.createDataFrame(tkRows, builder.value.getSchema)
        tkDf.write.partitionBy(TicketRowBuilder.DatePartField).insertInto(s"${settings.outputDb}.${builder.value.tableName}")
    }

  }

  case class BuilderSpec(recordType: String, builderClass: String)
}