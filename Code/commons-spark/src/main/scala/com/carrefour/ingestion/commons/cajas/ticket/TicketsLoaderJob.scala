package com.carrefour.ingestion.commons.cajas.ticket

import com.carrefour.ingestion.commons.cajas.ticket.builder.TicketRowBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.LoggerFactory
import com.carrefour.ingestion.commons.util.SparkJob
import com.carrefour.ingestion.commons.util.transform.FieldTransformationUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel

object TicketsLoaderJob extends SparkJob[TicketsLoaderSettings] {

  val Logger = LoggerFactory.getLogger(getClass)

  val RecordSep = "\\r?\\n"
  val FieldSep = ":"

  override def run(settings: TicketsLoaderSettings)(implicit sparkSession: SparkSession): Unit = {

    import sparkSession.implicits._
    val builders = sparkSession.table(settings.rowBuilderTable).map(r => BuilderSpec(r.getAs("recordtype"), r.getAs("builderclass"))).collect()

    val transformations = FieldTransformationUtil.loadTransformations(settings.transformationsTable)

    implicit val sc = sparkSession.sparkContext

    // (ticketInfo, recordType, recordFields)
    val pattern =  """(.*\d{8})_(\d{4}).*""".r

    val fileList = FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(settings.inputPath))
      .map(x => x.getPath.toString)
      .map(x => { val pattern(fecha,numTicket) = x
        fecha})
      .distinct
      .map(_ + "*")
      .grouped(settings.window)
      .foreach(ticket => {
        val tkLines: RDD[(TicketInfo, String, Array[String])] = TUtils.ticketFiles(ticket, settings).
          repartition(500).
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
          try {
            Logger.debug(s"Schema with ${builder.value.getSchema.fields.size} fields: ${builder.value.getSchema.fields.map(sf => sf.name).mkString("||")}")

            val tkDf = sparkSession.createDataFrame(tkRows, builder.value.getSchema(settings.outputDb, builder.value.tableName)(sparkSession))
            tkDf.coalesce(8).write.insertInto(s"${settings.outputDb}.${builder.value.tableName}")
          } catch {
            case e: Exception => {
              Logger.info(s"Tried to insert row with schema= ${builder.value.getSchema}")
              throw e
            }
          }
      }
      tkLines.unpersist(true)
    })
  }

  case class BuilderSpec(recordType: String, builderClass: String)
}