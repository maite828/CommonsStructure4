package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.cajas.ticket.TicketInfo
import com.carrefour.ingestion.commons.util.transform.{FieldInfo, TransformationInfo}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

abstract class TicketRowBuilder(allTransformations: Map[String, Map[String, TransformationInfo]]) extends Serializable {

  @transient
  private lazy val Logger = LoggerFactory.getLogger(classOf[TicketRowBuilder])

  /**
   * Transformations to be applied to each fields, preserving field order
   */
  lazy val fieldsInfo: Seq[FieldInfo] = FieldInfo.buildFieldsInfo(tableName, TicketRowBuilder.CommonFields ++ fieldNames ++ Array("year","month","day"), allTransformations)

  def tableName: String

  final def getSchema: StructType = StructType(fieldsInfo.map(_.schema))

  final def getSchema(database: String, table: String)(sparkSession: SparkSession): StructType = sparkSession.table(s"${database}.${table}").schema

  final def buildRow(ticketInfo: TicketInfo, fields: Seq[String]): Row = {
    var rawFields = Array(ticketInfo.fecha, ticketInfo.tienda, ticketInfo.pos, ticketInfo.num) ++ selectFields(fields, ticketInfo)
    //FIXME checking the numbers of fields is correct
    if (rawFields.size < fieldsInfo.size - 3) {
      //FIXME use log (nullpointerexception in distributed mode)
      Logger.warn(s"Expected ${fieldsInfo.size} fields, found ${rawFields.size}: ${rawFields.mkString("||")}. Filling missing fields with nulls.")
      rawFields = rawFields ++ Array.fill(fieldsInfo.size - rawFields.size)(null)
    }
    val transformedFields = (rawFields zip fieldsInfo).
      map { case (fieldValue, fieldInfo) => fieldInfo.transformation.transform(fieldValue, fieldInfo.transformationArgs: _*) }
    Row(transformedFields ++ Array((ticketInfo.year), (ticketInfo.month), (ticketInfo.day)): _*)
  }

  /**
   * Specific fields of the row, in order
   */
  protected def fieldNames(): Seq[String]

  /**
   * Extracts the output fields and returns them in order. Default implementation selects first fields().size elements from fields array.
   */
  protected def selectFields(fields: Seq[String], ticketInfo: TicketInfo): Seq[String] = fields.take(fieldNames.size)
}

object TicketRowBuilder {
  val FechaField = "fecha"
  val TiendaField = "tienda"
  val PosField = "pos"
  val NumTicketField = "numtk"

  private val CommonFields = Array(
    FechaField,
    TiendaField,
    PosField,
    NumTicketField)

  /**
   * Instantiates the specified TicketRowBuilder subclass with the given transformations
   */
  def apply(builderClass: String, allTransformations: Map[String, Map[String, TransformationInfo]]): TicketRowBuilder = {
    Class.forName(builderClass).getConstructors()(0).newInstance(allTransformations).asInstanceOf[TicketRowBuilder]
  }
}
