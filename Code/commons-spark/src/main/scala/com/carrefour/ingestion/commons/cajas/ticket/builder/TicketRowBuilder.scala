package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.cajas.ticket.TicketInfo
import com.carrefour.ingestion.commons.util.transform.{FieldInfo, TransformationInfo}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

abstract class TicketRowBuilder(allTransformations: Map[String, Map[String, TransformationInfo]]) extends Serializable {

  @transient
  private lazy val Logger = LoggerFactory.getLogger(classOf[TicketRowBuilder])

  /**
   * Transformations to be applied to each fields, preserving field order
   */
  lazy val fieldsInfo: Seq[FieldInfo] = FieldInfo.buildFieldsInfo(tableName, TicketRowBuilder.CommonFields ++ fieldNames, allTransformations)

  def tableName: String

  final def getSchema: StructType = StructType(fieldsInfo.map(_.schema))

  final def buildRow(ticketInfo: TicketInfo, fields: Seq[String]): Row = {
    var rawFields = Array(ticketInfo.fecha, ticketInfo.fecha, ticketInfo.tienda, ticketInfo.pos, ticketInfo.num) ++ selectFields(fields, ticketInfo)
    if (rawFields.size < fieldsInfo.size) {
      //FIXME use log (nullpointerexception in distributed mode)
      Logger.warn(s"Expected ${fieldsInfo.size} fields, found ${rawFields.size}: ${rawFields.mkString("||")}. Filling missing fields with nulls.")
      rawFields = rawFields ++ Array.fill(fieldsInfo.size - rawFields.size)(null)
    }
    val transformedFields = (rawFields zip fieldsInfo).
      map { case (fieldValue, fieldInfo) => fieldInfo.transformation.transform(fieldValue, fieldInfo.transformationArgs: _*) }
    Row(transformedFields: _*)
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
  val DatePartField = "date_part"
  val FechaField = "fecha"
  val TiendaField = "tienda"
  val PosField = "pos"
  val NumTicketField = "numtk"

  private val CommonFields = Array(
    DatePartField,
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
