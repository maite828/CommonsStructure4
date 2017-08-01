package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class FidelizacionBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_fidelizacion"

  val NumField = "num"
  val TarNumField = "tarnum"
  val FechaVtaField = "fechavta"
  val Puntos0Field = "puntos0"
  val Puntos1Field = "puntos1"
  val Puntos2Field = "puntos2"
  val Puntos3Field = "puntos3"
  val Puntos4Field = "puntos4"

  private[this] val Fields = Seq[String](
    NumField,
    TarNumField,
    FechaVtaField,
    Puntos0Field,
    Puntos1Field,
    Puntos2Field,
    Puntos3Field,
    Puntos4Field)

  override def fieldNames() = Fields
}