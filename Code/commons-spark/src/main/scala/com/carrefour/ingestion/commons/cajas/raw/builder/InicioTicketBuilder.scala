package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.cajas.raw.TicketInfo
import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class InicioTicketBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_inicio_ticket"

  val TipoTicketField = "tipotk"
  val FechaHoraField = "fechahora"
  val PlantaField = "planta"
  val PosFlipField = "posflip"
  val ConNField = "conn"
  val OpeField = "ope"
  val OpeFlipField = "opeflip"
  val HoraIniField = "horaini"
  val HoraTkAntField = "horatkant"
  val VtaDevField = "vtadev"
  val TkTipoField = "tktipo"

  private[this] val Fields = Seq[String](
    TipoTicketField,
    FechaHoraField,
    PlantaField,
    PosFlipField,
    ConNField,
    OpeField,
    OpeFlipField,
    HoraIniField,
    HoraTkAntField,
    VtaDevField,
    TkTipoField)

  override def fieldNames() = Fields

  override protected def selectFields(fields: Seq[String], ticketInfo: TicketInfo): Seq[String] = {
    Seq(ticketInfo.tipo, s"${ticketInfo.fecha} ${fields(7)}", fields(1)) ++ fields.slice(3, 11)
  }
}