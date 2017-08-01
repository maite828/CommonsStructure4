package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class TicketBalanzasBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_ticket_balanzas"

  val TckBlzField = "yckblz"
  val ArtCodField = "artcod"
  val SecTckField = "dectck"
  val PesoField = "peso"
  val PvpKgField = "pvpkg"
  val ImpLinField = "implin"
  val IcdVtaField = "icdvta"

  private[this] val Fields = Seq[String](
    TckBlzField,
    ArtCodField,
    SecTckField,
    PesoField,
    PvpKgField,
    ImpLinField,
    IcdVtaField)

  override def fieldNames() = Fields
}