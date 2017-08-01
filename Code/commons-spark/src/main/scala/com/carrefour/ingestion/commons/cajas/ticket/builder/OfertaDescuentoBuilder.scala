package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class OfertaDescuentoBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_oferta_descuento"

  val ArtCodField = "artcod"
  val SfamCodField = "sfamcod"
  val TipoField = "tipo"
  val CanField = "can"
  val IField = "i"
  val OfeIdField = "ofeid"

  private[this] val Fields = Seq[String](
    ArtCodField,
    SfamCodField,
    TipoField,
    CanField,
    IField,
    OfeIdField)

  override def fieldNames() = Fields
}