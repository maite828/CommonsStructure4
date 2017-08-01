package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class DetalleEmisionBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_detalle_emision"

  val NPromoField = "npromo"
  val PromTipoField = "promtipo"
  val CalculoField = "calculo"
  val ValeCodField = "valecod"
  val CodArtField = "codart"
  val OrdNField = "ordn"
  val SFamCodField = "sfamcod"
  val SecCodField = "seccod"
  val IVATipoField = "ivatipo"
  val NetoIField = "netoi"

  private[this] val Fields = Seq[String](
    NPromoField,
    PromTipoField,
    CalculoField,
    ValeCodField,
    CodArtField,
    OrdNField,
    SFamCodField,
    SecCodField,
    IVATipoField,
    NetoIField)

  override def fieldNames() = Fields
}