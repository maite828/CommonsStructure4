package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class PromoSorteoLocalBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_promocion_sorteo_local"

  val NPromoField = "npromo"
  val PromTipoField = "promtipo"
  val CalculoField = "calculo"
  val ValeCodField = "valecod"
  val ImpTckField = "imptck"
  val BaseField = "base"
  val ImpPromoField = "imppromo"
  val ImpBnfField = "impbnf"
  val ImpTotalField = "imptotal"
  val OrigenField = "origen"
  val RatioField = "ratio"

  private[this] val Fields = Seq[String](
    NPromoField,
    PromTipoField,
    CalculoField,
    ValeCodField,
    ImpTckField,
    BaseField,
    ImpPromoField,
    ImpBnfField,
    ImpTotalField,
    OrigenField,
    RatioField)

  override def fieldNames() = Fields
}