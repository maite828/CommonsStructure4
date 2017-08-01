package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class PromocionCentralDiferidaBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_promocion_central_diferida"

  val NPromField = "nprom"
  val PromTipoField = "promtipo"
  val CalculoField = "calculo"
  val CodigoField = "codigo"
  val ImpTckField = "imptck"
  val BaseField = "base"
  val ImpPromoField = "imppromo"
  val ValeCodField = "valecod"
  val OrigenField = "origen"
  val RatioField = "ratio"

  private[this] val Fields = Seq[String](
    NPromField,
    PromTipoField,
    CalculoField,
    CodigoField,
    ImpTckField,
    BaseField,
    ImpPromoField,
    ValeCodField,
    OrigenField,
    RatioField)

  override def fieldNames() = Fields
}