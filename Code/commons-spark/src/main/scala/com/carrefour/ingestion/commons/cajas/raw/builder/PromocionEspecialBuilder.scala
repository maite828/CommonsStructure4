package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class PromocionEspecialBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_promocion_especial"

  val TipoField = "tipo"
  val CodValeField = "codvale"
  val ImpBaseField = "impbase"
  val DtoField = "dto"
  val ImpValeField = "impvale"

  private[this] val Fields = Array[String](
    TipoField,
    CodValeField,
    ImpBaseField,
    DtoField,
    ImpValeField)

  override def fieldNames() = Fields
}