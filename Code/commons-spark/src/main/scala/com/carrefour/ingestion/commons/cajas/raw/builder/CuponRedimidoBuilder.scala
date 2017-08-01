package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class CuponRedimidoBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_cupon_redimido"

  val ArtCodField = "artcod"
  val FamCodField = "famcod"
  val SecCodField = "seccod"
  val AmbCupField = "ambcup"
  val CantArtField = "cantart"
  val ImpDtoField = "impdto"
  val TipCupField = "tipcup"
  val CodCupField = "codcup"
  val IVATipoField = "ivatipo"

  private[this] val Fields = Seq[String](
    ArtCodField,
    FamCodField,
    SecCodField,
    AmbCupField,
    CantArtField,
    ImpDtoField,
    TipCupField,
    CodCupField,
    IVATipoField)

  override def fieldNames() = Fields
}