package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class DetalleCalculoBonificacionBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_detalle_calculo_bonificacion"

  val NAcumField = "nacum"
  val SegmentoField = "segmento"
  val CodigoField = "codigo"
  val AmbitoField = "ambito"
  val SurtidoField = "surtido"
  val TipoBonField = "tipobon"
  val ValorBonField = "valorbon"
  val TipoBnfPagoField = "tipobnfpago"
  val ValorBnfPagoField = "valorbnfpago"
  val CodArtField = "codart"
  val SFamCodField = "sfamcod"
  val SecCodField = "seccod"
  val CantField = "cant"
  val ImporteBnfField = "importebnf"

  private[this] val Fields = Seq[String](
    NAcumField,
    SegmentoField,
    CodigoField,
    AmbitoField,
    SurtidoField,
    TipoBonField,
    ValorBonField,
    TipoBnfPagoField,
    ValorBnfPagoField,
    CodArtField,
    SFamCodField,
    SecCodField,
    CantField,
    ImporteBnfField)

  override def fieldNames() = Fields
}