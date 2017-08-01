package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class ReglasCalculoBonificacionBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_reglas_calculo_bonificacion"

  val NAcumField = "nacum"
  val SegmentoField = "segmento"
  val CodigoField = "codigo"
  val AmbitoField = "ambito"
  val SurtidoField = "surtido"
  val TipoBonField = "tipobon"
  val ValorBonField = "valorbon"
  val TipoBnfPagoField = "tipobnfpago"
  val ValorBnfPagoField = "valorbnfpago"
  val ImporteBaseField = "importebase"
  val CantField = "cant"
  val ImporteBnfField = "importebnf"
  val ImportePagoField = "importepago"

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
    ImporteBaseField,
    CantField,
    ImporteBnfField,
    ImportePagoField)

  override def fieldNames() = Fields
}