package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class DatosBoletaBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_datos_boleta"

  val OperationIdField = "operationid"
  val TarNField = "tarn"
  val NombreField = "nombre"
  val Apellido1Field = "apellido1"
  val Apellido2Field = "apellido2"
  val TipoViaField = "tipoVia"
  val NombreViaField = "nombrevia"
  val NumViaField = "numvia"
  val RestoViaField = "restovia"
  val CPField = "cp"
  val PoblacionField = "poblacion"
  val ProvinciaField = "provincia"
  val RefrigeradoField = "refrigerado"
  val CongeladoField = "congelado"
  val AscensorField = "ascensor"
  val DiaSemField = "diasem"
  val TramoField = "tramo"

  private[this] val Fields = Seq[String](
    OperationIdField,
    TarNField,
    NombreField,
    Apellido1Field,
    Apellido2Field,
    TipoViaField,
    NombreViaField,
    NumViaField,
    RestoViaField,
    CPField,
    PoblacionField,
    ProvinciaField,
    RefrigeradoField,
    CongeladoField,
    AscensorField,
    DiaSemField,
    TramoField)

  override def fieldNames() = Fields
}