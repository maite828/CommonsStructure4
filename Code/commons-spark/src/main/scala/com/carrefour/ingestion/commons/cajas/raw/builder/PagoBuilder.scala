package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.cajas.raw.TicketInfo
import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class PagoBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_pago"

  val CtoConField = "ctocon"
  val CanField = "can"
  val IField = "i"
  val DocNField = "docn"
  val EntAuxNField = "entauxn"
  val EntAuxField = "entaux"

  private[this] val Fields = Seq[String](
    CtoConField,
    CanField,
    IField,
    DocNField,
    EntAuxNField,
    EntAuxField)

  override def fieldNames() = Fields

  override protected def selectFields(fields: Seq[String], ticketInfo: TicketInfo): Seq[String] = {
    fields.take(Fields.size - 1) :+ fields.takeRight(fields.size - (Fields.size - 1)).mkString(":")
  }
}