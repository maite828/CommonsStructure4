package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class CuponEmitidoBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_cupon_emitido"

  val NumField = "num"
  val TarNumField = "tarnum"
  val CuponField = "cupon"
  val SegmField = "segm"
  val NumCupField = "numcup"
  val TipoCupField = "tipocup"

  private[this] val Fields = Seq[String](
    NumField,
    TarNumField,
    CuponField,
    SegmField,
    NumCupField,
    TipoCupField)

  override def fieldNames() = Fields

}