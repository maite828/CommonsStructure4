package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class SadBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_sad"

  val TAccionField = "taccion"
  val IdentCliField = "identcli"
  val NTarjClubField = "ntarjclub"
  val DniField = "dni"
  val NTarjPagField = "ntarjpag"
  val FCadField = "fcad"
  val ModPagField = "modpag"
  val IPorteField = "iporte"

  private[this] val Fields = Seq[String](
    TAccionField,
    IdentCliField,
    NTarjClubField,
    DniField,
    NTarjPagField,
    FCadField,
    ModPagField,
    IPorteField)

  override def fieldNames() = Fields
}