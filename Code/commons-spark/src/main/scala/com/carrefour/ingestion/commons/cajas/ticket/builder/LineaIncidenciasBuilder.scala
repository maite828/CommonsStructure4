package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class LineaIncidenciasBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_linea_incidencias"

  val TipoIcdField = "tipoicd"
  val ArtCodField = "artcod"
  val CanField = "can"
  val SfamCodField = "sfamcod"
  val PvpFileField = "pvpfile"
  val PvpOpeField = "pvpope"
  val VtaSNField = "vtasn"
  val DevSNField = "devsn"
  val IcdLlaveField = "icdllave"

  private[this] val Fields = Seq[String](
    TipoIcdField,
    ArtCodField,
    CanField,
    SfamCodField,
    PvpFileField,
    PvpOpeField,
    VtaSNField,
    DevSNField,
    IcdLlaveField)

  override def fieldNames() = Fields
}