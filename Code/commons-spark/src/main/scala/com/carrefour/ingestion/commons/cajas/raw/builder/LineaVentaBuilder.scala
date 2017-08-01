package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class LineaVentaBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_linea_venta"

  val ArtCodField = "artcod"
  val SfamCodField = "sfamcod"
  val IvaTipoField = "ivatipo"
  val LinTipoField = "lintipo"
  val VtaTipoField = "vtatipo"
  val VtaTimeField = "vtatime"
  val CanField = "can"
  val IField = "i"
  val PvpField = "pvp"
  val IcdPvpErrSNField = "icdpvperrsn"
  val IcdDatosErrSNField = "icddatoserrsn"
  val TipoEntField = "tipoent"
  val ArtTipoField = "arttipo"
  val PvpTipoField = "pvptipo"
  val SurtField = "surt"
  val EtiYellSNField = "etiyellsn"
  val OfeSNField = "ofesn"
  val SecCodField = "seccod"
  val OfeTipoField = "ofetipo"
  val SfamOfeDtoField = "sfamofedto"
  val SfamOfeTipoGpoField = "sfamofetipogpo"
  val VtaModField = "vtamod"

  private[this] val Fields = Seq[String](
    ArtCodField,
    SfamCodField,
    IvaTipoField,
    LinTipoField,
    VtaTipoField,
    VtaTimeField,
    CanField,
    IField,
    PvpField,
    IcdPvpErrSNField,
    IcdDatosErrSNField,
    TipoEntField,
    ArtTipoField,
    PvpTipoField,
    SurtField,
    EtiYellSNField,
    OfeSNField,
    SecCodField,
    OfeTipoField,
    SfamOfeDtoField,
    SfamOfeTipoGpoField,
    VtaModField)

  override def fieldNames() = Fields
}