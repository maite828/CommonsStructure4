package com.carrefour.ingestion.commons.cajas.raw.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class FinTicketBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_fin_ticket"

  val MovNField = "movn"
  val HoraField = "hora"
  val TimeVtaField = "timevta"
  val TimePagField = "timepag"
  val TimeEspField = "timeesp"
  val SortSNField = "sortsn"
  val Puntos0Field = "puntos0"
  val Puntos1Field = "puntos1"
  val Puntos2Field = "puntos2"
  val Puntos3Field = "puntos3"
  val Puntos4Field = "puntos4"
  val ArtDtnNField = "artdtnn"
  val ArtCodDtnNField = "artcoddtnn"
  val ArtInexNField = "artinexnfield"
  val LinAnuNField = "linanun"
  val BrutIField = "bruti"
  val LinAnuIField = "linanui"
  val TkAnuSNField = "tkanusn"
  val LlMaxField = "llmax"
  val StotCmpSNField = "stotcmpsn"
  val AnuArtSNField = "anuartsn"
  val CodAutField = "codaut"
  val MotivoAnlField = "motivoanl"
  val CodVddorField = "codvddor"
  val UsrVddorField = "usrvddor"
  val UniJumpField = "unijump"
  val TiempoJumpField = "tiempojump"

  private[this] val Fields = Seq[String](
    MovNField,
    HoraField,
    TimeVtaField,
    TimePagField,
    TimeEspField,
    SortSNField,
    Puntos0Field,
    Puntos1Field,
    Puntos2Field,
    Puntos3Field,
    Puntos4Field,
    ArtDtnNField,
    ArtCodDtnNField,
    ArtInexNField,
    LinAnuNField,
    BrutIField,
    LinAnuIField,
    TkAnuSNField,
    LlMaxField,
    StotCmpSNField,
    AnuArtSNField,
    CodAutField,
    MotivoAnlField,
    CodVddorField,
    UsrVddorField,
    UniJumpField,
    TiempoJumpField)

  override def fieldNames() = Fields
}