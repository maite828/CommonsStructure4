package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class BonificacionClubCarrefourBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_bonificacion_clubcarrefour"

  val TarjtaField = "tarjta"
  val SaldoField = "saldo"
  val BonfField = "bonf"
  val Neto1Field = "neto1"
  val Can1Field = "can1"
  val Bon1Field = "bon1"
  val Neto2Field = "neto2"
  val Can2Field = "can2"
  val Bon2Field = "bon2"
  val Neto3Field = "neto3"
  val Can3Field = "can3"
  val Bon3Field = "bon3"
  val Neto4Field = "neto4"
  val Can4Field = "can4"
  val Bon4Field = "bon4"
  val Neto5Field = "neto5"
  val Can5Field = "can5"
  val Bon5Field = "bon5"
  val Neto6Field = "neto6"
  val Can6Field = "can6"
  val Bon6Field = "bon6"
  val Neto7Field = "neto7"
  val Can7Field = "can7"
  val Bon7Field = "bon7"
  val Neto8Field = "neto8"
  val Can8Field = "can8"
  val Bon8Field = "bon8"
  val Neto9Field = "neto9"
  val Can9Field = "can9"
  val Bon9Field = "bon9"
  val NetoPASSField = "netopass"
  val CanPASSField = "canpass"
  val BnfPASSField = "bnfpass"
  val SegmentoField = "segmento"

  private[this] val Fields = Seq[String](
    TarjtaField,
    SaldoField,
    BonfField,
    Neto1Field,
    Can1Field,
    Bon1Field,
    Neto2Field,
    Can2Field,
    Bon2Field,
    Neto3Field,
    Can3Field,
    Bon3Field,
    Neto4Field,
    Can4Field,
    Bon4Field,
    Neto5Field,
    Can5Field,
    Bon5Field,
    Neto6Field,
    Can6Field,
    Bon6Field,
    Neto7Field,
    Can7Field,
    Bon7Field,
    Neto8Field,
    Can8Field,
    Bon8Field,
    Neto9Field,
    Can9Field,
    Bon9Field,
    NetoPASSField,
    CanPASSField,
    BnfPASSField,
    SegmentoField)

  override def fieldNames() = Fields
}