package com.carrefour.ingestion.commons.cajas.ticket.builder

import com.carrefour.ingestion.commons.util.transform.TransformationInfo

class DescuentoEmpleadosBuilder(fieldsInfo: Map[String, Map[String, TransformationInfo]]) extends TicketRowBuilder(fieldsInfo) {

  override def tableName: String = "t_descuento_empleados"

  val EmpDNIField = "empdni"
  val EmpNomField = "empnom"
  val BenDNIField = "bendni"
  val EmpCenField = "empcen"
  val EmpSecField = "empsec"
  val EmpSalField = "empsal"
  val EmpCsmField = "empcsm"
  val EmpSalProxField = "empsalprox"
  val DtoField = "dto"
  val MoField = "mo"
  val OpeBenSNField = "opebensn"

  private[this] val Fields = Seq[String](
    EmpDNIField,
    EmpNomField,
    BenDNIField,
    EmpCenField,
    EmpSecField,
    EmpSalField,
    EmpCsmField,
    EmpSalProxField,
    DtoField,
    MoField,
    OpeBenSNField)

  override def fieldNames() = Fields
}