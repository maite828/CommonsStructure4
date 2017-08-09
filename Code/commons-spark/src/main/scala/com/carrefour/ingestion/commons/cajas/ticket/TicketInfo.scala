package com.carrefour.ingestion.commons.cajas.ticket

import java.util.regex.Pattern
import org.slf4j.LoggerFactory

case class TicketInfo(fecha: String, tienda: String, pos: String, num: String, tipo: String, year: Integer, month: Integer, day: Integer)

object TicketInfo {

  val Logger = LoggerFactory.getLogger(getClass)

  val TicketFilenamePattern = Pattern.compile("(?<fecha>\\d{8})_(?<tienda>[\\d\\w]{4})_t(?<num>\\d{6})(?<tipo>\\w)\\.(?<pos>[\\d\\w]{3})")

  def apply(filename: String) = {
    val matcher = TicketFilenamePattern.matcher(filename)
    if (!matcher.matches()) {
      Logger.error(s"Unrecongnized ticket file name: $filename")
      throw new IllegalArgumentException(s"Unrecongnized ticket file name: $filename")
    }
    new TicketInfo(
      fecha = matcher.group("fecha"),
      tienda = matcher.group("tienda"),
      pos = matcher.group("pos"),
      num = matcher.group("num"),
      tipo = matcher.group("tipo"),
      year = Integer.valueOf(matcher.group("fecha").substring(0,4)),
      month = Integer.valueOf(matcher.group("fecha").substring(4,6)),
      day = Integer.valueOf(matcher.group("fecha").substring(6,8)))
  }
}