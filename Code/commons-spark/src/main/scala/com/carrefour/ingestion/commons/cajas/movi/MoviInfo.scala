package com.carrefour.ingestion.commons.cajas.movi

import java.util.regex.Pattern
import org.slf4j.LoggerFactory

case class MoviInfo(fecha: String, tienda: String, pos: String)

object MoviInfo {

  val Logger = LoggerFactory.getLogger(getClass)

  val MoviFilenamePattern = Pattern.compile("(?<fecha>\\d{8})_(?<tienda>[\\d\\w]{4})_movi\\.(?<pos>[\\d\\w]{3})")

  def apply(filename: String) = {
    val matcher = MoviFilenamePattern.matcher(filename)
    if (!matcher.matches()) {
      Logger.error(s"Unrecongnized movi file name: $filename")
      throw new IllegalArgumentException(s"Unrecongnized movi file name: $filename")
    }
    new MoviInfo(
      fecha = matcher.group("fecha"),
      tienda = matcher.group("tienda"),
      pos = matcher.group("pos"))
  }
}