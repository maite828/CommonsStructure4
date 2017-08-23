package com.carrefour.ingestion.commons.cajas.ticket

import java.util.regex.Pattern

import org.slf4j.{Logger, LoggerFactory}

/**
  * Class that contains metadata or common information to all the tickets, despite its type
  *
  * @param fecha  Generation date of the ticket
  * @param tienda Store identifier where the ticket was generated
  * @param pos  Point-of-Sale Terminal where the ticket was generated
  * @param num  Ticket number/identifier
  * @param tipo Ticket type/category
  * @param year Year in which the ticket was generated
  * @param month  Month in which the ticket was generated
  * @param day  Day in which the ticket was generated
  */
case class TicketInfo(fecha: String, tienda: String, pos: String, num: String, tipo: String, year: Integer, month: Integer, day: Integer)

object TicketInfo {

  val Logger: Logger = LoggerFactory.getLogger(getClass)

  val TicketFilenamePattern: Pattern = Pattern.compile("(?<fecha>\\d{8})_(?<tienda>[\\d\\w]{4})_t(?<num>\\d{6})(?<tipo>\\w)\\.(?<pos>[\\d\\w]{3})")

  /**
    * This method will build a new TicketInfo based on the ticket file to be ingested.
    * It does so by matching the file name against a pattern and parsing each token individually
    *
    * @param filename Name of the file. All the information mentioned above will be inferred by this name.
    * @return A new TicketInfo with its correspondent information.
    * @throws IllegalArgumentException When the ticket file name doesn't adjust to a valid structure
    */
  def apply(filename: String): TicketInfo = {
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