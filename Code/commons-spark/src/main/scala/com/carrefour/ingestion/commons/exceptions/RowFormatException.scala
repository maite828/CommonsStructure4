package com.carrefour.ingestion.commons.exceptions

/**
  * Excepción fatal
  *
  * @param message - mensaje de la excepción
  */
class RowFormatException(message: String) extends Exception(message)
