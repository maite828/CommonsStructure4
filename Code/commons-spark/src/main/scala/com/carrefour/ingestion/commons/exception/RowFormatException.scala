package com.carrefour.ingestion.commons.exception

/**
  * Fatal exception that will be thrown
  *
  * @param message - mensaje de la excepción
  */
class RowFormatException(message: String) extends Exception(message)
