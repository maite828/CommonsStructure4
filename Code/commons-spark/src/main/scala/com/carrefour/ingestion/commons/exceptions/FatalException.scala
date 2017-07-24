package com.carrefour.ingestion.commons.exceptions

/**
  * Excepción fatal
  *
  * @param message - mensaje de la excepción
  */
class FatalException(message: String) extends Exception(message)
