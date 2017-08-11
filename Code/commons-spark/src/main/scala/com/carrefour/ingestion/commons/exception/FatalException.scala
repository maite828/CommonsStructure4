package com.carrefour.ingestion.commons.exception

/**
  * Custom fatal exception
  *
  * @param message - Exception message
  */
class FatalException(message: String) extends Exception(message)
