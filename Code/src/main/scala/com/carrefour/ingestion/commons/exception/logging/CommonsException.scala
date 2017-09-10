package com.carrefour.ingestion.commons.exception.logging

/**
  *
  * @param message
  * @param cause
  */
case class CommonsException(message: String = None.orNull, cause: Throwable = None.orNull) extends Exception(message, cause) with Serializable {
}

