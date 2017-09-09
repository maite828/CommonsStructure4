package com.carrefour.ingestion.commons.exception.logging

import java.io.Serializable

case class CommonsException(message: String = None.orNull, cause: Throwable = None.orNull) extends Exception(message, cause) with Serializable {
}

