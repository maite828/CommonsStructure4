package com.carrefour.ingestion.commons.service.transform

import org.apache.spark.sql.types.ByteType
import org.slf4j.{Logger, LoggerFactory}

/**
  * Outputs the value as a tinyint.
  */
object TinyintTransformation extends FieldTransformation {

  protected val Logger: Logger = LoggerFactory.getLogger(getClass)

  override def transform(field: String, args: String*): Any = {
    if (isNullOrEmpty(field)) null
    else
      try
        field.trim.toByte
      catch {
        case e: NumberFormatException =>
          Logger.warn(s"Error parsing tinyint $field")
          throw e

      }
  }

  override def outputType(args: String*) = ByteType
}
