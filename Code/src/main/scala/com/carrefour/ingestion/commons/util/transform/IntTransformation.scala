package com.carrefour.ingestion.commons.service.transform

import org.apache.spark.sql.types.IntegerType
import org.slf4j.{Logger, LoggerFactory}

/**
 * Outputs the value as an Integer.
 */
object IntTransformation extends FieldTransformation {

  protected val Logger:Logger = LoggerFactory.getLogger(getClass)

  override def transform(field: String, args: String*): Integer = {
    if (isNullOrEmpty(field)) null
    else try field.trim.toInt
    catch {
      case _: NumberFormatException =>
        Logger.warn(s"Error parsing int $field")
        null
    }
  }
  override def outputType(args: String*) = IntegerType
}
