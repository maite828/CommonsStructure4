package com.carrefour.ingestion.commons.util.transform

import org.apache.spark.sql.types.ByteType
import org.slf4j.LoggerFactory

/**
 * Outputs the value as a tinyint.
 */
object TinyintTransformation extends FieldTransformation {

  protected val Logger = LoggerFactory.getLogger(getClass)

  override def transform(field: String, args: String*) = if (isNullOrEmpty(field)) null else try field.trim.toByte
  catch {
    case e: NumberFormatException => {
      Logger.warn(s"Error parsing tinyint $field")
      null
    }
  }
  override def outputType(args: String*) = ByteType
}
