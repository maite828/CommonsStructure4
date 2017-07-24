package com.carrefour.ingestion.commons.util.transform

import org.apache.spark.sql.types.IntegerType
import org.slf4j.LoggerFactory

/**
 * Outputs the value as a int.
 */
object IntTransformation extends FieldTransformation {

  protected val Logger = LoggerFactory.getLogger(getClass)

  override def transform(field: String, args: String*) = if (isNullOrEmpty(field)) null else try field.trim.toInt
  catch {
    case e: NumberFormatException => {
      Logger.warn(s"Error parsing int $field")
      null
    }
  }
  override def outputType(args: String*) = IntegerType
}
