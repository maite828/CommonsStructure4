package com.carrefour.ingestion.commons.util.transform

import org.apache.spark.sql.types.StringType

/**
 * Trims the input String field.
 */
object TrimTransformation extends FieldTransformation {

  override def transform(field: String, args: String*) = if (field == null) null else field.trim()

  override def outputType(args: String*) = StringType
}