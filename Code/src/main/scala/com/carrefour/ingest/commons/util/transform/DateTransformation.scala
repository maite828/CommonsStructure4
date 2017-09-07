package com.carrefour.ingest.commons.util.transform

import java.text.ParseException

import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}
/**
 * Formats the date string.
 */
object DateTransformation extends FieldTransformation {

  private val Logger: Logger = LoggerFactory.getLogger(getClass)

  override def transform(field: String, args: String*):String = {
    if (isNullOrEmpty(field))
      null
    else {
      val inputFormat = new java.text.SimpleDateFormat(args(0))
      val outputFormat = new java.text.SimpleDateFormat(args(1))
      try {
        outputFormat.format(inputFormat.parse(field))
      } catch {
        case _: ParseException =>
          Logger.warn(s"Error parsing date $field")
          null

      }
    }
  }

  override def outputType(args: String*) = StringType
}
