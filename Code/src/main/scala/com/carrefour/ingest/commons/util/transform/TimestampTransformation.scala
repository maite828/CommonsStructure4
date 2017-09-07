package com.carrefour.ingest.commons.util.transform

import java.sql.Timestamp
import java.text.ParseException

import org.apache.spark.sql.types.TimestampType
import org.slf4j.{Logger, LoggerFactory}

/**
 * Formats the date string.
 */
object TimestampTransformation extends FieldTransformation {

  private val Logger: Logger = LoggerFactory.getLogger(getClass)

  override def transform(field: String, args: String*): Timestamp = {
    if (isNullOrEmpty(field))
      null
    else {
      val inputFormat = new java.text.SimpleDateFormat(args(0))
      try {
        new Timestamp(inputFormat.parse(field).getTime)
      } catch {
        case _: ParseException =>
          Logger.warn(s"Error parsing timestamp $field")
          null

      }
    }
  }

  override def outputType(args: String*) = TimestampType
}