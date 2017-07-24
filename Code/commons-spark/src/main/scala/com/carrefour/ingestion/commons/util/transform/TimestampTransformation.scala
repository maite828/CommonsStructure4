package com.carrefour.ingestion.commons.util.transform

import java.text.ParseException

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.unix_timestamp
import java.sql.Timestamp
import org.slf4j.LoggerFactory

/**
 * Formats the date string.
 */
object TimestampTransformation extends FieldTransformation {

  private val Logger = LoggerFactory.getLogger(getClass)

  override def transform(field: String, args: String*) = {
    if (isNullOrEmpty(field))
      null
    else {
      val inputFormat = new java.text.SimpleDateFormat(args(0))
      val outputFormat = new java.text.SimpleDateFormat(args(1))
      try {
        new Timestamp(inputFormat.parse(field).getTime())
      } catch {
        case e: ParseException => {
          Logger.warn(s"Error parsing timestamp $field")
          null
        }
      }
    }
  }

  override def outputType(args: String*) = TimestampType
}