package com.carrefour.ingestion.commons.service.transform

import org.apache.spark.sql.types.DecimalType
import org.slf4j.{Logger, LoggerFactory}

/**
 * Superclass for decimal transformations
 */
abstract class DecimalTransformation extends FieldTransformation {

  protected val Logger:Logger = LoggerFactory.getLogger(getClass)

  private[this] def getPrecision(args: Seq[String]): Int = args.head.toInt
  private[this] def getScale(args: Seq[String]): Int = args(1).toInt

  override final def outputType(args: String*) = DecimalType(getPrecision(args), getScale(args))
}

/**
 * Builds a decimal field with the given precision and scale.
 */
object DecimalTransformation extends DecimalTransformation {
  override def transform(field: String, args: String*): BigDecimal = {
    if (isNullOrEmpty(field)) null
    else try BigDecimal(field.trim)
    catch {
      case _: NumberFormatException =>
        Logger.warn(s"Error parsing decimal $field")
        null

    }
  }
}

/**
  * Builds a decimal field with the given precision and scale, first dividing the input number by 10 to the number given as third argument.
  */
object DecimalFromIntTransformation extends DecimalTransformation {

  /**
    * @param args Arguments: precision, scale, decimal positions to create in the input number (divide it by 10 to n)
    */
  override def transform(field: String, args: String*): BigDecimal = {
    if (isNullOrEmpty(field.trim)) null
    else
      try
        BigDecimal(field.trim.toDouble / scala.math.pow(10, args(2).toInt))
      catch {
        case _: NumberFormatException =>
          Logger.warn(s"Error parsing decimal $field")
          null

    }
  }
  //TODO toDouble correct??
}

/**
  * Builds a decimal field with the given precision and scale, using the char in the third argument as the input decimal separator.
  */
object DecimalParseTransformation extends DecimalTransformation {
  override def transform(field: String, args: String*): BigDecimal = {
    if (isNullOrEmpty(field.trim)) null
    else
      try
        BigDecimal(field.trim.replace(args(2)(0), '.'))
      catch {
      case _: NumberFormatException =>
        Logger.warn(s"Error parsing decimal $field")
        null
    }
  }
}
