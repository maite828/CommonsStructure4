package com.carrefour.ingestion.commons.util.transform

import org.apache.spark.sql.types.{DataType, StringType}

import scala.reflect.runtime.{universe => ru}

/**
 * Receives a String value and produces a Object according to the transformation
 */
abstract class FieldTransformation extends Serializable {

  //  /**
  //   * Builds an object transforming the given string value
  //   */
  //  def transform(field: String): Any

  /**
   * Builds an object transforming the string value with the given parameters. Default implementation uses {@link #transform(String)}
   */
  def transform(field: String, args: String*): Any //= transform(field)

  def isNullOrEmpty(field: String): Boolean = field == null || field.isEmpty() || field.trim().equalsIgnoreCase("null")

  //  /**
  //   * Output type of the transformation
  //   */
  //  def outputType: DataType

  /**
   * Output type of the transformation, when parametrization is needed (i.e. decimal). Default implementation uses {@link #outputType}
   */
  def outputType(args: String*): DataType //= outputType
}


object FieldTransformation {
  /**
   * Retrieves the transformation object for the class
   * @param transformationClass a subclass of {@link #FieldTransformation}
   */
  def apply(transformationClass: String): FieldTransformation = {
    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    runtimeMirror.reflectModule(runtimeMirror.staticModule(transformationClass)).instance.asInstanceOf[FieldTransformation]
  }
}

/**
 * Returns the value unchanged, as String.
 */
object NopTransformation extends FieldTransformation {
  override def transform(field: String, args: String*) = field
  override def outputType(args: String*) = StringType
}