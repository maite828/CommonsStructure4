package com.carrefour.ingest.commons.util.transform

import org.apache.spark.sql.types.StructField

/**
 * Encapsulates field information (name and optionally the transformation to apply).
 */
case class FieldInfo(field: String, transformation: FieldTransformation, transformationArgs: Seq[String] = Seq.empty) {
  lazy val schema = StructField(field, transformation.outputType(transformationArgs: _*))
}

object FieldInfo {

  /**
   * Builds the sequence of field information for the given fields in a table, considering the specified transformations.
   * <p>
   * The transformation for a field will be the first of the following that is defined in the input transformations map:
   * <ul>
   * <li>The transformation for the table-field</li>
   * <li>The transformation for the field (no table specified)</li>
   * <li>The default transformation (no table nor field specified)</li>
   * </ul>
   * If none of the above is defined, default transformation is `NopTransformation`, which produces as ouput the same String given as input.
   *
   * @param tableName Name of the table
   * @param fieldNames Ordered list of field names
   * @param allTransformations Map of transformations that can be applied. Only transformations for this table or fields would be used.
   */
  def buildFieldsInfo(tableName: String, fieldNames: Seq[String], allTransformations: Map[String, Map[String, TransformationInfo]] = Map.empty): Seq[FieldInfo] = {
    val defaultTransformation: FieldTransformation = allTransformations.getOrElse("", Map.empty).get("") match {
      case None => NopTransformation
      case Some(t) => FieldTransformation(t.clazz)
    } //TODO args in default transformation
    val genericTransformations = allTransformations.getOrElse("", Map.empty)
    val tableTransformations = allTransformations.getOrElse(tableName, Map.empty) //FIXME WARNING empty transformations for table
    fieldNames.map { f =>
      (tableTransformations.get(f) match {
        case None => genericTransformations.get(f)
        case t => t
      }).map { case TransformationInfo(clazz, args) => FieldInfo(f, FieldTransformation(clazz), args) }.
        getOrElse(FieldInfo(f, defaultTransformation))
    }
  }
}