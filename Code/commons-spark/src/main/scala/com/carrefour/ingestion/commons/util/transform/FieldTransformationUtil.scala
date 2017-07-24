package com.carrefour.ingestion.commons.util.transform

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object FieldTransformationUtil {
  val ArgsSep = "\\|\\|"

  val TableNameField = "tablename"
  val FieldNameField = "fieldname"
  val TransformationClassField = "transfclass"
  val TransformationArgsField = "transfargs"

  /**
    * Loads the transformations table and build a map with all transformations that can be used with {@link FieldInfo#buildFieldsInfo}.
    * If no table is given, returns an empty map of tranformations.
    */
  def loadTransformations(transformationsTable: String)(implicit sqlContext: SQLContext): Map[String, Map[String, TransformationInfo]] = {
    if (transformationsTable == null || transformationsTable.isEmpty)
      Map.empty
    else
      sqlContext.table(transformationsTable).rdd.groupBy(_.getAs[String](TableNameField)).map {
        case (table, rows: Iterable[Row]) =>
          table -> rows.map(r =>
            r.getAs[String](FieldNameField) -> TransformationInfo(r.getAs[String](TransformationClassField), r.getAs[String](TransformationArgsField).split(ArgsSep).toSeq)).toMap
      }.collect.toMap
  }
}

case class TransformationInfo(clazz: String, args: Seq[String])