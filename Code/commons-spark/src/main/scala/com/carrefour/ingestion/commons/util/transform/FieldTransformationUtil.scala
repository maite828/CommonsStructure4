package com.carrefour.ingestion.commons.util.transform

import java.sql.Timestamp

import com.carrefour.ingestion.commons.exception.RowFormatException
import com.carrefour.ingestion.commons.loader.IngestionMetadata
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object FieldTransformationUtil {
  val ArgsSep = "\\|\\|"

  val TableNameField = "tablename"
  val FieldNameField = "fieldname"
  val TransformationClassField = "transfclass"
  val TransformationArgsField = "transfargs"

  /**
    * Loads the transformations table and build a map with all transformations that can be used with 
    * `FieldInfo#buildFieldsInfo`.
    * If no table is given, returns an empty map of transformations.
    */
  def loadTransformations(transformationsTable: String)(implicit sparkSession: SparkSession): Map[String, Map[String, TransformationInfo]] = {
    if (transformationsTable == null || transformationsTable.isEmpty)
      Map.empty
    else
      sparkSession.table(transformationsTable).rdd.groupBy(_.getAs[String](TableNameField)).map {
        case (table, rows: Iterable[Row]) =>
          table -> rows.map(r =>
            r.getAs[String](FieldNameField) -> TransformationInfo(r.getAs[String](TransformationClassField), r.getAs[String](TransformationArgsField).split(ArgsSep).toSeq)).toMap
      }.collect.toMap
  }

  def applySchema(fields: Seq[Any], structType: StructType, settings: IngestionMetadata): Seq[Any] = {

    if (fields.size != structType.size) {
      throw new RowFormatException("Error when applying type conversion to data. Number of input fields does not match the structure loaded from the table.")
    } else {
    val f = fields zip structType.fields
    val converted = f.map(x => {
        
        (x._1, x._2.dataType) match {
          case (field: String, DoubleType) =>
            if (field.trim.isEmpty || field.trim.equalsIgnoreCase("null")) null else field.trim.toDouble
          case (field: String, IntegerType) =>
            if (field.trim.isEmpty || field.trim.equalsIgnoreCase("null")) null else field.trim.toInt
          case (field: String, LongType) =>
            if (field.trim.isEmpty || field.trim.equalsIgnoreCase("null")) null else field.trim.toLong
          case (field: String, ShortType) =>
            if (field.trim.isEmpty || field.trim.equalsIgnoreCase("null")) null else field.trim.toShort
          case (field: String, BooleanType) =>
            if (field.trim.isEmpty || field.trim.equalsIgnoreCase("null")) null else field.trim.toBoolean
          case (field: String, DecimalType()) =>
            if (field.trim.isEmpty || field.trim.equalsIgnoreCase("null")) null else new java.math.BigDecimal(field.trim)
          case (field: String, TimestampType) =>
            if (field.trim.isEmpty || field.trim.equalsIgnoreCase("null")) null else {
              val format = new java.text.SimpleDateFormat(settings.timestampFormat)
              new Timestamp(format.parse(field.trim).getTime)
            }
          case (field:String, _) =>
            field.trim
          case _ =>
            x._1
        }

      }).toArray
      converted
    }
  }
}

case class TransformationInfo(clazz: String, args: Seq[String])