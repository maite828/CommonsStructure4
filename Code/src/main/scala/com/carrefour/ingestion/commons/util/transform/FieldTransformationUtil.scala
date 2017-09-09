package com.carrefour.ingestion.commons.util.transform

import java.sql.Timestamp

import com.carrefour.ingestion.commons.context.{FileSystemContext, SparkSessionContext}
import com.carrefour.ingestion.commons.controller.IngestionMetadata
import com.carrefour.ingestion.commons.context.impl.{FileSystemContextImpl, SparkSessionContextImpl}
import com.carrefour.ingestion.commons.exception.RowFormatException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object FieldTransformationUtil {
  val fs: FileSystemContext = FileSystemContextImpl
  val spark: SparkSessionContext = SparkSessionContextImpl


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
  def loadTransformations(transformationsTable: String): Map[String, Map[String, TransformationInfo]] = {
    if (transformationsTable == null || transformationsTable.isEmpty)
      Map.empty
    else
      spark.getSparkSession().table(transformationsTable).rdd.groupBy(_.getAs[String](TableNameField)).map {
        case (table, rows: Iterable[Row]) =>
          table -> rows.map(r =>
            r.getAs[String](FieldNameField) -> TransformationInfo(r.getAs[String](TransformationClassField), r.getAs[String](TransformationArgsField).split(ArgsSep).toSeq)).toMap
      }.collect.toMap
  }

  def applySchema(fields: Seq[Any], structType: StructType, settings: IngestionMetadata): Seq[Any] = {

    if (fields.size != structType.size) {
      throw new RowFormatException(
        s"""Error when applying type conversion to data. Number of input fields does not match the structure loaded from the table.
           |Schema loaded: $structType
           |Fields received: $fields
        """.stripMargin)
    } else {
      val f = fields zip structType.fields
      val converted = f.map(x => {
        val cleaned = x._1 match {
          case f: String => cleanField(f)(settings).getOrElse("")
          case other => other

        }

        (cleaned, x._2.dataType) match {
          case (field: String, DoubleType) =>
            if (field.isEmpty) null else field.toDouble
          case (field: String, IntegerType) =>
            if (field.isEmpty) null else field.toInt
          case (field: String, LongType) =>
            if (field.isEmpty) null else field.toLong
          case (field: String, ShortType) =>
            if (field.isEmpty) null else field.toShort
          case (field: String, BooleanType) =>
            if (field.isEmpty) null else field.toBoolean
          case (field: String, DecimalType()) =>
            if (field.isEmpty) {
              val dec: java.math.BigDecimal = null
              dec
            } else new java.math.BigDecimal(field)
          case (field: String, TimestampType) =>
            if (field.isEmpty) null else {
              val format = new java.text.SimpleDateFormat(settings.timestampFormat)
              new Timestamp(format.parse(field).getTime)
            }
          case (field: String, _) =>
            field
          case _ =>
            x._1
        }

      }).toArray
      converted
    }
  }

  def cleanField(field: String)(settings: IngestionMetadata): Option[String] = {
    val cleaned: String = field.replace(settings.encloseChar, "").trim
    if (cleaned.isEmpty || cleaned.equalsIgnoreCase("null")) None
    else Some(cleaned)
  }
}


case class TransformationInfo(clazz: String, args: Seq[String])