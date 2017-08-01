package com.carrefour.ingestion.commons.util.transform

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

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
  def applySchema(fields: Tuple2[Seq[String], Int], structType: StructType): Tuple2[Seq[Any], Int] = {

    var i = 0
    val result = new Array[Any](structType.size)
    if (fields._1.size != structType.size){
      (fields._1, -1)
    }else{
      try{
        for(structField <- structType){
          structField.dataType match{
            case DoubleType =>
              result(i) = if (fields._1(i).toString.isEmpty || fields._1(i).toString.equalsIgnoreCase("null")) null else fields._1(i).toDouble
            case IntegerType =>
              result(i) = if (fields._1(i).toString.isEmpty || fields._1(i).toString.equalsIgnoreCase("null")) null else fields._1(i).toInt
            case LongType =>
              result(i) = if (fields._1(i).toString.isEmpty || fields._1(i).toString.equalsIgnoreCase("null")) null else fields._1(i).toLong
            case ShortType =>
              result(i) = if (fields._1(i).toString.isEmpty || fields._1(i).toString.equalsIgnoreCase("null")) null else fields._1(i).toShort
            case BooleanType =>
              result(i) = if (fields._1(i).toString.isEmpty || fields._1(i).toString.equalsIgnoreCase("null")) null else fields._1(i).toBoolean
            case DecimalType() =>
              result(i) = if (fields._1(i).toString.isEmpty || fields._1(i).toString.equalsIgnoreCase("null")) null else new java.math.BigDecimal(fields._1(i))
            case _ =>
              result(i) = fields._1(i)
          }
          i = i+1;
        }
      }catch{
        case t: Throwable =>
          (fields._1, -2)
      }
    }
    (result, fields._2)
  }
}

case class TransformationInfo(clazz: String, args: Seq[String])