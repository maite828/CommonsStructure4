package com.carrefour.ingestion.commons.cajas.movi.builder

import com.carrefour.ingestion.commons.cajas.movi.MoviInfo
import com.carrefour.ingestion.commons.util.transform.{FieldInfo, TransformationInfo}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class MoviRowBuilder(fieldsConf: Seq[MoviFieldConf], allTransformations: Map[String, Map[String, TransformationInfo]]) extends Serializable {

  @transient
  private lazy val Logger = LoggerFactory.getLogger(classOf[MoviRowBuilder])

  private[this] lazy val sortedFieldsConf = fieldsConf.sortWith(_.fieldOutputPosition < _.fieldOutputPosition)

  /**
   * Transformations to be applied to each fields, preserving field order
   */
  private[this] lazy val fieldsInfo: Seq[FieldInfo] = FieldInfo.buildFieldsInfo(MoviRowBuilder.TableName, sortedFieldsConf.map(_.fieldName), allTransformations)

  /**
   * Defines first and last position for each record type
   */
  private[this] lazy val fieldPositions: Map[String, Tuple2[Int, Int]] = sortedFieldsConf.
    groupBy(_.recordType).
    mapValues(confs => (confs.head.fieldOutputPosition, confs.last.fieldOutputPosition))

  def getSchema: StructType = StructType(fieldsInfo.map(_.schema))

  def buildRow(moviInfo: MoviInfo, commonFields: Seq[String], specificFields: Seq[String]): Try[Row] = {
    if (MoviRowBuilder.RecordTypePosition >= commonFields.length) {
      Logger.error(s"Malformed record: ${commonFields.mkString("|")} || ${specificFields.mkString("|")}")
      return Failure(new IllegalArgumentException(s"Malformed record: ${commonFields.mkString("|")} || ${specificFields.mkString("|")}"))
    }
    val recordType = commonFields(MoviRowBuilder.RecordTypePosition)
    val lastCommonFieldPos = fieldPositions.getOrElse("", (0, 0))._2 //FIXME error si no hay campos comunes
    val allSpecificFields = fieldPositions.get(recordType) match {
      case None =>
        Logger.info(s"Record type $recordType without additional info. Building row with ${sortedFieldsConf.last.fieldOutputPosition - lastCommonFieldPos} null fields.")
        Array.fill(sortedFieldsConf.last.fieldOutputPosition - lastCommonFieldPos)(null)
      case Some((beginSpecific, endSpecific)) =>
        //process specific fields and fill with nulls before and after
        Array.fill(beginSpecific - lastCommonFieldPos - 1)(null) ++
          transform(specificFields.take(endSpecific - beginSpecific + 1), beginSpecific, endSpecific) ++
          //FIXME complete specific with nulls
          Array.fill(sortedFieldsConf.last.fieldOutputPosition - endSpecific)(null)
    }
    val allFields = transform(Seq(moviInfo.fecha, moviInfo.tienda) ++ commonFields.take(lastCommonFieldPos), 0, lastCommonFieldPos) ++ allSpecificFields
    //    MoviRowBuilder.Logger.debug(s"Built row with ${allFields.size}: ${allFields.mkString("||")}")
    //FIXME complete common with nulls
    Success(Row(allFields: _*))
  }

  private[this] def transform(raw: Seq[String], startPosition: Int, endPosition: Int) = {
    (raw zip fieldsInfo.slice(startPosition, endPosition + 1)).
      map {
        case (fieldValue, fieldInfo) =>
          fieldInfo.transformation.transform(fieldValue, fieldInfo.transformationArgs: _*)
      }
  }

}

object MoviRowBuilder {
  val Logger = LoggerFactory.getLogger(classOf[MoviRowBuilder])

  val TableName = "movi"
  val RecordTypePosition = 14
  val DatePartField = "date_part"
}
