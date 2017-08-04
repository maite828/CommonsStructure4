package com.carrefour.ingestion.commons.cajas.movi.builder

import org.apache.spark.sql.SQLContext

object MoviRowBuilderUtil {
  val FieldNameField = "fieldname"
  val FieldOutputPositionField = "fieldposition"
  val RecordTypeField = "recordtype"

  /**
   * Loads the fields configuration table and build a sequence of {@link MoviFieldConf}
   */
  def loadFieldsConf(fieldsConfTable: String)(implicit sqlContext: SQLContext): Seq[MoviFieldConf] = {
    import sqlContext.sparkSession.implicits._
    if (fieldsConfTable == null || fieldsConfTable.isEmpty)
      Seq.empty
    else
      sqlContext.table(fieldsConfTable).
        map(row => MoviFieldConf(fieldName = row.getAs(FieldNameField),
          fieldOutputPosition = row.getAs(FieldOutputPositionField),
          recordType = row.getAs(RecordTypeField))).
        collect()
  }
}

case class MoviFieldConf(fieldName: String, fieldOutputPosition: Int, recordType: String)
