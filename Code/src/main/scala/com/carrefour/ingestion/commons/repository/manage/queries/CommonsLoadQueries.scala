package com.carrefour.ingestion.commons.repository.manage.queries

trait CommonsLoadQueries {
  /**
    *
    * @param a busines_unit
    * @param b table
    * @return query
    */
  abstract def sQueryLoadMetadata_Entity(a: String, b: String): String = {
      if (b == "") {
        s"select bu.businessunit_id, bu.businessunit_name, " +
          s"ta.table_id, ta.table_name, ta.schema_name, ta.storeformat, ta.compressiontype, ta.transformationstable, ta.transformationsschema, " +
          s"fi.file_id, fi.file_name, fi.parentpath, fi.filemask, fi.datepart_file, " +
          s"ff.fileformat_id, ff.fileformat_type, ff.fileformat_format, ff.fielddelimiter, ff.linedelimiter, ff.endswithdelimiter, " +
          s"ff.headerlines, ff.datedefaultformat, ff.enclosechar, ff.escapechar " +
          s"from config.ingestion_businessunit bu " +
          s"left join config.ingestion_table ta on ta.businessunit_id = bu.businessunit_id " +
          s"left join config.ingestion_rel_table_file re on re.table_id = ta.table_id " +
          s"left join config.ingestion_file fi on fi.file_id = re.file_id " +
          s"left join config.ingestion_fileformat ff on ff.fileformat_id = fi.fileformat_id " +
          s"where bu.businessunit_name = '$a' "
      } else {
        s"select bu.businessunit_id, bu.businessunit_name, " +
          s"ta.table_id, ta.table_name, ta.schema_name, ta.storeformat, ta.compressiontype, ta.transformationstable, ta.transformationsschema, " +
          s"fi.file_id, fi.file_name, fi.parentpath, fi.filemask, fi.datepart_file, " +
          s"ff.fileformat_id, ff.fileformat_type, ff.fileformat_format, ff.fielddelimiter, ff.linedelimiter, ff.endswithdelimiter, " +
          s"ff.headerlines, ff.datedefaultformat, ff.enclosechar, ff.escapechar " +
          s"from config.ingestion_businessunit bu " +
          s"left join config.ingestion_table ta on ta.businessunit_id = bu.businessunit_id " +
          s"left join config.ingestion_rel_table_file re on re.table_id = ta.table_id " +
          s"left join config.ingestion_file fi on fi.file_id = re.file_id " +
          s"left join config.ingestion_fileformat ff on ff.fileformat_id = fi.fileformat_id " +
          s"where bu.businessunit_name = '$a' " +
          s"and ta.table_name = '$b' "
      }

  }
}
