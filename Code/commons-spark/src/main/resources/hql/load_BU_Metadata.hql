select bu.businessunit_id, bu.businessunit_name,
       ta.table_id, ta.table_name, ta.schema_name, ta.storeformat, ta.compressiontype, ta.transformationstable, ta.transformationsschema,
       fi.file_id, fi.file_name, fi.parentpath, fi.filemask,
       ff.fileformat_id, ff.fileformat_type, ff.fileformat_format, ff.fielddelimiter, ff.linedelimiter, ff.endswithdelimiter,
       ff.headerlines, ff.datedefaultformat, ff.enclosechar, ff.escapechar
from config.ingestion_businessunit bu
left join config.ingestion_table ta on ta.businessunit_id = bu.businessunit_id
left join config.ingestion_rel_table_file re on re.table_id = ta.table_id
left join config.ingestion_file fi on fi.file_id = re.file_id
left join config.ingestion_fileformat ff on ff.fileformat_id = fi.fileformat_id
 where ta.active = 1
   and bu.businessunit_name = '$1'