CREATE DATABASE config
LOCATION '/user/silkroad/config';

DROP TABLE IF EXISTS config.ingestion_table;
CREATE EXTERNAL TABLE config.ingestion_table (
  table_id				int 	COMMENT 'Table identifier',
  table_name			string 	COMMENT 'Table name',
  table_desc			string 	COMMENT 'Table description',
  schema_name			string 	COMMENT 'Schema name where table is located',
  storeformat			string 	COMMENT 'Table store format (text, avro, parquet...)',
  compressiontype		string 	COMMENT 'Table default compression type (uncompressed, snappy, lzo...)',
  businessunit_id		int 	COMMENT 'Business unit identifier',
  businessunit_name		string	COMMENT 'Business unit name',
  transformationstable	string 	COMMENT 'Transformation table name',
  transformationsschema	string 	COMMENT 'Transformation table schema'
)
COMMENT 'Table properties'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\;',
  'serialization.format'='\;')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

DROP TABLE IF EXISTS config.ingestion_file;
CREATE EXTERNAL TABLE config.ingestion_file (
  file_id 		int 	COMMENT 'File identifier',
  file_name		string 	COMMENT 'File name',
  file_desc		string 	COMMENT 'File description',
  parentpath	string 	COMMENT 'Parent path where de file is located',
  filemask		string	COMMENT	'RegExp with file name mask',
  fileformat_id	int 	COMMENT 'File format identifier'
)
COMMENT 'File properties'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\;',
  'serialization.format'='\;')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

DROP TABLE IF EXISTS config.ingestion_businessunit;
CREATE EXTERNAL TABLE config.ingestion_businessunit (
  businessunit_id	int 	COMMENT 'Business unit identifier',
  businessunit_name	string	COMMENT 'Business unit name',
  businessunit_desc	string	COMMENT 'Business unit description'
)
COMMENT 'Business units properties'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\;',
  'serialization.format'='\;')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

DROP TABLE IF EXISTS config.ingestion_fileformat;
CREATE EXTERNAL TABLE config.ingestion_fileformat (
  fileformat_id		int 	COMMENT 'File format identifier',
  fileformat_name   string 	COMMENT 'File format name',
  fileformat_desc   string 	COMMENT 'File format description',
  fileformat_type	string	COMMENT	'File format type',
  fileformat_format string  COMMENT 'File format (TEXT, GZ, ZIP...)',
  fielddelimiter    string 	COMMENT 'Field delimiter',
  linedelimiter     string	COMMENT	'Line delimiter',
  endswithdelimiter	boolean COMMENT 'TRUE if each registry ends with delimiter',
  headerlines		int 	COMMENT 'Number of lines os header',
  datedefaultformat string 	COMMENT 'Default date format',
  linepattern       string 	COMMENT 'RegExp with the line pattern. Useful with fixed width files',
  enclosechar       string 	COMMENT 'Char used to enclose fields',
  escapechar        string	COMMENT	'Char used as escape char'
)
COMMENT 'File format properties'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\;',
  'serialization.format'='\;')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

DROP TABLE IF EXISTS config.ingestion_rel_table_file;
CREATE EXTERNAL TABLE config.ingestion_rel_table_file (
  table_id 	int 	COMMENT 'Table identifier',
  file_id  	int 	COMMENT 'File identifier',
  condition	string	COMMENT	'Conditions needed to filter file'
)
COMMENT 'Relations between tables and files'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\;',
  'serialization.format'='\;')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
