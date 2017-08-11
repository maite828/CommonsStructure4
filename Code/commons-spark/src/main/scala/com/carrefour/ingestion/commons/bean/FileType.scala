package com.carrefour.ingestion.commons.bean

trait FileType {

}

case class DelimitedFileType(
                              fileFormat: FileFormats.FileFormat,
                              numPartitions: Int,
                              fieldDelimiter: String,
                              lineDelimiter: String,
                              endsWithDelimiter: Boolean,
                              headerLines: Int,
                              dateDefaultFormat: String,
                              encloseChar: String,
                              escapeChar: String) extends FileType

case class NoFileType() extends FileType
