package com.carrefour.ingest.commons.bean

/**
  * Generic trait. Every type of file must extend this trait, in order for it to be used.
  */
trait FileType {

}

/**
  * Type of file whose main characteristics are the following:
  *  - Every line with data has the same structure.
  *  - Every line with data ends with the same char or string (lineDelimiter)
  *  - The fields within a line are separated by the same char or string (fieldDelimiter)
  *
  * @param fileFormat The file format
  * @param numPartitions  Number of partitions that will be used for reading the file
  * @param fieldDelimiter String delimiter between fields within a line
  * @param lineDelimiter  String delimiter between lines
  * @param endsWithDelimiter  Whether the data lines end with a `fieldDelimiter` or not
  * @param headerLines  Number of lines that contain the header
  * @param dateDefaultFormat  Default format for the Date type
  * @param encloseChar  String that defines the enclosing character
  * @param escapeChar String that defines the escape character
  */
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

/**
  * Class that will be used when working with an undefined type of file
  */
case class NoFileType() extends FileType
