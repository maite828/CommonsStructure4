package com.carrefour.ingestion.commons.bean

/**
  * Object that stores the different file formats that will be accepted as input.
  * Every case object must extend the trait FileFormat, in order for it to be used.
  */
object FileFormats {
  sealed trait FileFormat
  case object TextFormat extends FileFormat
  case object GzFormat extends FileFormat
  case object ZipFormat extends FileFormat
  case object TarGzFormat extends FileFormat
}
