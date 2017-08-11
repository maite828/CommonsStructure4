package com.carrefour.ingestion.commons.bean


object FileFormats {
  sealed trait FileFormat
  case object TextFormat extends FileFormat
  case object GzFormat extends FileFormat
  case object ZipFormat extends FileFormat
}
