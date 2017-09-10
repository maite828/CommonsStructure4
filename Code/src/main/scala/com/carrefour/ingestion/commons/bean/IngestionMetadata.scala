package com.carrefour.ingestion.commons.bean

/**
    *
    * @param inputPath
    * @param outputDb
    * @param outputTable
    * @param fileType
    * @param timestampFormat
    * @param encloseChar
    * @param date
    * @param year
    * @param month
    * @param day
    * @param businessunit
    * @param entity
    */
  case class IngestionMetadata(
                                inputPath: String = "",
                                outputDb: String = "",
                                outputTable: String = "",
                                fileType: FileType,
                                timestampFormat: String = "",
                                encloseChar: String = "",
                                date: Int = 0,
                                year: Int = 0,
                                month: Int = 0,
                                day: Int = 0,
                                var businessunit: String = "",
                                entity: String = "")


