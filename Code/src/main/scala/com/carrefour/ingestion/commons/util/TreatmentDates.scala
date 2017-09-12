package com.carrefour.ingestion.commons.util

object TreatmentDates {
//
//  def getPartitionFields(settings:IngestionMetadata): Array[Int] = {
//    settings.partitionType match {
//      case parType:ParameterPartitionType => getPartFieldsFromParam(settings)
//      case parType:FileNamePartitionType => getPartFieldsFromFileName(settings)
//    }
//  }
//
//  def getPartFieldsFromParam(settings: IngestionMetadata): Array[Int] = {
//    Array(
//      settings.date.toString.substring(0, 4).toInt,
//      settings.date.toString.substring(4, 6).toInt,
//      settings.date.toString.substring(6, 8).toInt
//    )
//  }
//
//
//  def getPartFieldsFromFileName(settings:IngestionMetadata) : Array[Int] = {
//    val FilenamePattern = Pattern.compile(settings.partitionType.partition_param)
//  }
//
//  def dataDatePart(partitionType: PartitionType): Array[Int] = {
//
//    val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
//    initLog(methodName)
//
//    var loadYear: Int = 0
//    var loadMonth: Int = 0
//    var loadDay: Int = 0
//
//    if (settings.datepart_file == 0) {
//      loadYear = settings.date.toString.substring(0, 4).toInt
//      loadMonth = settings.date.toString.substring(4, 6).toInt
//      loadDay = settings.date.toString.substring(6, 8).toInt
//    } else {
//      val dat = settings.namefile.split("_")
//      infoLog(dat(2))
//
//      val date = dat(2).split(".")
//      warnLog(date(0))
//
////        loadYear = date(1).substring(0, 4).toInt
////        loadMonth = date(1).substring(4, 6).toInt
////        loadDay = date(1).substring(6, 8).toInt
//  }
//
//    endLog(methodName)
//
//    try {
//      Array(loadYear, loadMonth, loadDay)
//    } catch {
//      case _: Throwable => return Array(0, 0, 0)
//    }
//  }

}
