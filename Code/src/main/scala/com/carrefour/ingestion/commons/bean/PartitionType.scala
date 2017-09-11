package com.carrefour.ingestion.commons.bean

import java.util.regex.Pattern

/**
  * Generic trait. Every type of file must extend this trait, in order for it to be used.
  */
trait PartitionType {
  def getPartitionFields(settings: IngestionMetadata): Array[Int]
}

/**
  *
  * @param partition_date_format
  * @param partition_transform
  */
case class ParameterPartitionType(
                                   partition_date_format: String,
                                   partition_transform: String
                                 ) extends PartitionType {
  override def getPartitionFields(settings: IngestionMetadata): Array[Int] =
  //TODO Formatear fecha en función de partition_date_format
    partition_date_format match {
      case "yyyymmdd" => Array(
        settings.date.toString.substring(0, 4).toInt,
        settings.date.toString.substring(4, 6).toInt,
        settings.date.toString.substring(6, 8).toInt
      )
    }

  case class FileNamePartitionType(
                                    partition_param: String,
                                    partition_date_format: String,
                                    partition_transform: String
                                  ) extends PartitionType {
    override def getPartitionFields(settings: IngestionMetadata): Array[String] = {
      val partPattern = Pattern.compile(partition_param)
      val matcher = partPattern.matcher(settings.namefile)
      if (!matcher.matches()) {
        //TODO error via log de que no hay fecha en el nombre del fichero

      }
      Array(matcher.group("year"), matcher.group("month"), matcher.group("day"))
    }
  }

}

//^tbusuregistrados.*_(?<year>[0-9]{4})(?<month>[0-9]{2})(?<day>[0-9]{2})_[0-9]{6}\.csv$


