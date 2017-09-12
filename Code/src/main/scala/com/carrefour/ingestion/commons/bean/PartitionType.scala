package com.carrefour.ingestion.commons.bean

import java.util.regex.Pattern

import com.carrefour.ingestion.commons.exception.logging.{CommonsException, LazyLogging}

/**
  *
  */
trait PartitionType {
  def getPartitionFields(partString: String): Array[Int]
}

case class ParameterPartitionType(partition_date_format: String, partition_transform: String) extends PartitionType with LazyLogging {
  val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
  initLog(methodName)

  def getPartitionFields(partString: String): Array[Int] = {

    val msgError: String = s"$methodName > No hay fecha en el parametro de entrada"

    //TODO Formatear fecha en función de partition_date_format
    infoLog(partition_date_format.toString)

    partition_date_format match {
      case "yyyymmdd" => Array(
        partString.substring(0, 4).toInt,
        partString.substring(4, 6).toInt,
        partString.substring(6, 8).toInt,
      )
    }
  }

  endLog(methodName)
}

case class FileNamePartitionType(partition_param:String, partition_date_format: String, partition_transform: String) extends PartitionType with LazyLogging {
  val methodName: String = Thread.currentThread().getStackTrace()(1).getMethodName
  initLog(methodName)

  def getPartitionFields(partString: String): Array[Int] = {
    val msgError: String = s"$methodName > No hay fecha en el nombre del fichero"

    val partPattern = Pattern.compile(partition_param)
    infoLog(partPattern.toString)

    val matcher = partPattern.matcher(partString)
    warnLog(matcher.toString)

    if (!matcher.matches()) throw CommonsException(msgError)

    partition_date_format match {
      case "yyyymmdd" => Array(
        matcher.group("year").toInt,
        matcher.group("month").toInt,
        matcher.group("day").toInt
      )
    }
  }
  endLog(methodName)
}


