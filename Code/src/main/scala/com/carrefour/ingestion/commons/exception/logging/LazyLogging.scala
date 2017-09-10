package com.carrefour.ingestion.commons.exception.logging

import java.text.SimpleDateFormat
import java.util.Calendar

import org.slf4j.{Logger, LoggerFactory}

trait LazyLogging extends CommonsCte with LazyLoggingCte {

  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private val ourApp: String = appName + ": "

  /***
    * @param message Information message to display in the log
    **/
  def infoLog(message: String): Unit = logger.info(ourApp + message)

  /**
    * Write a warning log with space before the message
    **/
  def warnLog(message: String): Unit = logger.warn(ourApp + message)

  def debugLog(message: String): Unit = logger.debug(ourApp + message)

  /**
    * Write error log with spaces before message
    **/
  def errorLog(message: String): Unit = logger.error(ourApp + message)

  /**
    * Trace error in log
    *
    * @param clase   Class where the error is generated
    * @param metodo  Method where the error is generated
    * @param hora    Time at which the error is generated
    * @param message Error message
    */
  def errorLog(clase: String, metodo: String, hora: String, message: String): Unit = {
    errorLog(clase + " > " + metodo + " > " + hora + ": " + message)
  }


  /**
    * Reports start of execution of a method
    *
    * @param methodName Name of method executed
    */
  def initLog(methodName: String): Unit = {
    warnLog(executing + methodName)
  }

  /**
    *
    * @param format Date format in which we want to get the output String
    * @return with the date format received as parameter
    */
  def getTime(format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    val now = Calendar.getInstance().getTime
    dateFormat.format(now)
  }


  /**
    * Reports that we have finished executing a method
    *
    * @param methodName Name of method executed
    */
  def endLog(methodName: String): Unit = warnLog(endOfExecution + methodName)
}
