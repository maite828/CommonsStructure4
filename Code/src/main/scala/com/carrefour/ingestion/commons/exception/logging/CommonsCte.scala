package com.carrefour.ingestion.commons.exception.logging

/**
  *
  */
trait CommonsCte {

  val customAppName: String = "Data Ingestion"
  var appName: String = customAppName

  val success :Boolean= true
  val failure :Boolean = false

}