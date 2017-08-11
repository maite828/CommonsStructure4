package com.carrefour.ingestion.commons.util

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import scopt.OptionParser

import scala.io.Source.fromFile


object HqlUtils {



  case class Settings
  (
    inputPath: String = "",
    outputPath: String = "",
    outputSchema: String = "",
    outputTable: String = "",
    hasHeader: Boolean = false,
    isExternal: Boolean = false,
    location: String = "",
    nameCol: Int = -1,
    typeCol: Int = -1,
    typeArgsCol: Int = -1,
    descCol: Int = -1,
    partitionCol: Int = -1,
    otherProperties: String = null
  )

  trait fieldToString {
    this:field =>
    override def toString: String = {
      s"`$fieldName` " +
        s"$fieldType " +
        s"COMMENT '$fieldDescription'"
    }
  }
  trait typeToString{
    this:fieldCC =>
    override def toString: String = {
      val typ: String = transformType(typeName)
      typ + (if(typeHasArgs(typ) && !typeArgs.isEmpty) s"(${typeArgs})" else "")
    }
  }

  case class fieldCC
  (
    typeName: String = "string",
    typeArgs: String = ""
  ) extends typeToString

  case class field
  (
  fieldName: String,
  fieldType: fieldCC,
  fieldDescription: String = "",
  isPartition: Boolean = false

  ) extends fieldToString

  case class outTable
  (
  fields: Array[field]
  )

  def typeHasArgs(str:String): Boolean = {
    str match{
      case str:String if str.equalsIgnoreCase("decimal") => true
      case _ => false
    }
  }

  def readCSV(settings: Settings): Iterator[Array[String]] = {

    //Check whether specified file exists or not
    if(!Files.exists(Paths.get(settings.inputPath))){
      throw new IllegalArgumentException("Specified input file doesn't exist")
    }
    val bufferedSource = fromFile(settings.inputPath, "ISO-8859-1")
    if(settings.hasHeader) bufferedSource.getLines().drop(1).map(_.split("\t"))
    else bufferedSource.getLines().map(_.split("\t"))
  }

  def fillFields(fields: Iterator[Array[String]])(settings: Settings): outTable = {
    val table: outTable = outTable(fields.map(line => {
      field(
        line(settings.nameCol),
        fieldCC(line(settings.typeCol),if(settings.typeArgsCol != -1) line(settings.typeArgsCol) else null),
        line(settings.descCol),
        if(settings.partitionCol != -1)
          line(settings.partitionCol).equalsIgnoreCase("yes")
        else false)
    }).toArray)
    table
  }

  def transformType(fieldType: String):String = {
    fieldType match{
      case str: String if str.equalsIgnoreCase("char") => "string"
      case str: String if str.equalsIgnoreCase("date") => "timestamp"
      case str: String if str.equalsIgnoreCase("number") => "decimal"
      case unknown => throw new IllegalArgumentException(s"Unknown type: $unknown")
    }
  }

  def buildHql(table: outTable)(settings: Settings): String = {
    val builder = StringBuilder.newBuilder
    builder.append("CREATE ")
    if (settings.isExternal)
      builder.append("EXTERNAL ")
    builder.append(s"TABLE `${settings.outputSchema}`.`${settings.outputTable}`")
    builder.append(table.fields.filter(field => !(field.isPartition)).mkString("(\n", ",\n", ")\n"))
    if (!(table.fields.filter(_.isPartition).isEmpty)) {
      builder.append("PARTITIONED BY ")
      builder.append(table.fields.filter(_.isPartition).mkString("(\n", ",\n", ")\n"))
    }
    builder.append(settings.otherProperties.split(",").mkString("\n"))
    builder.toString()

  }

  def saveHql(hql: String, settings: Settings): Unit = {
    val file = new File(settings.outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(hql)
    bw.close()
  }

  object loadSettings extends OptionParser[Settings]("Settings loader"){
    opt[String]('i',"inputFile") required () valueName "<input file>" action{ (value, config) =>
      val inputValue = value
      config.copy(inputPath = inputValue)
    } text "Path of the input file to load. If the file is not found, an IllegalArgumentException will be thrown."

    opt[String]('o',"outputFile") required () valueName "<output file>" action{ (value, config) =>
      val outputValue = value
      config.copy(outputPath = outputValue)
    } text "Path of the output file to write."

    opt[String]('s',"schema") required () valueName "<schema name>" action{ (value, config) =>
      val schemaValue = value
      config.copy(outputSchema = schemaValue)
    } text "Name of the schema where the table will be stored."

    opt[String]('t',"table") required () valueName "<table name>" action{ (value, config) =>
      val tableValue = value
      config.copy(outputTable = tableValue)
    } text "Name of the output table"

    opt[Unit]('h',"header") action{ (value, config) =>
      config.copy(hasHeader = true)
    } text "This parameter specifies that the input file has a header."

    opt[Unit]('e',"external") action{ (value, config) =>
      config.copy(isExternal = true)
    } text "This parameter specifies that the table is to be created as external."

    opt[String]('l',"location") valueName "<table location>" action{ (value, config) =>
      val locationValue = value
      config.copy(location = locationValue)
    } text "Output location in HDFS where the table data will be stored. It will only be used if it has been configured as external."

    opt[String]('n',"nameCol") valueName "<field name column>" action{ (value, config) =>
      val nameValue = value
      try {
        config.copy(nameCol = nameValue.toInt)
      }catch{
        case e: Exception => {
          throw new IllegalArgumentException(s"Specified name column is not valid: $nameValue")
        }
      }
    } text "Index of the column that contains the name of the fields (Note that the first column has the index 0)."

    opt[String]('y',"typeCol") valueName "<field type column>" action{ (value, config) =>
      val typeValue = value
      try {
        config.copy(typeCol = typeValue.toInt)
      }catch{
        case e: Exception => {
          throw new IllegalArgumentException(s"Specified type column is not valid: $typeValue")
        }
      }
    } text "Index of the column that contains the type of the fields (Note that the first column has the index 0)."

    opt[String]('a',"typeArgsCol") valueName "<field type arguments column>" action{ (value, config) =>
      val typeArgsValue = value
      try {
        config.copy(typeArgsCol = typeArgsValue.toInt)
      }catch{
        case e: Exception => {
          throw new IllegalArgumentException(s"Specified type argument column is not valid: $typeArgsValue")
        }
      }
    } text "Index of the column that contains the type of the fields (Note that the first column has the index 0)."

    opt[String]('d',"descCol") valueName "<field description column>" action{ (value, config) =>
      val descValue = value
      try {
        config.copy(descCol = descValue.toInt)
      }catch{
        case e: Exception => {
          throw new IllegalArgumentException(s"Specified description column is not valid: $descValue")
        }
      }
    } text "Index of the column that contains the description of the fields (Note that the first column has the index 0)."

    opt[String]('p',"partitionCol") valueName "<field partition column>" action{ (value, config) =>
      val partitionValue = value
      try {
        config.copy(partitionCol = partitionValue.toInt)
      }catch{
        case e: Exception => {
          throw new IllegalArgumentException(s"Specified partition column is not valid: $partitionValue")
        }
      }
    } text "Index of the flag column which specifies if that field is a partitioning field (Note that the first column has the index 0)."

    opt[String]('x',"other") valueName "<other properties>" action{ (value, config) =>
      val otherValue = value
      config.copy(otherProperties = otherValue)
    } text "Other properties."

  }

  def main(args: Array[String]): Unit = {
    loadSettings.parse(args, Settings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")){
      settings => {
        val str = readCSV(settings)
        val table = fillFields(str)(settings)
        val hqlString = buildHql(table)(settings)
        saveHql(hqlString, settings)
      }
    }

  }

}
