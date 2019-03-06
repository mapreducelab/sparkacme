package com.mapreducelab.spark

import java.io.File
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.SparkSession

package object anomalies  extends LazyLogging {

  val / = File.separator
  val defaultWorkingDir = / + "src" + / + "main" + / + "resources" + / + "data"

  /** Validate provided email
    *
    * @param email
    * @return Boolean true if email is valid and false otherwise
    */
  def isValidEmail(email : String): Boolean = if("""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$""".r.findFirstIn(email) == None) false else true

  /** Validate provided zip code
    *
    * @param zipCode
    * @return Boolean true if zip code is valid and false otherwise
    */
  def isValidZipCode(zipCode: String): Boolean = if("""^[0-9]{5}$""".r.findFirstIn(zipCode) == None) false else true

  /** Validate provided phone number
    *
    * @param phone
    * @return Boolean true if phone number has 10 digits and false otherwise
    */
  def isValidPhoneNumber(phone: String): Boolean = if("""^[0-9]{10}$""".r.findFirstIn(phone) == None) false else true

  /** Create Dataset from the given path
    *
    * @param filePath
    * @param cache
    * @return jsonDataset
    */
  def readJsonFile(filePath: String, cache: Boolean)(implicit spark: SparkSession) = {
    val dataset =  spark.read.json(filePath)
    dataset.printSchema
    dataset.show(120,220,false)
    if (cache) dataset.cache else dataset
  }

  /** Create Dataset from the given path and register temporary table
    *
    * @param filePath
    * @param tableName
    * @param cache
    * @return jsonDataset
    */
  def readJsonFile(filePath: String, tableName: String, cache: Boolean)(implicit spark: SparkSession) = {
    val dataset =  spark.read.json(filePath)
    dataset.createOrReplaceTempView(tableName)
    dataset.printSchema
    dataset.show(120,220,false)
    if (cache) dataset.cache else dataset
  }

  /** Create Dataset from the given path and register temporary table
    *
    * @param filePath
    * @param tableName
    * @param cache
    * @return DataFrame
    */
  def readParquetFile(filePath: String, tableName: String, cache: Boolean )(implicit spark: SparkSession) = {
    val dataset =  spark.read.json(filePath)
    dataset.createOrReplaceTempView(tableName)
    dataset.printSchema
    dataset.show(120,220,false)
    if (cache) dataset.cache else dataset
  }

  /** Register validators as User Defined Functions
    *
    * @param spark
    * @return UserDefinedFunction
    */
  def registerUserDefinedFunctions(implicit spark: SparkSession) = {
    spark.sqlContext.udf.register("is_valid_email", (email: String) => isValidEmail(email))
    spark.sqlContext.udf.register("is_valid_zip_code", (zip: String) => isValidZipCode(zip))
    spark.sqlContext.udf.register("is_valid_phone", (phone: String) => isValidPhoneNumber(phone))
  }

  /** Spark Sql shortcut
    *
    * @param sqlCode
    * @return Dataset
    */
  def sql(sqlCode: String)(implicit spark: SparkSession) = spark.sql(sqlCode.stripMargin)

  /** Stop execution
    *
    * @param exitMessage
    * @return
    */
  def systemExit(exitMessage: String) = {
    log.error(exitMessage)
    sys.exit(1)
  }

  def systemExit(exitMessage: String, spark: SparkSession) = {
    log.error(exitMessage)
    spark.stop()
    sys.exit(1)
  }

  /** Remove line separator from the given string
    *
    * @param str line separator to be removed
    * @return replaced line separator by space
    */
  def strip(str: String, isStrip: Boolean = false) = if (isStrip) str.stripMargin.replaceAll("\r|\n", " ") else str

  object log {
    def info(info: String, isStrip: Boolean = false) = logger.info(strip(info,isStrip))
    def warn(warn: String, isStrip: Boolean= false) = logger.warn(strip(warn,isStrip))
    def debug(debug: String, isStrip: Boolean= false) = logger.debug(strip(debug,isStrip))
    def error(error: String, isStrip: Boolean= false) = logger.error(strip(error,isStrip))
  }
}


