package com.mapreducelab.spark.anomalies

import org.apache.spark.sql.SparkSession
import scala.language.implicitConversions
import scala.util.Try
import com.typesafe.config._
import java.io.File

/** Main object to run anomalies detection.
  */
object Detector   {

  def main (args: Array[String]): Unit = {
    if (System.getenv("SPARK_HOME") == null) {
      systemExit("SPARK_HOME is not defined")
    }
    if (args.length == 0 ) {
      log.warn("Configuration file is not provided")
    }

    val configPath = Try(args(0)).getOrElse(new java.io.File(".").getCanonicalPath + / + "src" + / + "main" + / + "resources" + / + "config" + / + "acme.conf")
    log.info(s"Config path is $configPath")
    val configFactory = ConfigFactory.parseFile(new File(configPath))
    val config = ConfigFactory.load(configFactory)

    val hasParams = Try {
      config.hasPath("source.working-dir")
    }.getOrElse(false)

    if (!hasParams)
      throw new IllegalArgumentException("Working dir is not defined in the configuration file.")
    else {
      log.info(s"Config file params validation passed.")
    }

    val acmeConf = AcmeConf(config)
    log.info("Acme config: " + acmeConf.toString)

    implicit val spark: SparkSession = SparkSession.builder.appName(acmeConf.spark.appName).master(acmeConf.spark.master).getOrCreate()


    try {
      registerUserDefinedFunctions

      val members = readJsonFile(acmeConf.source.filePath, acmeConf.source.tableName, true)

      log.info(acmeConf.target.anomalies.DataBoundsValidation.printMessage)
      sql(s"${acmeConf.target.anomalies.DataBoundsValidation.query}").show(120, 220, false)

      log.info(acmeConf.target.anomalies.DataBoundsValidationTotal.printMessage)
      sql(
        acmeConf.target.anomalies.DataBoundsValidationTotal.query).show(120, 220, false)

      log.info(acmeConf.target.anomalies.ActiveCanceledValidation.printMessage)
      sql(acmeConf.target.anomalies.ActiveCanceledValidation.query).show

      log.info(acmeConf.target.anomalies.boundsFinalResult.printMessage)
      sql(acmeConf.target.anomalies.boundsFinalResult.query).show(120,220,false)

      // Save records with anomalies to the json file
      sql(s"${acmeConf.target.anomalies.boundsFinalResult.query}".stripMargin).write.mode("overwrite").format("org.apache.spark.sql.json").
        save(acmeConf.target.anomalies.jsonOutputPath)

      // Save valid records to conform the data types
      sql(s"${acmeConf.target.anomalies.validRecords.query}".stripMargin).coalesce(1).write.mode("overwrite").format("org.apache.spark.sql.json").
        save(acmeConf.target.valid.outputJsonPath)

      val validMembers = readJsonFile(acmeConf.target.valid.outputJsonPath,false)

    } catch {
            case e: Exception =>  systemExit(s"$e", spark)
    } finally {
    log.info("Validation is finished.")
    spark.stop()
    }



  }

}
