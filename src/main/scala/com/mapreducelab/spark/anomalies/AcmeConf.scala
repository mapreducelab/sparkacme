package com.mapreducelab.spark.anomalies

import com.mapreducelab.spark.anomalies
import com.typesafe.config.Config

import scala.util.Try

/** A QueryConf.
  *
  *  @constructor create a new QueryConf with a query, printMessage.
  *  @param query
  *  @param printMessage
  */
case class QueryConf (
                        query: String = "",
                        printMessage: String = ""
                      )

/** Factory for [[anomalies.QueryConf]] instances. */
object QueryConf {
  def apply(config: Config) = {
    new QueryConf(
      query = Try(config.getString("query")).getOrElse(s""),
      printMessage = Try(config.getString("print-message")).getOrElse("")
    )
  }
  def apply() = new QueryConf()
}

/** A SourceConf.
  *
  *  @constructor create a new SourceConf with a workingDir, filePath and tableName.
  *  @param workingDir the source working directory
  *  @param filePath path for the file to be red
  *  @param tableName table name for the given file to be registered as a temporary table
  */
case class SourceConf (
  workingDir: String = defaultWorkingDir,
  filePath: String = "${defaultWorkingDir}${fs}data.json",
  tableName: String = "members"
  )

/** Factory for [[anomalies.SourceConf]] instances. */
object SourceConf {
  def apply(config: Config) = {
    new SourceConf(
      workingDir = Try(config.getString("working-dir")).getOrElse(s"${defaultWorkingDir}"),
      filePath = Try(config.getString("file-path")).getOrElse(s"${defaultWorkingDir}${/}data.json"),
    tableName = Try(config.getString("table-name")).getOrElse("members")
    )
  }
  def apply() = new SourceConf()
}

/** A AnomaliesConf.
  *
  *  @constructor create a new AnomaliesConf.
  *  @param jsonOutputPath
  *  @param tableName
  */
case class AnomaliesConf(
  jsonOutputPath: String = s"${defaultWorkingDir}${/}anomalies",
  tableName: String = "anomalies",
  DataBoundsValidation: QueryConf = QueryConf(),
  DataBoundsValidationTotal: QueryConf = QueryConf(),
  ActiveCanceledValidation: QueryConf = QueryConf(),
  boundsFinalResult: QueryConf = QueryConf(),
  validRecords: QueryConf = QueryConf()
  )

/** Factory for [[anomalies.AnomaliesConf]] instances. */
object AnomaliesConf {

  def apply(config: Config) = {
    new AnomaliesConf(
      jsonOutputPath = Try(config.getString("json-output-path")).getOrElse(s"${defaultWorkingDir}${/}anomalies"),
      tableName = Try(config.getString("table-name")).getOrElse("anomalies"),
      DataBoundsValidation = QueryConf(config.getConfig("data-bounds-validation")),
      DataBoundsValidationTotal = QueryConf(config.getConfig("data-bounds-validation-cube")),
      ActiveCanceledValidation = QueryConf(config.getConfig("active-canceled-validation")),
      boundsFinalResult = QueryConf(config.getConfig("data-bounds-final-result")),
      validRecords = QueryConf(config.getConfig("valid-records"))
    )
  }
  def apply() = new AnomaliesConf()
}

/** A ValidConf.
  *
  *  @constructor create a new ValidConf.
  *  @param parquetPath
  *  @param outputJsonPath
*  @param parquetTableName
  */
case class ValidConf(
  parquetPath: String = s"${defaultWorkingDir}${/}parquet",
  outputJsonPath: String = s"${defaultWorkingDir}${/}validoutput",
  parquetTableName: String = "members_parquet"
  )

/** Factory for [[anomalies.ValidConf]] instances. */
object ValidConf {

  def apply(config: Config) = {
    new ValidConf(
      parquetPath = Try(config.getString("parquet-path")).getOrElse(s"${defaultWorkingDir}${/}parquet"),
      outputJsonPath = Try(config.getString("json-output-path")).getOrElse(s"${defaultWorkingDir}${/}validoutput"),
      parquetTableName = Try(config.getString("parquet-table-name")).getOrElse("members_parquet")
    )
  }
  def apply() = new ValidConf()
}

/** A TargetConf.
  *
  *  @constructor create a new TargetConf.
  *  @param anomalies
  *  @param valid
  */
case class TargetConf(
  workingDir: String = s"${defaultWorkingDir}",
  anomalies: AnomaliesConf = AnomaliesConf(),
  valid:ValidConf = ValidConf()
)

/** Factory for [[anomalies.TargetConf]] instances. */
object TargetConf {

  def apply(config: Config) = {
    new TargetConf(
      workingDir = Try(config.getString("working-dir")).getOrElse(defaultWorkingDir),
      anomalies = Try(AnomaliesConf(config.getConfig("anomalies"))).getOrElse(AnomaliesConf()),
      valid = ValidConf(config.getConfig("valid"))
    )
  }
  def apply() = new TargetConf()
}

/** A ValidConf.
  *
  *  @constructor create a new ValidConf.
  *  @param master
  *  @param appName
  */
case class SparkAppConf(
 master: String = "master",
 appName: String = "chimebank-detector"
)

/** Factory for [[anomalies.SparkAppConf]] instances. */
object SparkAppConf {
  def apply(config: Config) = {
    new SparkAppConf(
      master = Try(config.getString("master")).getOrElse("local"),
      appName = Try(config.getString("app-name")).getOrElse("chimebank-detector"))
  }
  def apply() = new SparkAppConf()
}

/** A AcmeConf.
  *
  *  @constructor create a new ValidConf.
  *  @param spark
  *  @param source
  *  @param target
  */
case class AcmeConf(
  spark: SparkAppConf = SparkAppConf(),
  source: SourceConf = SourceConf(),
  target: TargetConf = TargetConf()
)

/** Factory for [[anomalies.AcmeConf]] instances. */
object AcmeConf {
  def apply (config: Config) = {
    new AcmeConf(
      spark = Try(SparkAppConf(config.getConfig("spark"))).getOrElse(SparkAppConf()),
      source = Try(SourceConf(config.getConfig("source"))).getOrElse(SourceConf()),
      target = Try(TargetConf(config.getConfig("target"))).getOrElse(TargetConf())
    )
  }
    def apply () = {
      new AcmeConf()
    }
}