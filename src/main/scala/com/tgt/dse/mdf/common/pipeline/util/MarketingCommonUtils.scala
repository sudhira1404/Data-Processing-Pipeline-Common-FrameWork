package com.tgt.dse.mdf.common.pipeline.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.exceptions.{ConfigException, SparkUserAppException}
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{AlpacaException, GeneralException}
import com.tgt.dse.mdf.common.pipeline.validation.ConfigValidatorService
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.{File, InputStreamReader}
import java.net.URI
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object MarketingCommonUtils extends LoggingTrait {

  /**
   * Create spark Session. Based on the need configure more spark related configurations in application.conf
   * read them and apply them while creating the session
   *
   * @param pipeLineConfig configurations
   * @return valid SparkSession
   */
  def createSparkSession(pipeLineConfig: PipeLineConfig, env: String): SparkSession = {

    if (env != "local") {
      val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(SparkSession.builder
        .appName(pipeLineConfig.config.getString(MarketingConstants.APP_NAME))
        .enableHiveSupport()
        .config(MarketingConstants.SPARK_SQL_SOURCES_PARTITIONOVERWRITEMODE,
          pipeLineConfig.config.getString(MarketingConstants.SPARK_SOURCES_PARTITIONOVERWRITEMODE))
        .config(MarketingConstants.HIVE_EXEC_DYNAMIC_PARTITION, MarketingConstants.HIVE_PARTITION)
        .config(MarketingConstants.HIVE_EXEC_DYNAMIC_PARTITION_MODE, MarketingConstants.HIVE_PARTITION_MODE)
        .config(MarketingConstants.SPARK_SQL_ORC_IMPL,
          pipeLineConfig.config.getString(MarketingConstants.SPARK_ORC_IMPL))
        .config(MarketingConstants.SPARK_SQL_ORC_ENABLEVECTORIZEDREADER,
          pipeLineConfig.config.getString(MarketingConstants.SPARK_ENABLEVECTORIZEDREADER))
        .config(MarketingConstants.HIVE_EXEC_MAX_DYNAMIC_PARTITIONS,
          pipeLineConfig.config.getString(MarketingConstants.HIVE_MAX_DYNAMIC_PARTITIONS))
        .config(MarketingConstants.SPARK_SHUFFLE_PARTITIONS, MarketingConstants.ONE_HUNDRED)
        .config(MarketingConstants.SPARK_MAX_TO_STRING_FIELDS, MarketingConstants.ONE_HUNDRED)
        .getOrCreate())
      log.info("created sparkSession")
      sparkSession
    }
    else {
      val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
        .appName("dpp_local").config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.orc.impl", "native") //avoid exception
        .getOrCreate()

      sparkSession
    }
  }


  /**
   * Load the configuration file and log them
   *
   * @param configPath configuration path
   * @return config object read from configuration path
   */
  def loadConfigurationFile(configPath: String, validateConfig: Seq[String] = Seq.empty[String]): PipeLineConfig = {
    log.info(s"Loading configuration file $configPath")
    val applicationConf: Config = if (configPath.startsWith("hdfs")) {
      getHadoopFileConfiguration(configPath)
    } else {
      ConfigFactory.parseFile(new File(configPath))
    }

    //initialize the pipeLineConfig, log all the configurations and check if these paths exists, we are not sending metric to
    // kafka so these params are null
    val pipeLineConfig: PipeLineConfig = new PipeLineConfig(applicationConf,
      "",
      "")
    val validConfig = ConfigValidatorService.validateConfigurations(pipeLineConfig, validateConfig)
    log.info(s"Status of validation configurations is $validConfig")

    //if the configurations are incorrect then throw the exception
    if (!validConfig) {
      log.error(s"Invalid configurations, please correct $configPath and rerun ..app exiting")
      throw ConfigException(s"Invalid configurations, please correct $configPath  and rerun ..app exiting")
    }
    log.info(s"Loading configuration file completed successfully from : $configPath")
    pipeLineConfig
  }

  /**
   * Used to read the contents of a file on HDFS into a Typesafe Config object. The configPath variable passed in here
   * MUST begin with "hdfs://". This supports HDFS paths with or without authority, but they all must start with "hdfs://".
   *
   * Examples of valid paths: {{{
   *   - hdfs://user/SVMDEDMD/landing/criteo/criteo_category_dly/configurations/properties/criteo_category_dly_tsv_DPP.conf
   *   - hdfs:///user/SVMDEDMD/landing/criteo/criteo_category_dly/configurations/properties/criteo_category_dly_tsv_DPP.conf
   *   - hdfs://bigred3ns/user/SVMDEDMD/landing/criteo/criteo_category_dly/configurations/properties/criteo_category_dly_tsv_DPP.conf
   * }}}
   * @param configPath Path to the HDFS file starting with "hdfs://"
   * @return A Typesafe Config object
   */
  def getHadoopFileConfiguration(configPath: String): Config = {
    val hdfs: FileSystem = FileSystem.get(new URI("hdfs:///"), new Configuration())
    try {
      val configPathURI = new URI(configPath)
      val configPathURIAuthority = configPathURI.getAuthority
      val configPathPath: String = if (configPathURIAuthority != null) {
        configPathURIAuthority match {
          case "bigred3ns" | "bigredns" => configPathURI.getPath
          case _ => "/" + configPathURIAuthority + configPathURI.getPath
        }
      } else {
        configPathURI.getPath
      }
      val reader = new InputStreamReader(hdfs.open(new Path(configPathPath)))
      ConfigFactory.parseReader(reader)
    } finally {
      hdfs.close()
    }
  }

  /**
   * Usage description and load the application configuration
   *
   * @param args input arguments
   * @return application configuration
   */
  def usage(args: Array[String]): Unit = {
    if (args.length < 1) {
      val usageArguments = "spark-submit --class com.tgt.dse.mdf.store.pressign.StorePresentationSignEventsDataPipeline \\\n" +
        "    --master local[*] \\\n" +
        "    --driver-memory 4g \\\n" +
        "    --executor-memory 2g \\\n" +
        "    --executor-cores 1 \\\n" +
        "    StorePresentationSign*.jar \\\n" +
        "    <appConfPath> "

      log.info(usageArguments)
      log.error("Please correct the configurations and rerun the application as per above template .. app exiting")
      throw SparkUserAppException(-1)
    }
  }


  /**
   * Append / in the end
   *
   * @param inputPath inputPath
   * @return inputPath appended with /
   *
   */
  def appendSlashInEnd(inputPath: String): String = {
    //File.separator - check file separated
    if (inputPath.endsWith("/")) {
      inputPath
    } else {
      inputPath + "/"
    }
  }

  def csvStructSchema(schemaString: String): StructType = {
    val typeToSparkType = Map("int" -> IntegerType, "string" -> StringType, "date" -> DateType, "timestamp" -> TimestampType, "double" -> DoubleType
      , "long" -> LongType, "boolean" -> BooleanType, "float" -> FloatType, "byte" -> ByteType, "short" -> ShortType
    )
    val parsedSchemaString = schemaString.replaceAll(",\\s+", ",").replaceAll("\\s+,", ",")
    val colNameType = parsedSchemaString.split(",").map { s =>
      val values = s.split("\\s+")
      (values(0), values(1))
    }
    val schema = StructType(colNameType.map(t => StructField(t._1, typeToSparkType(t._2), true)))
    log.info(s"Parsed Schema is ${schema.mkString("Array(", ", ", ")")}")
    schema.printTreeString()
    schema

  }

  /**
   * Iterate over a MAP and produces commma seperated result in the format key=value */

  def iterateMap(mapInput: Map[String, Any]): String = {

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    var concatResult = ""
    for (key <- mapInput.keySet) {
      val parseMapToString = mapper.writeValueAsString(mapInput.get(key))
      concatResult = key + "=" + parseMapToString + "," + concatResult
    }
    concatResult
  }

  def setJobGroup(groupId: String, description: String)(
    implicit sparkSession: SparkSession
  ): Unit = {
    sparkSession.sparkContext.setJobGroup(groupId, description)
  }


//  /**
//   * Retry Function
//   *
//   * @param retries      : Retry count
//   * @param functionName : Function name
//   * @tparam T : Function T
//   * @return : Try Function
//   */
//  def withRetry[T](retries: Int, sleep: Long, functionName: String)(fun: => T): Try[T] = (1 to retries)
//    .foldLeft[Try[T]](Failure(new Exception)) {
//      case (x@Success(_), _) => x
//      case _ =>
//        if (s"$retries" > "1") {
//          log.info(s"Sleeping for $sleep before executing the function $functionName,with retry being $retries times")
//          Thread.sleep(sleep)
//        }
//        Try(fun)
//    }

  /**
   * Retry Function which sleeps before making next call after the first attempt
   *
   * @param retries      : Retry count
   * @param functionName : Function name
   * @tparam T : Function T
   * @return : Try Function
   */
  @tailrec
  def withRetries[T](retries: Int, sleep: Long, functionName: String)(fn: => T): Try[T] = {
    Try(fn) match {
      case x: Success[T] => x
      case _ if retries > 1 => log.info(s"Sleeping for $sleep before executing the function $functionName,with remaining retry being $retries - 1 times")
        Thread.sleep(sleep)
        withRetries(retries - 1, sleep, functionName)(fn)
    }
  }

  /**
   * Subtracts a number of days from a given date.
   *
   * @param columnName1 Date
   * @param columnName2 Number of days
   * @return new Column
   */
  def subtractDateColumns(columnName1: String, columnName2: String): Column = {
    val subtractColumn: Column = expr(s"date_sub($columnName1, $columnName2)")
    subtractColumn
  }


  /**
   * Retry Function which sleeps before making next call after the first attempt and
   * throws exception in case of failure after retry
   *
   * @param retries      : Retry count
   * @param sleep        : sleep
   * @param functionName : Function name
   * @tparam T : Function T
   * @return : Try Function
   */

  @tailrec
  def withRetryAndThrowException[T](retries: Int, sleep: Long, functionName: String,
                                    exceptionMsg: String = "Error reading from external table =>")(fn: => T): T = {
    log.info("Attempts remaining: " + retries)
    Try(fn) match {
      case Success(t) => t
      case Failure(_) if retries > 1 =>
        log.info(s"Sleeping for $sleep before executing the function $functionName,with remaining retry being $retries - 1 times")
        Thread.sleep(sleep)
        withRetryAndThrowException(retries - 1, sleep, functionName, exceptionMsg)(fn)
      case Failure(e) => throw GeneralException(exceptionMsg + e)
    }
  }

  /**
   * Read from table using alpaca
   *
   * @param hiveConfig  :                  hiveConfig map object
   * @param inputQuery  :                  inputQuery
   * @param env                :                         env
   * @return Dataframe
   */
  @SuppressWarnings(Array("TryGet"))
  def alpacaRead(hiveConfig: mutable.Map[String, String], inputQuery: String, env: String)(implicit sparkSession: SparkSession): DataFrame = {

    if (env == "local") {
      import sparkSession.implicits._
      val alpacaReadOutputDf = Seq(("2022-07-13", 12345)).toDF("report_d", "count1")
      alpacaReadOutputDf.withColumn("count", $"count1".cast(LongType)).drop(col("count1"))
    }

    else {
      val alpacaReadDF = alpacaWithRetry(3, 3 * 1000, "alpacaRead")(
        sparkSession.read.format("alpaca").options(hiveConfig).load(inputQuery)
      )

      //      if (alpacaReadDF.isFailure) {
      //        throw AlpacaException(s"AlpacaRead failed.Error....$alpacaReadDF")
      //      }

      //alpacaReadDF.get

      alpacaReadDF
    }
  }

  /**
   * Retry Function which sleeps before making next call after the first attempt and
   * throws exception in case of failure after retry
   *
   * @param retries      : Retry count
   * @param functionName : Function name
   * @tparam T : Function T
   * @return : Try Function
   */

  @tailrec
  def alpacaWithRetry[T](retries: Int, sleep: Long, functionName: String)(fn: => T): T = {
    log.info("Attempts remaining: " + retries)
    Try(fn) match {
      case Success(t) => t
      case Failure(_) if retries > 1 =>
        log.info(s"Sleeping for $sleep before executing the function $functionName,with remaining retry being $retries - 1 times")
        Thread.sleep(sleep)
        alpacaWithRetry(retries - 1, sleep, functionName)(fn)
      case Failure(e) => throw AlpacaException("AlpacaRead failed.Error...." + e)
    }
  }

  /**
   * Rounded value based on roundConditionValue
   *
   * @param d                   Value to be rounded
   * @param roundConditionValue Condition like  0.1.If value is 2.2 that needs to be rounded and condition is 0.1 then result is 3.
   *                            If condition is 0.3 then rounded value is 2
   * @return Rounded value based on roundConditionValue
   */
  //noinspection ScalaStyle

  def rounding(d: Double, roundConditionValue: Double): Int = {
    val dAbs = Math.abs(d)
    val i = dAbs.toInt
    val result = dAbs - i.toDouble
    if (result < roundConditionValue) if (d < 0) {
      -i
    }
    else {
      i
    }
    else if (d < 0) {
      -(i + 1)
    }
    else {
      i + 1
    }
  }

  /**
   * Determine if hive table is EXTERNAL or MANAGED
   *
   * @param env        env
   * @param schemaName schemaName
   * @param tableName  tableName
   * @return EXTERNAL or MANAGED
   */
  def hiveTableType(env: String,
                    schemaName: String,
                    tableName: String
                   )(implicit sparkSession: SparkSession): String = {

    val tableType = if (env == "local") {
      "EXTERNAL"
    }
    else {
      sparkSession.catalog.listTables(schemaName)
        .where(s"name='${tableName}'")
        .select("tableType")
        .collect()(0)(0).toString
    }

    log.info(s"$schemaName.$tableName is=>" + tableType)

    tableType
  }

}