package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.constants.{CoreConstants, DPPMetricsConstants}
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.HiveWriteException
import com.tgt.dse.mdf.common.pipeline.service.HiveExternalTableWriter.{generateSqlStatement, generateViewName}
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success}

/**
 * Instantiate this when writing to a Hive external table without using Alpaca, Kelsa 2-Step Write, or JDBC.
 *
 * @param env Which environment does this run in. If set to "local" will write the DataFrame via the DataFrame.write method.
 *            Otherwise uses Spark.sql to write to a Hive external table.
 * @param insertColumnNames This is the final list of columns to select from the DataFrame when writing to the Hive
 *                                    external table. This list should be in order of the columns in the Hive table, and should
 *                                    include all partition column names as well (at the end of the Sequence).
 * @param partitionColumnNames [OPTIONAL] This is the names of only the column(s) making up the partition in the Hive external table. Defaults to `Seq("report_d")`
 * @param dirWriteLocation [OPTIONAL] This is where to write the files when performing a local unit test or when inserting to a directory instead of to Hive table.
 * @param schema [OPTIONAL] The database name of the Hive external table to write to or select from (when writing to a directory). Not used for local testing.
 * @param tableName [OPTIONAL] The table name of the Hive external table to write to or select from (when writing to a directory). Not used for local testing.
 * @param writeRetryCount [OPTIONAL] Sets how many attempts to retry in case there are failures encountered with the insert to Hive external table.
 *                            Defaulted to 3.
 * @param writeRetrySleep [OPTIONAL] Sets how long to sleep in between retry attempts. Defaulted to 3 seconds.
 * @param localSaveFormat [OPTIONAL] Only for local testing, used to specify the format to save the DataFrame to in the local filesystem. Defaults to "orc"
 * @param writeMode [OPTIONAL] Used to specify the mode either saving to local filesystem or writing to external Hive table. Defaults to "overwrite".
 *                  Use only "overwrite" or "append"
 * @param dirIsLocal [OPTIONAL] Used with dirWriteLocation. If true, writes location specified to a local directory,
 *                   else writes to hdfs directory. Defaults to false.
 * @param repartition [OPTIONAL] Defaulted to true. If true, partitions based on partitionColumnNames else will not
 */
final case class HiveExternalTableWriter(env: String,
                                         insertColumnNames: Seq[String],
                                         partitionColumnNames: Seq[String] = MarketingConstants.DEFAULT_PARTITION_COLUMNS,
                                         dirWriteLocation: String = "",
                                         schema: String = "",
                                         tableName: String = "",
                                         writeRetryCount: Int = MarketingConstants.WRITE_RETRY_COUNT,
                                         writeRetrySleep: Long = MarketingConstants.WRITE_RETRY_SLEEP,
                                         localSaveFormat: String = CoreConstants.ORC,
                                         writeMode: String = CoreConstants.OVERWRITE,
                                         dirIsLocal: Boolean = false,
                                         repartition: Boolean = true,
                                         recordFinalCount: Boolean = true
                                        ) {
  val viewName: String = generateViewName(env,schema,tableName,dirWriteLocation)
  val sqlStatement: String = {
    if (env.equals(MarketingConstants.LOCAL_ENV)) {
      //Not passing directory so generated SQL statement that prints to debug logs shows what the statement would be
      //if this had run on BigRED3
      generateSqlStatement(insertColumnNames, viewName, partitionColumnNames, writeMode, schema, tableName)
    } else {
      generateSqlStatement(insertColumnNames, viewName, partitionColumnNames, writeMode, schema, tableName, dirWriteLocation, dirIsLocal)
    }
  }

}

object HiveExternalTableWriter extends LoggingTrait {

  /**
   * Writes a given DataFrame to the Hive external table specified in the instantiated HiveExternalTableWriter object.
   * In local test environment uses the DataFrame.write method to write to the directory specified in the HiveExternalTableWriter.coreHistoryExternalLocation.
   * In stage/test environments uses sparksession.sql to submit an insert overwrite sql statement to the Hive external table.
   * Please note this method does not require the use of jdbc, llap, Alpaca, Kelsa 2-step write, or HWC. It also does not
   * require alter table statements to drop/add partitions, nor does it require landing the data to an intermediate HDFS location.
   * @param inputDF The DataFrame to be inserted into the Hive external table.
   * @param hiveExternalTableWriter An instantiated [[HiveExternalTableWriter]]
   * @param sparkSession Implicit Spark session
   * @return True if it succeeded in writing to the Hive external table. False in case all retry attempts fail.
   */
  def writeToExternalFndTable(inputDF: DataFrame, hiveExternalTableWriter: HiveExternalTableWriter)
                             (implicit sparkSession: SparkSession): Boolean = {

    var isSuccessful: Boolean = true
    if (hiveExternalTableWriter.recordFinalCount) {
      log.info("Set Pipeline Metrics to Final Insert Overwrite stage")
      PipeLineMetadataService.setStringMetricValue(MarketingConstants.STAGE, MarketingConstants.FINALINSERTOVERWRITE)
    }

    val inputCount = inputDF.count()
    if (inputCount > 0) {
      val repartitionDF: DataFrame = if(hiveExternalTableWriter.repartition) {
        log.info("Repartitioning input DataFrame before final table write.")
        inputDF.repartition(hiveExternalTableWriter.partitionColumnNames.map(col): _*)
      } else {
          inputDF
      }

      repartitionDF.createOrReplaceTempView(hiveExternalTableWriter.viewName)

      MarketingCommonUtils.withRetries(hiveExternalTableWriter.writeRetryCount, hiveExternalTableWriter.writeRetrySleep, "writeToExternalFndTable")(
        // writing the input dataframe to the external table.
        if (hiveExternalTableWriter.env != MarketingConstants.LOCAL_ENV) {
          sparkSession.sql(hiveExternalTableWriter.sqlStatement)
        } else {
          log.debug("Local testing mode, will write to filesystem instead of Hive table.")
          sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
          repartitionDF.select(hiveExternalTableWriter.insertColumnNames.map(col): _*)
            .write
            .partitionBy(hiveExternalTableWriter.partitionColumnNames: _*)
            .mode(hiveExternalTableWriter.writeMode)
            .format(hiveExternalTableWriter.localSaveFormat)
            .save(hiveExternalTableWriter.dirWriteLocation)
        }
      )
      match {
        case Success(_) =>
          log.info(s"Completed writing data from input dataframe into the external table location ${hiveExternalTableWriter.dirWriteLocation}")
        case Failure(e) =>
          log.error(e.getMessage, e.getCause)
          isSuccessful = false
      }
    } else {
      log.warn("There are 0 rows in the DataFrame to write to external Hive table. Skipping write.")
    }
    if (hiveExternalTableWriter.recordFinalCount) {
      log.info(s"Setting the final row count $inputCount to Pipeline metrics")
      PipeLineMetadataService.setLongMetricValue(DPPMetricsConstants.CDSROWSCOUNT, inputCount)
    }
    isSuccessful
  }

  /**
   * This will use spark.sql to generate an INSERT INTO DIRECTORY or INSERT INTO LOCAL DIRECTORY sql statement that would
   * then select rows from the database and table specified in the [[HiveExternalTableWriter]] and insert them into the
   * location specified. Two helper classes which can be leveraged to instantiate the [[HiveExternalTableWriter]] for use
   * with this method are:
   * {{{
   *   HdfsExternalDirectory - Generates an INSERT INTO DIRECTORY sql
   *   LocalExternalDirectory - Generates an INSERT INTO LOCAL DIRECTORY sql
   * }}}
   * This utilizes the [[writeToExternalFndTable()]] method, passing an empty DataFrame to that class.
   * @param hiveExternalTableWriter An instantiated [[HiveExternalTableWriter]] class
   * @param sparkSession Implicit Spark session
   * @return True if it succeeded in writing to the Hive external table. False in case all retry attempts fail.
   */
  def writeToExternalDirectory(hiveExternalTableWriter: HiveExternalTableWriter)(implicit sparkSession: SparkSession): Boolean = {
    writeToExternalFndTable(sparkSession.emptyDataFrame, hiveExternalTableWriter)
  }

  /**
   * Generates a sql statement that can be used either when writing from a DataFrame to a Hive external table OR from an
   * external Hive table to a directory. This is to be used in conjunction with either the [[writeToExternalFndTable()]]
   * or [[writeToExternalDirectory()]] methods. When writing from a DataFrame, the [[writeToExternalFndTable()]] method creates
   * a temp view called `repartitionDF` which is written into the sql statements for writing from a DataFrame.
   *
   * When the `writeToDir` is empty, and the `saveMode` is `append` the generated sql string will be:
   * {{{
   *   insert into table <dbName>.<tableName> partition <partitionColumns> select <allColumns> from repartitionDF
   * }}}
   *
   * When the `writeToDir` is empty, and the `saveMode` is `overwrite` (default) the generated sql string will be:
   * {{{
   *   insert overwrite table <dbName>.<tableName> partition <partitionColumns> select <allColumns> from repartitionDF
   * }}}
   *
   * [EXPERIMENTAL] When the `writeToDir` has a path name in it, and the `localDirWrite` is set to true the generated sql string will be:
   * {{{
   *   insert overwrite local directory <writeToDir> select <allColumns> from <dbName>.<tableName>
   * }}}
   *
   * [EXPERIMENTAL] When the `writeToDir` has a path name in it, and the `localDirWrite` is set to false (default) the generated sql string will be:
   * {{{
   *   insert overwrite directory <writeToDir> select <allColumns> from <dbName>.<tableName>
   * }}}
   * @param allColumns The list of columns to select either from the DataFrame view named `repartitionDF` or from the `tableName` used when writing to a directory.
   * @param partitionColumns The list of columns which are part of the partitioning of the table. This is not used when writing to a directory from a table.
   * @param mode Whether to overwrite or append to the table. This is not used when writing to a directory from a table.
   * @param dbName The database containing the table which will either be written to or selected from.
   * @param tableName The table which will either be written to or selected from.
   * @param writeToDir The directory to write to. Only pass a value to this field when writing to a directory from a table.
   * @param localDirWrite When set to true, the directory is local and the resulting sql statement will include the local keyword, eg "insert overwrite local directory".
   *                      Defaults to false, which would write to an HDFS location instead. Please note the ability to write to a local directory is experimental.
   * @return A sql statement
   */
  def generateSqlStatement(allColumns: Seq[String], viewName: String, partitionColumns: Seq[String] = Seq(""), mode: String = "",
                           dbName: String = "", tableName: String = "", writeToDir: String = "", localDirWrite: Boolean = false): String = {

    var from: String = ""
    var writeTo: String = ""
    var partition: String = ""

    if (writeToDir.isEmpty) {
      partition = s"partition (${partitionColumns.mkString(",")}) "
      from = s"select ${allColumns.mkString(",")} from $viewName"
      writeTo = mode match {
        case "append" => s"into table $dbName.$tableName"
        case "overwrite" => s"overwrite table $dbName.$tableName"
        case _ => throw HiveWriteException(s"Write mode $mode specified for external table writer not supported. Only use `overwrite` or `append`")
      }
    } else {
      from = s"select ${allColumns.mkString(",")} from $viewName"
      writeTo = if (localDirWrite) {
        s"overwrite local directory $writeToDir"
      } else {
        s"overwrite directory $writeToDir"
      }
    }
    val statement: String = s"insert $writeTo $partition $from"
    log.debug(s"Generated sql statement that would be used in non-local environments is: ${statement}")
    statement
  }

  def generateViewName(env: String, dbName: String = "", tableName: String = "", writeToDir: String = ""): String = {
    if (writeToDir.isEmpty || env.equals(MarketingConstants.LOCAL_ENV)) {
      s"${tableName}_${env}_vw"
    } else {
      s"$dbName.$tableName"
    }
  }
}

