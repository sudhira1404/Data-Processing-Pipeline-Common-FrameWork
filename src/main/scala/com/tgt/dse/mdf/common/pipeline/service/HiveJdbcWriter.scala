package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.constants.{CoreConstants, DPPMetricsConstants, ServiceConstants}
import com.tgt.dsc.kelsa.datapipeline.core.service.{DataReadWriteHelper, PipeLineMetadataService}
import com.tgt.dsc.kelsa.datapipeline.core.types.TableDetails
import com.tgt.dsc.kelsa.datapipeline.core.types.TableDetails.{FinalManagedTable, IntermediateExternalTable}
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.sql.{DriverManager, Statement}
import java.util.Properties
import scala.util.{Failure, Success, Try}


final case class HiveJdbcWriter(pipeLineConfig: PipeLineConfig, foundationInsertColumnNames: Seq[String],
                                foundationPartitionColumnNames: Seq[String],
                                externalStageDatabaseName: String="",
                                externalStageTableName: String="",
                                tempExternalStageLocation: String="",
                                coreHistorySchema: String="",
                                coreHistoryTableName: String=""
                               ) {

  val appQueueName: String = pipeLineConfig.config.getString(MarketingConstants.APP_QUEUENAME)
  val appExecutionEngine: String = pipeLineConfig.config.getString(MarketingConstants.APP_EXECUTIONENGINE)
  val appDynamicPartition: String = pipeLineConfig.config.getString(MarketingConstants.APP_DYNAMICPARTITION)
  val appDynamicPartitionMode: String = pipeLineConfig.config.getString(MarketingConstants.APP_DYNAMICPARTITIONMODE)
  val appJdbcUrl: String = pipeLineConfig.config.getString(MarketingConstants.APP_JDBCURL)
  val HDFS_WRITE_RETRY_COUNT : Int = 2
  val coreHistTableName: String = if (coreHistoryTableName.isEmpty)
    {pipeLineConfig.config.getString(MarketingConstants.APP_coreHistTable)}
  else
     {coreHistoryTableName}

  val coreHistSchema: String = if (coreHistorySchema.isEmpty)
    {pipeLineConfig.config.getString(MarketingConstants.APP_FNDSCHEMA)}
  else {coreHistorySchema}

  val tempFndExtLocation: String = if (tempExternalStageLocation.isEmpty)
  {pipeLineConfig.config.getString(MarketingConstants.APP_TEMPFNDEXTLOCATION)}
  else {tempExternalStageLocation}

  val externalFndTableName: String = if (externalStageTableName.isEmpty)
  {pipeLineConfig.config.getString(MarketingConstants.APP_fndExtTable)}
  else {externalStageTableName}

  val externalFndDatabaseName: String = if (externalStageDatabaseName.isEmpty)
  {pipeLineConfig.config.getString(MarketingConstants.APP_WRKSCHEMA)}
  else {externalStageDatabaseName}

  val externalTableDetails : IntermediateExternalTable = IntermediateExternalTable(externalFndDatabaseName,
    externalFndTableName,tempFndExtLocation)
  val managedTableDetails : FinalManagedTable = FinalManagedTable(coreHistSchema, coreHistTableName,
    foundationPartitionColumnNames)
  val HIVE_DRIVER_CLASS: String = MarketingConstants.APP_JDBCHIVEDRIVER
}



/**
 * Read messages from delta and merge with core history
 */

object HiveJdbcWriter extends LoggingTrait {
  @deprecated(message = "Switch to external table and use HiveExternalTableWriter.writeToExternalFndTable method instead", since = "mdf_dpp_common 3.0.1")
  def lndInsert(insdataDF: DataFrame,fileWriter: HiveJdbcWriter)(implicit sparkSession: SparkSession): Boolean = {

    logInfo(" File Writer Begins ")
    log.info("Rearrange columns in distinct Dataset")
    val tupleRearrange: (DataFrame,Long) = arrangeColumnOrder(insdataDF, fileWriter)
    val managedInsertOverwriteDF = tupleRearrange._1
    val managedInsertOverwriteDFcount = tupleRearrange._2

    log.info("Generate Hive Properties")
    val properties = generateHiveProperties(fileWriter)

    log.info("Set Pipeline Metrics to Final Insert Overwrite stage")
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.STAGE, MarketingConstants.FINALINSERTOVERWRITE)
    PipeLineMetadataService.setLongMetricValue (DPPMetricsConstants.CDSSTAGINGCOUNT,managedInsertOverwriteDFcount)

    log.info("Final step to Insert Overwrite partitions in the table using 2-step process")
    val corehistTableLoadStatus : Boolean = DataReadWriteHelper.
      writeToManagedTableWith2StepProcessByInsertOverwrite(
        managedInsertOverwriteDF,
        fileWriter.externalTableDetails,
        fileWriter.managedTableDetails,
        properties,
        fileWriter.HDFS_WRITE_RETRY_COUNT)
    log.info (s"Was final Insert Overwrite to partitions in table using 2-step process successful? Answer: $corehistTableLoadStatus")
    log.info(s"Setting the final row count $managedInsertOverwriteDFcount to Pipeline metrics")
    PipeLineMetadataService.setLongMetricValue (DPPMetricsConstants.CDSROWSCOUNT,managedInsertOverwriteDFcount)
    corehistTableLoadStatus
  }

  def arrangeColumnOrder(insdataDF: DataFrame,fileWriter: HiveJdbcWriter): (DataFrame,Long) = {
    log.info("Rearrange columns in distinct Dataset")
    val managedInsertOverwriteDF = insdataDF.
      select (fileWriter.foundationInsertColumnNames.map (colname => col (colname)): _*)
    val managedInsertOverwriteDFcount = managedInsertOverwriteDF.count()
    (managedInsertOverwriteDF, managedInsertOverwriteDFcount)
  }

  def generateHiveProperties(fileWriter: HiveJdbcWriter): TableDetails.JDBCConfigDetails = {
    val properties = new Properties()
    properties.put("hive.execution.engine",fileWriter.appExecutionEngine)
    properties.put("tez.queue.name",fileWriter.appQueueName)
    properties.put("hive.exec.dynamic.partition",fileWriter.appDynamicPartition)
    properties.put("hive.exec.dynamic.partition.mode",fileWriter.appDynamicPartitionMode)

    TableDetails.
      JDBCConfigDetails(
        fileWriter.appJdbcUrl,
        fileWriter.HIVE_DRIVER_CLASS,
        properties)
  }

  def jdbcClient(jdbcUrl: String, queueName: String): Statement = {
    try {
      Class.forName(MarketingConstants.APP_JDBCHIVEDRIVER)
    }
    catch {
      case e: ClassNotFoundException =>
        log.error(e.getMessage, e.getCause)
        System.exit(1)
    }
    val connectionProperties = setJdbcConnectionProperties(queueName)
    val con = DriverManager.getConnection(jdbcUrl, connectionProperties)
    val stmt: Statement = con.createStatement
    stmt
  }

  def setJdbcConnectionProperties(queueName : String): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "")
    connectionProperties.setProperty("password", "")
    connectionProperties.setProperty("tez.queue.name", queueName)
    connectionProperties.setProperty("hive.tez.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat")
    connectionProperties.setProperty("mapreduce.job.reduces=", "50")
    connectionProperties
  }

  /**
   * Takes a List of List of partition paths, and issues a combined alter table statement to either drop or add (or both)
   * all partitions in a single statement. This should be leveraged over the singleton [[partitionHandler]] method, which was designed
   * for only handling a single partition at-a-time. Leverage the FileServiceHelper.retrievePartitions from Kelsa Core library
   * to generate the List of List of partition paths.
   * @param dbName The database name of the table.
   * @param tablename The table name.
   * @param jdbcclient The jdbc connection string.
   * @param droppartition Set to true to drop the partitions, false to only add the partitions.
   * @param partitions A List of List of partition locations. Use the FileServiceHelper.retrievePartitions to generate this.
   * @param destpathfordelete Only used when dropping partitions. The root path containing the partition folders to be dropped.
   * @param spark Implicit spark session
   * @return True if the partitions were added and (if specified) dropped. False if the partition changes failed.
   */
  def partitionHandler(dbName: String, tablename: String, jdbcclient: Statement,
                       droppartition: Boolean, partitions: List[List[String]], destpathfordelete: String)(implicit spark: SparkSession): Boolean = {

    val altercmd = s"""alter table $dbName.$tablename add IF NOT EXISTS ${stringBuilderPartitionList(partitions, CoreConstants.SPACE)}"""
    if (droppartition) {
      val droppartitioncmd = s"""alter table $dbName.$tablename drop IF EXISTS ${stringBuilderPartitionList(partitions, CoreConstants.COMMA)}"""
      log.info("Executing delete files from the path : " + destpathfordelete)
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(destpathfordelete), true)
      //log.info("Executing delete directory from the path : " + destpathfordelete)
      //FileUtil.fullyDelete(new File(destpathfordelete))
      //log.info("Executing chmod")
      //FileUtil.chmod(destpathfordelete,"755")
      log.info("Executing : " + droppartitioncmd)
      jdbcclient.execute(droppartitioncmd)
    }
    log.info("Executing : " + altercmd)
    jdbcclient.execute(altercmd)
  }

  /**
   * API for retrieving all physical partitions in a List of paths with the given separator.
   *
   * @param partitionList List of paths in HDFS of partitions. Leverage the FileServiceHelper.retrievePartitions method to generate.
   * @param partitionSeparator : ","-for dropping partitions [OR] " "- for adding partitions to hive meta-store using sql
   * @return String : used for updating meta-store
   * @example
   *         ''' Input path directory structure
   *         root
   *         - date=2020-07-01
   *         --- hour=1
   *         --- hour=2
   *         --- hour=3
   *         --- hour=4
   *         - date=2020-07-02
   *         --- hour=1
   *         '''
   *
   *         Sample function call :
   * {{{
   *
   *          val dropPartitions : Sting = HiveJdbcWriter.stringBuilderPartitionList("/user/NUID/BR3Path/",",")
   *          s"ALTER TABLE \${DatabaseName}.\${TableName} DROP IF EXISTS \${dropPartitions}"
   *
   *          val addPartitions : Sting = FileServiceHelper.constructPartitionsAsString("/user/NUID/BR3Path/"," ")
   *          s"ALTER TABLE \${DatabaseName}.\${TableName} ADD IF NOT EXISTS \${addPartitions}"
   *
   *          }}}
   *
   *         '''
   *         dropPartitions : PARTITION(date="2020-07-01",hour="1"),PARTITION(date="2020-07-01",hour="2"),PARTITION(date="2020-07-01",hour="3"),PARTITION(date="2020-07-01",hour="4"),PARTITION(date="2020-07-02",hour="1")
   *         addPartitions : PARTITION(date="2020-07-01",hour="1") PARTITION(date="2020-07-01",hour="2") PARTITION(date="2020-07-01",hour="3") PARTITION(date="2020-07-01",hour="4") PARTITION(date="2020-07-02",hour="1")
   *         '''
   */
  def stringBuilderPartitionList(partitionList: List[List[String]], partitionSeparator: String = CoreConstants.SPACE): String = {

    Try {
      ServiceConstants.PARTITION + CoreConstants.OPEN_BRACKET + partitionList
        .map(_partitionList => {
          _partitionList
            .map(
              partitionName =>
                partitionName
                  .split(CoreConstants.EQUAL)
                  .head + CoreConstants.EQUAL + CoreConstants.QUOTES + partitionName
                  .split(CoreConstants.EQUAL)
                  .last + CoreConstants.QUOTES
            )
            .reduce(_ + CoreConstants.COMMA + _)
        })
        .reduce(
          _ + CoreConstants.CLOSE_BRACKET + partitionSeparator + ServiceConstants.PARTITION + CoreConstants.OPEN_BRACKET + _
        ) + CoreConstants.CLOSE_BRACKET
    } match {
      case Failure(exception) =>
        logError(
          s"Exception while deriving partitions in the path : $exception"
        )
        CoreConstants.EMPTYSTRING
      case Success(value) =>
        value match {
          case ServiceConstants.DEFAULT_PARTITION =>
            CoreConstants.EMPTYSTRING
          case partitionValue =>
            partitionValue
        }
    }
  }
}
