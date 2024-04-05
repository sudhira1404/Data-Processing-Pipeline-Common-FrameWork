package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.service.DataReadWriteHelper
import com.tgt.dsc.kelsa.datapipeline.core.types.HiveTableReadConfigDetails
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.exceptions.DateSequenceException
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.{getLatestByPartitionDateArray, getNonPartitionColumns}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util.Properties
import scala.util.{Failure, Success, Try}

final case class BuildAllPartitionsFromEarliestPartitionRead(
                                        columnNames: Seq[String],
                                        partitionColumn: String,
                                        fndDb: String,
                                        fndTable: String,
                                        fndTimestamp: String,
                                        kafkaTimestamp: String,
                                        env: String,
                                        queueName: String,
                                        fndSchema: StructType,
                                        tempFndTablePath: String,
                                        jdbcUrl: String,
                                        hiveEngine: String,
                                        retryCount: Int = 3,
                                        testFoundationMockupFile: String = ""
                                      )
object BuildAllPartitionsFromEarliestPartitionRead extends LoggingTrait {

  def initializeStartingDataFrame(dates: Seq[String], initialDF: DataFrame, earliestPartitionRead: BuildAllPartitionsFromEarliestPartitionRead)
                                 (implicit sparkSession: SparkSession): DataFrame = {
    var startingFoundationDF: DataFrame = sparkSession.emptyDataFrame
    val numberOfDates: Int = dates.length
    var currentDate: Int = 1
    dates.foreach(d => {
      if (currentDate <= numberOfDates) {
        Try(readEarliestPartitionTableToDataFrame(d, earliestPartitionRead)(sparkSession)) match {
          case Success(initialDF) =>
            startingFoundationDF = initialDF
            currentDate = numberOfDates + 1
          case Failure(exception) =>
            if (currentDate < numberOfDates) {
              log.warn(s"Exception : $exception")
              log.warn("There are still one or more future dates to try, will try the next date")
              currentDate += 1
            } else {
              log.error(s"Exception : $exception")
              log.error("There are no dates left to try retrieving historical foundation data for." +
                "Will build the earliest partition from the source DataFrame only.")
              startingFoundationDF = getEarliestPartitionFromDataFrame(initialDF, d, earliestPartitionRead.columnNames,
                earliestPartitionRead.partitionColumn, earliestPartitionRead.kafkaTimestamp, earliestPartitionRead.fndTimestamp)(sparkSession)
            }
        }
      }
    })
    startingFoundationDF
  }

  /**
   * Retrieve only the records from the foundation table where the partition column date value matches
   * the date passed in the minFoundationDate parameter. In local testing
   * this would instead return the records from the minFoundationDate value found in the hive_load_date field within
   * the kafkaInputDF DataFrame.
   * @param minFoundationDate This should be the first date to use when querying for the data in yyyy-MM-dd format.
   * @param earliestPartitionRead     Initialized ReadFromTableEarliestUnionIncremental object.
   * @param tableIsExternal If true, the foundation table is external, and can be read using spark.sql. If false, the table
   *                        is managed, and will be read using Kelsa 2-Step Read method. Defaults to true.
   * @param sparkSession Implicit spark session.
   * @return A new DataFrame containing the records for only one partition date
   */
  def readEarliestPartitionTableToDataFrame(minFoundationDate: String, earliestPartitionRead: BuildAllPartitionsFromEarliestPartitionRead,
                                            tableIsExternal: Boolean = true)
                                           (implicit sparkSession: SparkSession): DataFrame = {
    val inputQuery = generateEarliestPartitionQuery(minFoundationDate, earliestPartitionRead.columnNames,
      earliestPartitionRead.partitionColumn, earliestPartitionRead.fndDb, earliestPartitionRead.fndTable,
      earliestPartitionRead.fndTimestamp, earliestPartitionRead.kafkaTimestamp)

    if (earliestPartitionRead.env != "local") {
      log.info("Read from input FND table for prev day data")

      if (tableIsExternal) {
        sparkSession.sql(inputQuery)
      } else {
        val properties: Properties = new Properties()
        properties.put("tez.queue.name", earliestPartitionRead.queueName)
        properties.put("hive.execution.engine", earliestPartitionRead.hiveEngine)

        val hiveTableReadConfigDetails = new HiveTableReadConfigDetails(inputQuery, earliestPartitionRead.tempFndTablePath,
          earliestPartitionRead.jdbcUrl, properties)
        val readFromHiveTableDF = DataReadWriteHelper.readFromHiveManagedTableWith2StepProcess(hiveTableReadConfigDetails,
          earliestPartitionRead.retryCount)

        if (readFromHiveTableDF.count() == 0) {
          throw DateSequenceException(s"No data returned for date specified: $minFoundationDate")
        } else {
          log.info(s"Data returned successfully from foundation for date specified: $minFoundationDate")
        }
        readFromHiveTableDF
      }
    }
    else {
      log.debug(s"The Hive query to use against foundation would have been: $inputQuery")
      log.debug(s"Local testing, with empty ephemeral data, will load from test orc file : ${earliestPartitionRead.testFoundationMockupFile}")
      sparkSession.read.schema(earliestPartitionRead.fndSchema).orc(earliestPartitionRead.testFoundationMockupFile)
    }
  }

  /**
   * Generates the sql statement which will be used to retrieve a single day of data. This takes the date passed in
   * the minFoundationDate in the generated where clause to filter against the partition column hive_load_date. However,
   * it generates a new value in the returned data for hive_load_date which is equivalent to the minFoundationDate value + 1 day.
   * So for example, if the minFoundationDate is 2022-08-16, it retrieves the data from the partition column hive_load_date='2022-08-16'
   * but returns a new column `date_add(hive_load_date,1) as hive_load_date`. The other derived column it adds to the
   * resulting data is `hive_load_timestamp`, which is the same data from the `updated_utc_ts` column. The `hive_load_timestamp` is used
   * further downstream in the windowed aggregate deduping logic between foundation and incremental Kafka data.
   *
   * @param minFoundationDate The date in 'yyyy-MM-dd' format used to filter data from foundation on.
   * @return A Hive sql statement
   */
  def generateEarliestPartitionQuery(minFoundationDate: String, columnNames: Seq[String], partitionColumn: String,
                                     fndDb: String, fndTable: String, fndTimestamp: String, kafkaTimestamp: String): String = {
    val nonPartitionColumns: String = getNonPartitionColumns(columnNames, partitionColumn)
    log.debug(s"Foundation columns which are not part of the partition: ${nonPartitionColumns}")

    val sqlStatement: String = s"select ${nonPartitionColumns}, " +
      s"date_add(${partitionColumn}, 1) as ${partitionColumn}, " +
      s"${fndTimestamp} as ${kafkaTimestamp} " +
      s"from ${fndDb}.${fndTable} " +
      s"""where ${partitionColumn} = "${minFoundationDate}""""
    log.info(s"Generated sql statement to retrieve historical partitions from Foundation: $sqlStatement")
    sqlStatement
  }

  /**
   * This retrieves the records from a DataFrame matching the specified date. By default this method is used to mock up
   * test DataFrames for local testing, in which the partitionDate value being passed in is from a prior date (eg. 2022-08-15)
   * but the DataFrame itself only has data greater than that date, so the dayLater value is defaulted to true. When this
   * is used for a production/staging run, meaning as part of the loop through the merged DataFrame by date, it should
   * instead have the partitionDate equal the actual date it needs to retrieve from the DataFrame, as well as set the dayLater = false.
   * That would cause the query to get only the data from the DataFrame for that date specified in the partitionDate, instead
   * of the data for the next day from the partitionDate.
   *
   * @param inputDF       The DataFrame having records that span at least one or more hive_load_date dates (eg. records from 2022-08-15 and 2022-08-16)
   * @param partitionDate The date in `yyyy-MM-dd` format which will be used to query only the records with the hive_load_date matching
   * @param dayLater      Defaults to true. When true, this changes the partitionDate to be one day later than specified. Set to false to use the partitionDate as is.
   * @param sparkSession  Implicit spark session.
   * @return A new DataFrame having records from the inputDF which match the filter criteria.
   */
  def getEarliestPartitionFromDataFrame(inputDF: DataFrame, partitionDate: String, columnNames: Seq[String], partitionColumn: String,
                                        kafkaTimestampColumn: String, foundationTimestampColumn: String,
                                        dayLater: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {
    inputDF.createOrReplaceTempView("kafkaDF")
    val selectStatement: String = s"select ${columnNames.mkString(",")} from kafkaDF "
    val whereStatement: String = if (dayLater) {
      s"where ${partitionColumn}=date_add(to_date('$partitionDate'),1)"
    } else {
      s"where ${partitionColumn}='$partitionDate'"
    }

    val earliestPartitionDF: DataFrame = sparkSession.sql(s"$selectStatement $whereStatement")

    earliestPartitionDF.withColumn(kafkaTimestampColumn, col(foundationTimestampColumn))
  }

  def buildAllPartitionsFromEarliestPartition(dates: Seq[String], earliestDate: String, initialDF: DataFrame,
                                              columnNames: Seq[String], partitionColumn: String, timestampColumn: String,
                                              env: String, primaryKeyColumns: Seq[String], windowingStrategy: String)
                                             (implicit sparkSession: SparkSession): DataFrame = {
    val nonPartitionColumns: String = getNonPartitionColumns(columnNames, partitionColumn)
    //First remove the earliest date from the sequence as there won't be any data in the DataFrame matching it
    val removeFirst: Seq[String] = dates.filterNot(d => d.equals(earliestDate))
    val finalDates: Seq[String] = if (env == "local") {
      log.debug(s"Local testing, will only build at max 2 dates out of ${dates.length} dates passed in the original ")
      removeFirst.take(2)
    } else {
      removeFirst
    }

    log.debug(s"Dates to build from earliest partition : ${finalDates.mkString(",")}")

    val dateRange: Seq[Int] = 1 to finalDates.length

    val checkpointDF: DataFrame = initialDF.checkpoint()
    val initialViewName = "initialDF"
    checkpointDF.createOrReplaceTempView(initialViewName)

    var currentDate: Int = 1

    //Loop through all remaining dates and generate sliced DataFrames from the initial DataFrame for each day. On the
    //first day (slice) don't union with any other day. Then each following day, union that current day slice with the
    //day prior slice by generating a temp view and DataFrame that can uniquely identify to the sparkSession.sql a view
    //corresponding to each day.
    finalDates.foreach(d => {
      val statement: String = s"select * from $initialViewName where ${partitionColumn}='$d'"
      val viewName: String = s"slice_$currentDate"
      val priorViewName: String = s"slice_${currentDate - 1}"
      log.debug(s"Generating slice $currentDate view from sql: $statement")

      val mergedDF: DataFrame = if (currentDate.equals(1)) {
        sparkSession.sql(statement)
      } else {
        val thisSlice: DataFrame = sparkSession.sql(statement)
        val priorStatement: String = s"select $nonPartitionColumns, " +
          s"date_add(${partitionColumn},1) as ${partitionColumn}, " +
          s"${timestampColumn} from $priorViewName"
        log.debug(s"Now merging this slice with prior slice from sql: $priorStatement")

        getLatestByPartitionDateArray(
          thisSlice.union(sparkSession.sql(priorStatement)),
          primaryKeyColumns,
          timestampColumn,
          windowingStrategy)
      }
      mergedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      mergedDF.createOrReplaceTempView(viewName)
      if (env == "local") {
        log.debug(s"count of slice $currentDate:")
        sparkSession.sql(s"select count(*) from $viewName").show()
      }
      currentDate += 1
    })

    //Now that there are temp views for every day slice, bring them all back together in one final DataFrame
    val selectStatement: String = "select * from slice_" + dateRange.mkString(" union select * from slice_")
    log.debug(s"Final select statement for all temp view slices: $selectStatement")

    val allSlices: DataFrame = sparkSession.sql(selectStatement)
    allSlices

  }



}
