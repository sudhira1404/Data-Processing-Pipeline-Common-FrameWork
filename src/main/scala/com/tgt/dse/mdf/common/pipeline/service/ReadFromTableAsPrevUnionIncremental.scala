package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.{getDistinctInputDates, getLatestByPartitionWithOneOrTwoOrderByCols, minMax, readInputTableToDFWrapper, twoStepFndCountRead}
import com.tgt.dse.mdf.common.pipeline.validation.DataFrameValidatorService
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.text.SimpleDateFormat


final case class ReadFromTableAsPrevUnionIncremental(
                                                      pipeLineConfig: PipeLineConfig,
                                                      incrementalRightDF: DataFrame,
                                                      tempFNDReadLocation:String="",
                                                      joinCondition:String="",
                                                      inputHiveQuery:String="",
                                                      selectColumns: Seq[String] = Seq.empty[String],
                                                      fndPartitionDateColName:String = "",
                                                      fndKeyColName:Seq[String] = Seq.empty[String],
                                                      fndOrderByColName:String = "",
                                                      fndSelectColumns:String="",
                                                      snapShot_F:Boolean=false,
                                                      adhocSnapShotBuildFromEarliestPartitionInFND_F:Boolean=false,
                                                      snapShotFndKeyColName:Seq[String] = Seq.empty[String],
                                                      snapShotFndOrderByColName:String = "",
                                                      snapShotWindowingStrategy:String = "row_number",
                                                      snapShotHandleMissingDates:Boolean=false,
                                                      snapShotDebugDFToLog:Boolean=false,
                                                      tableIsExternal:Boolean=true,
                                                      atomicTableName:String=""
                                                    )


object ReadFromTableAsPrevUnionIncremental extends LoggingTrait {

  /**
   * Reads from hive fnd table for previous day and union with incremental and get distinct records and
   * if snapShot_F is set, will built snapshot
   *         ReadFromTableAsPrevUnionIncremental case class holds below parameters
   *         pipeLineConfig:          PipeLine config object
   *         incrementalRightDF:      Dataframe corresponding to delta or incremental
   *         tempFNDReadLocation      Defaulted to emtpy.temp hdfs location for 2 step read if tableIsExternal is set to false.
   *         joinCondition:           Defaulted to empty string.If empty then join to previous day dataset
   *                                  (got from fnd table,refer inputHiveQuery parameter to generate previous day dataset)
   *                                  and incremental is ignored and it just union previous and incremental dataset
   *                                  If not empty,If join column name is same in left(previous from fnd table) and right DF(incremental DF) then
   *                                  right DF columns should be prefixed with right_ like
   *                                  val joinCondition = "id = right_id and date = right_date and group_id=campaign_id" and gets all records
   *                                  from previous not in incremental and then union previous and incremental dataset.
   *                                  This option could be used instead of removing dups based on fndKeyColName and fndOrderByColName using row_number.
   *         inputHiveQuery:          Defaulted to empty string.This is used to generate the previous day dataset from Fnd table.
   *                                  If empty string,then will get distinct partition date from incremental DF and
   *                                  will generate hive query with distinct partition date from incremental DF in filter clause to be
   *                                  executed on fnd table.partitionDateColName parameter needs to set if this parameter is empty.
   *         selectColumns:           Defaulted to empty.Will select these columns from incremental as well generated previous day DF
   *         fndPartitionDateColName: Defaulted to empty.Used when inputHiveQuery parameter is empty.
   *         fndKeyColName:           Defaulted to empty.If specified then after getting the final dataset
   *                                  (record in previous day dataset not in incremental and union with incremental to get final dataset)
   *                                  will remove dups based on the FndKeyColName ordered by FndOrderByColName in desc
   *                                  and picks latest by using row_number.
   *                                  This option could be used when all columns in table make record unique and
   *                                  instead of providing all columns to join prev and incremental in joincondition parameter,
   *                                  could use this option
   *        fndOrderByColName:        Defaulted to empty.Used along with FndKeyColName.Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   *        fndSelectColumns:         Defaulted to empty string.If set then if inputHiveQuery is empty and then
   *                                  query to be executed on the FND table will be build with these columns else will use "*" to fetch all.
   *        snapShot_F:               Defaulted to false.If set to true will get min date available in incrementalRightDF
   *                                  and gets latest partition in fnd table which is less than or equal to this date
   *                                  and Builds fnd query else and inputHiveQuery is set to empty string
   *                                  will get all records from fnd table based on distinct dates available in incremental
   *       adhocSnapShotBuildFromEarliestPartitionInFND_F:Defaulted to false.Used along with snapShot_F. If set to true and snapShot_F=true
   *                                                      will get min date available in incrementalRightDF and
   *                                                      gets all the partition in fnd table which is greater than or equal to this date
   *                                                      and Builds fnd query.
   *                                                      May be helpful if there are older partitions in the ephemeral(incremental run) which needs to be build
   *       snapShotFndKeyColName:     Defaulted to empty.Sequence of partition column name.Defaulted to empty.If set then removes dups based on this
   *       snapShotFndOrderByColName: Defaulted to empty.order by column name.Defaulted to empty.If set then removes dups based on this.Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   *       snapShotWindowingStrategy  Defaulted to row_number.possible values row_number/dense_rank/rank
   *       snapShotHandleMissingDates:Defaulted to false.If set, generates date range based on min and max from sourceDF
   *                                  and snapshot is built for all these dates from the previous partition
   *       snapShotDebugDFToLog:      Defaulted to false.If set will output Distinct Partition Dates for which snapshot needs to be built
   *                                  and shows sample Snapshot dataset before removing Dups by partitionColName like below.
   *       tableIsExternal:           Defaulted to true.If set then foundation table is external else its a managed table
   *       atomicTableName            Defaulted to empty string.If set then will build query based on atomic table and not fnd table.Tablename and Schema name fetched
   *       from MarketingConstants(APP_LNDSCHEMA,APP_FNDSCHEMA,APP_coreHistTable)

   * @return Dataframe
   */


  /**
   * Reads from hive fnd table and joins with delta and get distinct records and
   * if snapShot_F is set, will built snapshot
   * @param readFromTableAsPrevUnionIncremental case class holds parameters
   * @return Dataframe
   */

  def readFromTableAsPrevJoinIncremental(readFromTableAsPrevUnionIncremental:ReadFromTableAsPrevUnionIncremental
                                        )(implicit spark: SparkSession):DataFrame = {

    //add schema check if possible between fnd and source
    val appEnv: String = readFromTableAsPrevUnionIncremental.pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    val appQueueName: String = readFromTableAsPrevUnionIncremental.pipeLineConfig.config.getString(MarketingConstants.APP_QUEUENAME)
    val appJdbcUrl: String = readFromTableAsPrevUnionIncremental.pipeLineConfig.config.getString(MarketingConstants.APP_JDBCURL)
    val inputHiveQuery = if (readFromTableAsPrevUnionIncremental.inputHiveQuery == "") {
      buildFndQueryFromDistinctPartitionDateFromDelta(readFromTableAsPrevUnionIncremental.incrementalRightDF,
        readFromTableAsPrevUnionIncremental.fndPartitionDateColName,
        readFromTableAsPrevUnionIncremental.snapShot_F,
        readFromTableAsPrevUnionIncremental.pipeLineConfig,
        readFromTableAsPrevUnionIncremental.fndSelectColumns,
        readFromTableAsPrevUnionIncremental.adhocSnapShotBuildFromEarliestPartitionInFND_F,
        readFromTableAsPrevUnionIncremental.atomicTableName
      )
    } else
    {
      readFromTableAsPrevUnionIncremental.inputHiveQuery
    }

    //Need to check count from fnd table for the input query.
    //If count is zero then 2 step read can't infer schema for ORC.
    //So if count is zero skip 2 step and create empty DF from incremental
    val prevLeftDF =if (appEnv != "local"  && twoStepFndCountRead(inputHiveQuery,
      readFromTableAsPrevUnionIncremental.tempFNDReadLocation,
      appEnv,appQueueName,appJdbcUrl,readFromTableAsPrevUnionIncremental.tableIsExternal) > 0) {
      val appQueueName: String = readFromTableAsPrevUnionIncremental.pipeLineConfig.config.getString(MarketingConstants.APP_QUEUENAME)
      val appJdbcUrl: String = readFromTableAsPrevUnionIncremental.pipeLineConfig.config.getString(MarketingConstants.APP_JDBCURL)
      readInputTableToDFWrapper(
        inputHiveQuery,
        appQueueName,
        appJdbcUrl,
        3,
        readFromTableAsPrevUnionIncremental.tempFNDReadLocation,
        readFromTableAsPrevUnionIncremental.tableIsExternal)
    } else {
      log.info(s"Skipping two step read process as fnd does not have data for the input query $inputHiveQuery")
      readFromTableAsPrevUnionIncremental.incrementalRightDF.limit(0)
    }
    log.info("Building previous from fnd table with Incremental")
    val joinPrevIncrementalToDF = joinPrevIncrementalToDataFrames(prevLeftDF,
      readFromTableAsPrevUnionIncremental.incrementalRightDF,
      readFromTableAsPrevUnionIncremental.joinCondition,
      readFromTableAsPrevUnionIncremental.selectColumns,
      readFromTableAsPrevUnionIncremental.fndKeyColName,
      readFromTableAsPrevUnionIncremental.fndOrderByColName
    )

    if (readFromTableAsPrevUnionIncremental.snapShot_F) {
      log.info("Building snapshot")
      generateSnapShot(joinPrevIncrementalToDF,
        readFromTableAsPrevUnionIncremental.fndPartitionDateColName,
        readFromTableAsPrevUnionIncremental.snapShotFndKeyColName,
        readFromTableAsPrevUnionIncremental.snapShotFndOrderByColName,
        readFromTableAsPrevUnionIncremental.snapShotWindowingStrategy,
        readFromTableAsPrevUnionIncremental.snapShotHandleMissingDates,
        readFromTableAsPrevUnionIncremental.snapShotDebugDFToLog)
    }
    else
    {
      joinPrevIncrementalToDF
    }

  }

  /**
   * Join previous day dataset from Fnd table based on inputQuery parameter with incremental dataset if join condition is not empty and
   * identifies record in previous day dataset not in incremental and union with incremental to get final dataset.
   * if join condition is empty then it unions previous day dataset with incremental without joining.
   * @param prevDayLeftDF       temp hdfs location for 2 step read
   * @param incrementalRightDF: input query to be executed
   * @param joinCondition       Defaulted to empty string.If empty then join to previous day dataset(got from fnd table)
   *                            and incremental is ignored and it just union previous and incremental dataset
   *                            If not empty,If join column name is same in left(previous from fnd table) and right DF(incremental DF) then
   *                            right DF columns should be prefixed with right_ like
   *                            val joinCondition = "id = right_id and date = right_date and group_id=campaign_id" and gets all records
   *                            from previous not in incremental and then union previous and incremental dataset.
   *                            This option could be used instead of removing dups based on fndKeyColName and fndOrderByColName using row_number.
   * @param selectColumns       Defaulted to empty.Columns to be selected from the merged or final dataframe else all columns
   * @param fndKeyColName       Defaulted to empty.If specified then after getting the final dataset
   *                            (record in previous day dataset not in incremental and union with incremental to get final dataset)
   *                            will remove dups based on the FndKeyColName ordered by FndOrderByColName in desc
   * @param fndOrderByColName   Defaulted to empty.Used along with FndKeyColName and picks latest by using row_number.
   *                            Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * @return Dataframe
   */

  def joinPrevIncrementalToDataFrames(prevDayLeftDF: DataFrame,incrementalRightDF: DataFrame,joinCondition:String="",
                                      selectColumns: Seq[String] = Seq.empty[String],fndKeyColName:Seq[String] = Seq.empty[String],
                                      fndOrderByColName:String = ""
                                     ):DataFrame = {

    val prevDayLeftSelectColsDF =if (selectColumns.nonEmpty) prevDayLeftDF.select(selectColumns.map(name => col(name)): _*) else prevDayLeftDF
    val incrementalRightSelectColsDF =if (selectColumns.nonEmpty) incrementalRightDF.select(selectColumns.map(name => col(name)): _*) else incrementalRightDF

    log.info("Incremental DF schema")
    incrementalRightSelectColsDF.printSchema()

    log.info("Prev Day from fnd table schema")
    prevDayLeftSelectColsDF.printSchema()

    val schemaComparisonResult = DataFrameValidatorService.schemaComparison(incrementalRightSelectColsDF,prevDayLeftSelectColsDF)
    log.info("Schema difference between incremental and Prev day DF from Fnd table=" + schemaComparisonResult._1)
    log.info("Schema difference between Prev day and incremental DF from Fnd table=" + schemaComparisonResult._2)

    log.info("PrevDayLeftSelectColsDFCount=" + prevDayLeftSelectColsDF.cache.count())
    log.info("IncrementalRightSelectColsDFCount=" + incrementalRightSelectColsDF.cache.count())

    val joinPrevIncrementalDF =  if (joinCondition !="") {
      val joinPrevIncrementalDFTuple = DataFrameUtils.joinTwoDataFrames(prevDayLeftSelectColsDF,
        incrementalRightSelectColsDF,
        "leftanti",
        joinCondition,
        prefixRightDFCommonCol = "right",
        selectColumns)
      val joinPrevIncrementalDFDropRightList = joinPrevIncrementalDFTuple._2
      log.info(joinPrevIncrementalDFDropRightList.mkString(","))
      joinPrevIncrementalDFTuple._1.drop(joinPrevIncrementalDFDropRightList: _*)
    } else {
      prevDayLeftSelectColsDF
    }
    //replace left table with prevDayLeftSelectColsDF in input sql
    //replace right table with incrementalRightSelectColsDF in input sql
    //execute this sql

    val mergeDF = joinPrevIncrementalDF.union(incrementalRightSelectColsDF)
    val mergeDFCount = mergeDF.cache.count()
    val mergeRemoveDupsDF = if (fndKeyColName.nonEmpty && fndOrderByColName !="" ) {

      //mergeDF.filter("sap_project_id ='C-002034-01-282'").show(25,false)
      //DataFrameUtils.getLatestByPartitionDateArray(mergeDF, fndKeyColName, fndOrderByColName, windowingStrategy = "row_number")
      val removeDupsDF = getLatestByPartitionWithOneOrTwoOrderByCols(mergeDF, fndKeyColName, fndOrderByColName, windowingStrategy = "row_number","ReadFromTableAsPrevUnionIncremental in joinPrevIncrementalToDataFrames method error...Provide one or two orderByColName")

      //removeDupsDF.filter("sap_project_id ='C-002034-01-282'").show(25,false)

      removeDupsDF
    } else
    {
      mergeDF
    }

    log.info(s"Merge PrevDayDF(union of Incremental and Previous days data not in incremental from Fnd table)rowcount : $mergeDFCount")

    mergeRemoveDupsDF
  }

  /**
   * Builds foundation query based on distinct partition dates available in source/incremental dataset
   * select * from fndschema.fnd table where partition_dates in (distinct partition dates from incrementalDF)
   * @param incrementalDF             Incremental/source dataset
   * @param partitionDateColName      Partition column name
   * @param snapShot_F:               Defaulted to false.If set to true will get min date available in incrementalRightDF
   *                                  and gets latest partition in fnd table which is less than or equal to this date
   *                                  and Builds fnd query else and inputHiveQuery is set to empty string
   *                                  will get all records from fnd table based on distinct dates available in incremental
   * @param pipeLineConfig:           PipeLine config object
   * @param fndSelectColumns:         Defaulted to empty string.If set then if inputHiveQuery is empty and then
   *                                  query to be executed on the FND table will be build with these columns else will use "*" to fetch all.
   * @param adhocSnapShotBuildFromEarliestPartitionInFND_F:Defaulted to false.Used along with snapShot_F. If set to true and snapShot_F=true
   *                                                      will get min date available in incrementalRightDF and
   *                                                      gets all the partition in fnd table which is greater than or equal to this date
   *                                                      and Builds fnd query.
   * @param atomicTableName           Defaulted to empty string.If set then will build query based on atomic table
   * @return hive query
   */

  def buildFndQueryFromDistinctPartitionDateFromDelta(incrementalDF: DataFrame,
                                                      partitionDateColName:String,
                                                      snapShot_F:Boolean=false,
                                                      pipeLineConfig: PipeLineConfig ,
                                                      fndSelectColumns:String="",
                                                      adhocSnapShotBuildFromEarliestPartitionInFND_F:Boolean=false,
                                                      atomicTableName:String=""
                                                     ): String = {

    val minMaxValue =  minMax(incrementalDF,partitionDateColName)
    val atomicDB: String = pipeLineConfig.config.getString(MarketingConstants.APP_LNDSCHEMA)
    val managedDB: String = if ( atomicTableName !="") atomicDB else pipeLineConfig.config.getString(MarketingConstants.APP_FNDSCHEMA)
    val managedTable: String = if ( atomicTableName !="") atomicTableName else  pipeLineConfig.config.getString(MarketingConstants.APP_coreHistTable)
    val fndSelectCols =  if (fndSelectColumns !="") fndSelectColumns else "*"
    val inputQuery =  if (snapShot_F) {
      val earliestFndDate = minMaxValue._1
      log.info(s"EarliestFndDate that will be used to fetch from fnd table $earliestFndDate")

      if (adhocSnapShotBuildFromEarliestPartitionInFND_F)
      {
        //s"select * from $managedDB.$managedTable where $partitionDateColName >=" + s""""$earliestFndDate""""
        s"select $fndSelectCols from $managedDB.$managedTable where $partitionDateColName >= " +
          s"(select max($partitionDateColName) from $managedDB.$managedTable where $partitionDateColName <=" + s""""$earliestFndDate"""" +")"
      }
      else
      {
        //s"select * from $managedDB.$managedTable where $partitionDateColName =" + s""""$earliestFndDate""""
        s"select $fndSelectCols from $managedDB.$managedTable where $partitionDateColName = " +
          s"(select max($partitionDateColName) from $managedDB.$managedTable where $partitionDateColName <=" + s""""$earliestFndDate"""" +")"

      }

    } else {
      val deltaDistinctDates = getDistinctInputDates(incrementalDF,partitionDateColName)
      s"select $fndSelectCols from $managedDB.$managedTable where $partitionDateColName in ($deltaDistinctDates)"
    }

    log.info("InputQuery that will executed against the Fnd table to get previous day's data " +
      s"which will be compared against incremental dataset is $inputQuery")

    inputQuery
  }

  /**
   * Builds snapshot from the previous partitions to the next one.
   * @param sourceDF            Dataset for which snapshot to be built
   * @param keyColName          Sequence of partition column name.Defaulted to empty.If set then removes dups based on this
   * @param orderByColName      order by column name.Defaulted to empty.If set then removes dups based on this.
   *                            Another column can be specified as comma separated to sort on 2 columns but not more than 2 cols
   * @param windowingStrategy   Defaulted to row_number.possible values row_number/dense_rank/rank
   * @param handleMissingDates  Boolean.If set, generates date range based on min and max from sourceDF
   *                            and snapshot is built for all these dates from the previous partition
   * @param debugDFToLog        defaulted to false.If set will output Distinct Partition Dates for which snapshot needs to be built
   *                            and shows sample Snapshot dataset before removing Dups by partitionColName like below.

   * @return Dataframe
   */

  def generateSnapShot(sourceDF: DataFrame,partitionColName:String,
                       keyColName:Seq[String]=Seq.empty[String],
                       orderByColName:String="",
                       windowingStrategy:String="row_number",
                       handleMissingDates:Boolean=false,
                       debugDFToLog:Boolean=false
                      )(implicit sparkSession: SparkSession):DataFrame = {

    import org.joda.time.{Days, LocalDate}
    import sparkSession.implicits._

    val distinctPartitionDatesSourceDF =if (handleMissingDates)
    {
      val minMax = sourceDF.select(partitionColName)
        .agg(functions.min(partitionColName).as("min"), functions.max(partitionColName).as("max"))

      val minDate = try {
        minMax.head().getDate(0)
      } catch {

        case _: ClassCastException => minMax.head().getString(0)
      }
      val MaxDate  = try {
        minMax.head().getDate(1)
      } catch {

        case _: ClassCastException => minMax.head().getString(1)
      }

      log.info("MinDate in the generateSnapShot input dataset =" + minDate)
      log.info("MaxDate in the generateSnapShot input dataset=" + MaxDate)

      val start = new LocalDate(minDate)
      val end = new LocalDate(MaxDate)
      val formatter = new SimpleDateFormat("yyyy-MM-dd")
      val numberOfDays = Days.daysBetween(start, end).getDays
      val days = (for (f <- 0 to numberOfDays)
        yield formatter.format(new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(start.plusDays(f).toDate.toString))).toList
      days.toDF(partitionColName)
    } else
    {
      sourceDF.select(partitionColName).distinct
    }

    if (debugDFToLog) {
      log.info("Distinct Partition Dates between the min and max in the DF with adding any missing dates " +
        "in between from the ephemeral (if handleMissingDates is set)" +
        " or just Distinct Partition Dates after merging Prev with Incremental")
      distinctPartitionDatesSourceDF.show(false)
    }

    val joinCondition = s"$partitionColName >= right_$partitionColName"
    val rightDF= sourceDF
    val leftDF= distinctPartitionDatesSourceDF
    log.info("Number of Partitions in the combined dataset(prev + incremental)=" + distinctPartitionDatesSourceDF.cache.count)
    val  snapshotDF =   if (distinctPartitionDatesSourceDF.count > 0) {
      DataFrameUtils.joinTwoDataFrames(leftDF,
        rightDF,
        "inner",
        joinCondition,
        prefixRightDFCommonCol = "right")._1
    }
    else

    {
      sourceDF
    }

    log.info("Snapshot Dataframe schema")
    snapshotDF.printSchema()

    if (debugDFToLog) {
      log.debug("Snapshot dataset before removing dups")
      snapshotDF.groupBy(partitionColName, s"right_$partitionColName").count().show(false)
    }

    getLatestByPartitionWithOneOrTwoOrderByCols(snapshotDF,keyColName, orderByColName, windowingStrategy,"ReadFromTableAsPrevUnionIncremental error...Provide one or two orderByColName")
  }

}