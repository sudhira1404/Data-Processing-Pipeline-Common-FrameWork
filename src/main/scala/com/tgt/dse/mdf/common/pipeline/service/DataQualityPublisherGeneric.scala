package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.constants.DPPMetricsConstants
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dsc.kelsa.datapipeline.core.transformations.TransFunctions.renameColumns
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.DataQualityException
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.iterateMap
import com.tgt.dse.mdf.common.pipeline.util.{DPPMetricUtils, DataFrameUtils, MarketingCommonUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util


final case class MetricsCheck(failedArrayLists: util.ArrayList[String], diffFailedArrayLists: util.ArrayList[String], tgtTotalCount: Long)

final case class DataQualityPublisherGeneric(
                                              pipelineConfig: PipeLineConfig,
                                              dqColumns: Seq[String],
                                              dupsCheckKey: Seq[String]= Seq.empty,
                                              nullsCheckKey: Seq[String]= Seq.empty,
                                              loggerName: String,
                                              reportingDateColumnFlag: Boolean,
                                              srcColumnrenameList: Seq[(String, String)] = Seq.empty,
                                              tgtColumnrenameList: Seq[(String, String)] = Seq.empty,
                                              scale: Int = 0,
                                              throwExceptionUponFailure: Boolean = true,
                                              nullCheckFlag: Boolean = true,
                                              dupCheckFlag: Boolean = true,
                                              partitionDateColName:String="report_d",
                                              targetTablename:String="",
                                              threshold:Double=0,
                                              thresholdPercent:Double=0,
                                              thresholdPercentJson:String="",
                                              ignoreCountCheck:Boolean=false,
                                              dqCheckResultShowToLog:Boolean=false)

object DataQualityPublisherGeneric extends LoggingTrait {
  /**
   * Peforms the DQ check(value check,dups check and null check) by comparing the source and target dataframes based on dq columns.
   *
   * @param publisher DataQualityPublisherGeneric case class which has below listed parameter objects
   *                  dqColumns:              It takes dq columns as seq for which dq check is performed and if there is any
   *                                          partition date column in the target dataframe it should be specified as the last column
   *                                          like val dqColumns = Seq("clickthrough_1","clickthrough_2","impression_1","impression_2","engagement_1",
   *                                          "engagement_2","spend_in_dollar","report_d")
   *                  dupsCheckKey:           dups column as seq for which dups check is performed.If we don't have dupsCheckKey,pass as empty string
   *                  nullsCheckKey:          null column as seq for which null check is performed.If we don't have nullCheckKey,pass as empty string
   *                  loggerName:             logger name as string
   *                  reportingDateColumnFlag:If there is any reporting date or partition date column in the target dataframe specify as true.
   *                                          Dq check will perform counts of all dq columns by max of this date
   *                  srcColumnrenameList:   Defaulted to empty.If source dataframe columns needs to be renamed then specify as seq[(string,string)]
   *                  tgtColumnrenameList:   Defaulted to empty.If target dataframe columns needs to be renamed then specify as seq[(string,string)]
   *                  scale:                 Defaulted to 0.While doing the sum of the dq columns,if scale is 0 then it rounds the metrics to zero else rounds*                                         to specified value and performs the source and target compare.
   *                                         If we cast decimal value of the target dataframe say decimal(38,8) and if this is passed as
   *                                         target dataframe to dq check and If scale is set as 8 then sometimes source and target counts for
   *                                         decimal dq columns may not completely match so try reducing the scale may be to 2 or 0 or
   *                                         don't cast decimal value in the execute pipeLine
   *                  throwExceptionUponFailure: Defaulted to true.Will throw exception if set to true
   *                  nullCheckFlag:          Defaulted to true.If set,then nullcheck is performed else skip.
   *                                          This is used along with nullCheckKey parameter
   *                  dupCheckFlag:           Defaulted to true.If set,then dupscheck is performed else skip.
   *                                          This is used along with dupsCheckKey parameter
   *                  partitionDateColName:  Defaulted to "report_d".This is used to get row counts in the target dataframe by
   *                                         partition date column and publish as tgtDates and tgtTotalCounts as key metrics by
   *                                         collapsing the value to single row.Used when reportingDateColumnFlag is set to true
   *                  targetTablename:       Defaulted to empty.string.Target table name.Will be published to grafana as targetTablename metric.If
   *                                         targetTablename is not empty and reportingDateColumnFlag is set to true then
   *                                         it will get row counts in the target dataframe by partition date column and publish
   *                                         tgtDates(flattened dates),tgtTotalCounts(flattened counts),tgtTableName to influx(large volume will impact the performance)
   *                  threshold:             Defaulted to 0.Will be applied to all DQ columns.Expressed as Double value.
   *                                         If set to 1 then if difference of sum value of dq columns between source and target
   *                                         is greater than 1 then result is set to fail.
   *                  thresholdPercent:      Defaulted to 0.Will be applied to all DQ columns.Expressed as % value(Double).
   *                                         If set to 1 then If difference between sum value of dq columns between source and target is less
   *                                         than 1%of source dq column value(or is within 1% of source)then will set flag metrics to pass else fail.
   *                                         If calculation((source dq column value-target dq column value)-threshold*source dq column value/100),
   *                                         yield's negative value then target is within the threshold.
   *                                         If calculation yield's positive number then target column value is not within the threshold.
   *                                         If sum of target column value is more than sum of source column value then target column value not within the threshold.
   *                 thresholdPercentJson:   Defaulted to empty string.Same as thresholdPercent but dq column values for applying the
   *                                         threshold check is expressed as key value pair.key is dq column and Value is the percentage.
   *                                         if some dq column is not mentioned in the threshold json ,it will defaulted to 0.
   *                                         like val dqThreshold = "{\"count\":\"2\",\"clicks\":\"5\",\"impressions\":\"5\",\"spend\":\"5\"}"
   *                 ignoreCountCheck:       Defaulted to false.If set ,will ignore the count check.
   *                                         If count is not matching b/t source and target,count match will still be set to pass in the final result.
   *                 dqCheckResultShowToLog: Defaulted to false.If set ,will show the output of dq check to logs.
   *                                         This will slow down the process for huge volumne.
   *                                         Results could also be exposed in grafana through influx metrics as well.

   * @return returns target dataframe after publishing the metrics
   *
   *         If dq columns are Seq("impressions","report_d") then below set are published to grafana.
   *         If there are more columns then similar set of Metrics will be published
   *         source dataframe metrics:srcTotalImpressions|srcDate|srcTotalCount|
   *         target dataframe metrics:tgtTotalImpressions|tgtDate |tgtTotalCount|
   *         difference metrics with pass or fail:srcTgtDiffImpressionsFlag|srcTgtDiffCountFlag|srcTgtDiffImpressions|srcTgtDiffCount|
   *         nullcheck metrics:nullRowsCountFlag|nullRowsCount|
   *         dupcheck metrics:duplicateRowsCountFlag|DUPLICATEDROWSCOUNT|
   *         threshold metrics:thresholdCount|thresholdImpressions| etc based on DQ columns
   *         Nulldetails metric:Null error message
   *         Dupsdetails metric:Dups error message
   *         details metric:Overall pass or fail message
   *         CDSROWSCOUNT metric:total target count
   *       if targetTablename is set to schema.tablename and reportingDateColumnFlag is true
   *          tgtDates metric:flattened partitioned dates that are loaded),
   *          tgtTotalCounts metric:flattened partitioned dates counts that are loaded)
   *          tgtMinMax metric:min max of partitioned dates that are loaded )
   *          tgtSum metric:totalcounts of records loaded)
   *          tgtDfPartitionCount metric:total number of partitions that are loaded)

   *       If source dataframe is empty then dq check will skip and return the target dataframe
   */

  def dqCheck(sourceDataFrame: DataFrame, targetDataFrame: DataFrame,
              publisher: DataQualityPublisherGeneric)
             (implicit spark: SparkSession): DataFrame = {

    if (sourceDataFrame.head(1).isEmpty) {
      log.info("sourceDataFrame is empty,will skip the dq check")
      targetDataFrame
    }
    else {
      countByPartitionDateAndCollectPushGrafana(targetDataFrame, publisher)
      log.info("starting to check value and count diff for the dq columns")
      val valueCntCheckDF = valueCntCheck(sourceDataFrame, targetDataFrame, publisher: DataQualityPublisherGeneric)
      log.info("starting to check null and dup values for the nulls/dups CheckKey columns")
      val finalDF: DataFrame = nullDupCheck(targetDataFrame, publisher, valueCntCheckDF)
      log.info("starting to publish metrics")
      parseDataframeResult(publisher, finalDF)

      finalDF
    }
  }

  private def countByPartitionDateAndCollectPushGrafana(targetDataFrame: DataFrame, publisher: DataQualityPublisherGeneric) = {

    if (publisher.reportingDateColumnFlag && publisher.targetTablename.nonEmpty) {
      log.info("get row counts in the target dataframe by partition date column and publish")
      val tgtDfPartitionDateCount = DataFrameUtils.countByPartitionDateAndCollect(targetDataFrame, publisher.partitionDateColName)
      val tgtDfPartitionDate = tgtDfPartitionDateCount._1
      val tgtDfCount = tgtDfPartitionDateCount._2
      val tgtDfMinMax = tgtDfPartitionDateCount._3
      val tgtDfSum = tgtDfPartitionDateCount._4
      val tgtDfPartitionCount = tgtDfPartitionDateCount._5

      PipeLineMetadataService.setStringMetricValue("tgtDates", tgtDfPartitionDate, influxField = true)
      PipeLineMetadataService.setStringMetricValue("tgtTotalCounts", tgtDfCount, influxField = true)
      PipeLineMetadataService.setStringMetricValue("tgtTableName", publisher.targetTablename, influxField = true)
      PipeLineMetadataService.setStringMetricValue("tgtMinMax", tgtDfMinMax, influxField = true)
      PipeLineMetadataService.setStringMetricValue("tgtSum", tgtDfSum, influxField = true)
      PipeLineMetadataService.setStringMetricValue("tgtDfPartitionCount", tgtDfPartitionCount, influxField = true)

    }
  }

  private def parseDataframeResult(publisher: DataQualityPublisherGeneric, dupCheckDF: DataFrame) = {
    log.info("columns to check for nulls")
    log.info(publisher.nullsCheckKey.mkString(","))
    log.info("columns to check for dups")
    log.info(publisher.dupsCheckKey.mkString(","))
    log.info("------------------------------------")
    log.info("dq columns to check for value match")
    log.info(publisher.dqColumns.mkString(","))
    log.info("------------------------------------")
    log.info("rename target columns to match source columns for comparisons")
    log.info(publisher.tgtColumnrenameList.mkString(","))
    log.info("------------------------------------")
    if (publisher.dqCheckResultShowToLog) {
      log.info("final output dataset with result")
      dupCheckDF.show(false)
    }
    generateMetricAndPublish(dupCheckDF,publisher,true)
  }

  private def nullDupCheck(targetDataFrame: DataFrame, publisher: DataQualityPublisherGeneric, valueCntCheckDF: DataFrame) = {

    log.info("starting to check null values for the nullCheckKey columns")
    val nullCheckDF = if (publisher.nullCheckFlag) {
      nullCheck(valueCntCheckDF, targetDataFrame, publisher: DataQualityPublisherGeneric)
    } else {
      log.info("ignoring nullsCheck")
      valueCntCheckDF
    }
    log.info("starting to check dups values for the dupsCheckKey columns")
    val dupCheckDF = if (publisher.dupCheckFlag) {
      dupCheck(nullCheckDF, targetDataFrame, publisher: DataQualityPublisherGeneric)
    } else {
      log.info("ignoring dupsCheck")
      nullCheckDF
    }
    dupCheckDF
  }

  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.TraversableLast"
  ))
  private def valueCntCheck(sourceDataFrame: DataFrame,
                            targetDataFrame: DataFrame,
                            publisher: DataQualityPublisherGeneric
                           )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    val srcRenameDF = if (publisher.srcColumnrenameList.nonEmpty) renameColumns(sourceDataFrame, publisher.srcColumnrenameList) else sourceDataFrame
    val tgtRenameDF = if (publisher.tgtColumnrenameList.nonEmpty) renameColumns(targetDataFrame, publisher.tgtColumnrenameList) else targetDataFrame
    val srcDF: DataFrame = srcRenameDF.select(publisher.dqColumns.map(m => col(m)): _*).withColumn("dummy", lit(1))
    val tgtDF: DataFrame = tgtRenameDF.select(publisher.dqColumns.map(m => col(m)): _*).withColumn("dummy", lit(1))
    val aggSumColumns = publisher.dqColumns.map { c =>
      if (publisher.reportingDateColumnFlag && c == publisher.dqColumns.last) {
        max(c).alias(c)
      } else {
        round(sum(c), if (publisher.scale > 0) {
          publisher.scale
        } else {
          0
        }).alias(c)
      }
    }
    log.info("aggSumColumns columns")
    log.info(aggSumColumns.mkString(","))
    log.info("------------------------------------")
    val srcAggSumDF = srcDF.groupBy("dummy").agg(lit("placeholder"), aggSumColumns: _*).drop("placeholder").alias("srcAggSumDf")
    val srcAggCntDF = srcDF.groupBy("dummy").agg(count("*").alias("Count")).alias("srcAggCntDF")
    val srcAggSumCntDF1 = srcAggSumDF.join(srcAggCntDF, col("srcAggSumDf.dummy") === col("srcAggCntDF.dummy"), "inner").drop("dummy").
      drop("placeholder").alias("srcAggSumCntDf")
    val tgtAggSumDF = tgtDF.groupBy("dummy").agg(lit("placeholder"), aggSumColumns: _*).drop("placeholder").alias("tgtAggSumDf")
    val tgtAggCntDF = tgtDF.groupBy("dummy").agg(count("*").alias("Count")).alias("tgtAggCntDF")
    val tgtAggSumCntDF1 = tgtAggSumDF.join(tgtAggCntDF, col("tgtAggSumDf.dummy") === col("tgtAggCntDF.dummy"), "inner").drop("dummy").
      drop("placeholder").alias("tgtAggSumCntDf")
    srcAggSumDF.persist(StorageLevel.MEMORY_AND_DISK)
    srcAggCntDF.persist(StorageLevel.MEMORY_AND_DISK)
    srcAggSumCntDF1.persist(StorageLevel.MEMORY_AND_DISK)
    tgtAggSumDF.persist(StorageLevel.MEMORY_AND_DISK)
    tgtAggCntDF.persist(StorageLevel.MEMORY_AND_DISK)
    tgtAggSumCntDF1.persist(StorageLevel.MEMORY_AND_DISK)
    val columns1 = srcAggSumCntDF1.schema.fields.map(_.name)
    val columns = "Count" +: columns1.dropRight(1)
    val srcAggSumCntDF = srcAggSumCntDF1.select(columns.map(col): _*)
    val tgtAggSumCntDF = tgtAggSumCntDF1.select(columns.map(col): _*)
    srcAggSumCntDF.persist(StorageLevel.MEMORY_AND_DISK)
    tgtAggSumCntDF.persist(StorageLevel.MEMORY_AND_DISK)
    val diffArrayDataset = columns.map(col => srcAggSumCntDF.select(col).except(tgtAggSumCntDF.select(col)))
    val threshold = publisher.threshold
    val thresholdPercent = publisher.thresholdPercent
    val thresholdPercentJson = publisher.thresholdPercentJson
    //val diffArrayDataset1 = columns.map(col => srcAggSumCntDF.select( expr("col - (col*(threshold/100))")).except(tgtAggSumCntDF.select(col)))
    //val diffArrayDataset1 = columns.map(col => srcAggSumCntDF.select( expr("col - (col*(threshold/100))")).except(tgtAggSumCntDF.select(col)))
    val diffArray = diffArrayDataset.map(diff => {
      if (diff.count > threshold) diff.count else 0
    })
    val diffString = diffArray.mkString(",")
    val difflist = List(diffString)
    val diffArrays = difflist.map(_.split("\\,"))
    val maxCols = diffArrays.map(_.length).max
    val diffDF = diffArrays.toDF("arr")
      .select((0 until maxCols).map(i => $"arr"(i).as(s"col_$i")): _*)
    val diffOldColumn = diffDF.columns
    val dqDropColumns = if (publisher.reportingDateColumnFlag) publisher.dqColumns.dropRight(1) else publisher.dqColumns
    //val dqNewColumns = dqDropColumns :+ "Count"
    val dqNewColumns = "Count" +: dqDropColumns
    val diffNewColumnList = dqNewColumns.flatMap(i => Seq("srcTgtDiff" + i.capitalize + "Flag"))
    val diffRenameColumn = diffOldColumn.zip(diffNewColumnList).map(f => {
      col(f._1).as(f._2)
    })

    val diffRenameColumnDF = if (threshold > 0 || thresholdPercent > 0 || thresholdPercentJson != "") {
      thresholdCheck(srcAggSumCntDF,tgtAggSumCntDF,dqNewColumns ,publisher)
    }
    else {
      diffNewColumnList
        .foldLeft(diffDF.select(diffRenameColumn: _*))((diffNewColumnsAliasDF, column) => diffNewColumnsAliasDF.withColumn(column, when(col(column) === 0 || (col("srcTgtDiffCountFlag") > 0 && publisher.ignoreCountCheck), "pass")
          .otherwise(lit("fail"))
        )).withColumn("dummy", lit(1))
    }

    diffRenameColumnDF.persist(StorageLevel.MEMORY_AND_DISK)

    val srcTgtDiffDF = columnValueDiff(srcAggSumCntDF, tgtAggSumCntDF, dqNewColumns, publisher)
    val srcTgtDiffDummyDF = srcTgtDiffDF.withColumn("dummy", lit(1))
    srcTgtDiffDummyDF.persist(StorageLevel.MEMORY_AND_DISK)
    val srcRenameColumnDF = aliasColumn(srcAggSumCntDF, "srcTotal", shouldCapitalize = true, publisher)
      .drop("srcDummy").withColumn("dummy", lit(1)).alias("srcRenameColumnDF")
    srcRenameColumnDF.persist(StorageLevel.MEMORY_AND_DISK)
    val tgtRenameColumnDF = aliasColumn(tgtAggSumCntDF, "tgtTotal", shouldCapitalize = true, publisher).drop("tgtDummy")
      .withColumn("dummy", lit(1)).alias("tgtRenameColumnDF")
    tgtRenameColumnDF.persist(StorageLevel.MEMORY_AND_DISK)
    val valueCntCheckDF = srcRenameColumnDF.join(tgtRenameColumnDF, col("srcRenameColumnDF.dummy") === col("tgtRenameColumnDF.dummy"), "inner")
      .join(diffRenameColumnDF, srcRenameColumnDF("dummy") === diffRenameColumnDF("dummy"), "inner")
      .join(srcTgtDiffDummyDF, srcRenameColumnDF("dummy") === srcTgtDiffDummyDF("dummy"), "inner")
      .drop("dummy")

    valueCntCheckDF

  }

  private def thresholdCheck(sourceAggDataframe: DataFrame ,
                             targetAggDataframe: DataFrame,
                             dqNewColumns:Seq[String],
                             publisher: DataQualityPublisherGeneric
                            )(implicit spark: SparkSession): DataFrame = {

    //Will apply threshold check
    val diffDF: DataFrame = if (publisher.threshold > 0 || publisher.thresholdPercent > 0) {
      columnValueDiff(sourceAggDataframe, targetAggDataframe,dqNewColumns, publisher)
    } else {
      columnValueDiffThresholdPerJson(sourceAggDataframe, targetAggDataframe,dqNewColumns, publisher)
    }
    val diffAliasDF = aliasColumnSuffix(diffDF)
    val diffAliasDeriveDF = deriveSelectExprForThreshold(diffAliasDF, publisher)
    val diffAliasDeriveDFCols = diffAliasDeriveDF.columns
    val diffAliasDeriveFlagDF = diffAliasDeriveDFCols
      .foldLeft(diffAliasDeriveDF.select("*"))((diffNewColumnsAliasDF, column) => diffNewColumnsAliasDF.withColumn(column, when(col(column) === 0 ||(col("srcTgtDiffCountFlag") > 0 && publisher.ignoreCountCheck), "pass")
        .otherwise(lit("fail"))
      )).withColumn("dummy", lit(1))
    diffAliasDeriveFlagDF.persist(StorageLevel.MEMORY_AND_DISK)
    log.info("DQ check result after applying threshold calculation")
    diffAliasDeriveFlagDF.show(false)

    diffAliasDeriveFlagDF

  }

  @SuppressWarnings(Array("UnusedMethodParameter"))
  private def columnValueDiff(aggSourceDataFrame: DataFrame,
                              aggTargetDataframe: DataFrame,
                              dqNewColumns: Seq[String],
                              publisher: DataQualityPublisherGeneric
                             ): DataFrame = {

    // This will calculate difference b/t source and target and applies threshold represented as double
    val srcInputDF: DataFrame = aggSourceDataFrame.select(dqNewColumns.map(m => col(m)): _*)
    val tgtInputDF: DataFrame = aggTargetDataframe.select(dqNewColumns.map(m => col(m)): _*)
    val srcInputColumn = srcInputDF.columns.map(srcInputDF(_))
    val tgtInputRenameColumnDF = aliasColumn(tgtInputDF, "tgt", shouldCapitalize = false, publisher)
    val tgtInputColumn = tgtInputRenameColumnDF.columns.map(tgtInputRenameColumnDF(_))
    val diffColumn = if (publisher.threshold > 0 || publisher.thresholdPercent == 0) {
      (srcInputColumn zip tgtInputColumn).map(s => (s._1 - s._2).alias(s._1.toString()))
    }
    else {
      (srcInputColumn zip tgtInputColumn).map(s => (abs(s._1 - s._2) - s._1 * (publisher.thresholdPercent / 100)).alias(s._1.toString()))
    }

    if (publisher.thresholdPercent > 0) {
      log.info("ThresholdPercentage calculation/expression which will executed against aggregated source and " +
        "target dataframes=>" + diffColumn.mkString(","))
    }
    val srcInputNewDF = srcInputDF.withColumn("dummy", lit(1))
    val tgtInputRenameColumnNewDF = tgtInputRenameColumnDF.withColumn("tgtdummy", lit(1))
    val diffDF = srcInputNewDF.join(tgtInputRenameColumnNewDF, srcInputNewDF(("dummy")) === tgtInputRenameColumnNewDF("tgtdummy")).select(diffColumn: _*)
    val diffRenameColumnDF = aliasColumn(diffDF, "srcTgtDiff", shouldCapitalize = true, publisher)
    srcInputNewDF.persist(StorageLevel.MEMORY_AND_DISK)
    tgtInputRenameColumnNewDF.persist(StorageLevel.MEMORY_AND_DISK)
    log.info("Difference values for the dq columns b/t source and target")
    diffRenameColumnDF.show(false)
    //only for debugging
    if (publisher.thresholdPercent > 0) {
      val thresholdCal = (srcInputColumn zip tgtInputColumn).map(s => (s._1 * (publisher.thresholdPercent / 100)).alias(s._1.toString()))
      val thresholdCalDF = srcInputNewDF.join(tgtInputRenameColumnNewDF, srcInputNewDF(("dummy")) === tgtInputRenameColumnNewDF("tgtdummy"))
        .select(thresholdCal: _*)
      log.info("Values for the dq columns after applying threshold % on source")
      val thresholdCalAliasDF = aliasColumn(thresholdCalDF, "threshold", shouldCapitalize = true, publisher)
      thresholdCalAliasDF.show(false)
    }

    diffRenameColumnDF
  }

  private def columnValueDiffThresholdPerJson(aggSourceDataFrame: DataFrame,
                                              aggTargetDataframe: DataFrame,
                                              dqNewColumns:Seq[String],
                                              publisher: DataQualityPublisherGeneric
                                             )(implicit spark: SparkSession) = {

    //This will calculate difference b/t source and target and applies Threshold represented as key value pair
    val srcInputDF: DataFrame = aggSourceDataFrame.select(dqNewColumns.map(m => col(m)): _*)
    val tgtInputDF: DataFrame = aggTargetDataframe.select(dqNewColumns.map(m => col(m)): _*)
    val srcInputColumn = srcInputDF.columns.map(srcInputDF(_))
    val tgtInputRenameColumnDF = aliasColumn(tgtInputDF, "tgt", shouldCapitalize = false, publisher)
    val tgtInputColumn = tgtInputRenameColumnDF.columns.map(tgtInputRenameColumnDF(_))
    val thresholdColumnDF = deriveThresholdDF(publisher)
    val thresholdColumn = thresholdColumnDF.columns.map(thresholdColumnDF(_))
    val diffColumn=((srcInputColumn , tgtInputColumn , thresholdColumn).zipped.toArray)
      .map(s => (abs(s._1 - s._2) -   s._1 * (s._3/100)).alias(s._1.toString()))
    log.info("ThresholdPercentage calculation/expression which will executed against aggregated source and " +
      "target dataframes=>" + diffColumn.mkString(","))
    val srcInputNewDF = srcInputDF.withColumn("dummy", lit(1))
    val tgtInputRenameColumnNewDF = tgtInputRenameColumnDF.withColumn("tgtdummy", lit(1))
    val diffDF = srcInputNewDF.join(tgtInputRenameColumnNewDF, srcInputNewDF(("dummy")) === tgtInputRenameColumnNewDF("tgtdummy"))
      .join(thresholdColumnDF, srcInputNewDF(("dummy")) === thresholdColumnDF("thresholddummy"))
      .select(diffColumn: _*)
    val diffRenameColumnDF = aliasColumn(diffDF, "srcTgtDiff", shouldCapitalize = true, publisher)
    srcInputNewDF.persist(StorageLevel.MEMORY_AND_DISK)
    tgtInputRenameColumnNewDF.persist(StorageLevel.MEMORY_AND_DISK)
    thresholdColumnDF.persist(StorageLevel.MEMORY_AND_DISK)
    //only for debugging
    if (publisher.thresholdPercentJson !="") {
      val thresholdCal=((srcInputColumn , tgtInputColumn , thresholdColumn).zipped.toArray).map(s => (s._1 * (s._3/100)).alias(s._1.toString()))
      val thresholdCalDF = srcInputNewDF.join(tgtInputRenameColumnNewDF, srcInputNewDF(("dummy")) === tgtInputRenameColumnNewDF("tgtdummy"))
        .join(thresholdColumnDF, srcInputNewDF(("dummy")) === thresholdColumnDF("thresholddummy"))
        .select(thresholdCal: _*)
      log.info("Values for the dq columns after applying threshold % on source." +
        " Diff of source and target dq column values should be less than this to get pass status")
      val thresholdCalAliasDF = aliasColumn(thresholdCalDF, "threshold", shouldCapitalize = true, publisher)
      generateMetricAndPublish(thresholdCalAliasDF,publisher)
      thresholdCalAliasDF.show(false)
    }

    diffRenameColumnDF
  }

  private def deriveThresholdDF(publisher: DataQualityPublisherGeneric)(implicit spark: SparkSession): DataFrame = {

    //This will load the threshold json string which is the input and compares with dq column and adds default if column not
    //present in threshold json string
    import spark.implicits._
    val df = spark.read.json(Seq(publisher.thresholdPercentJson).toDS)
    val dqNewColumns = "Count" +: publisher.dqColumns
    val columnsAddedDF = dqNewColumns.foldLeft(df) { case (d, c) =>
      if (d.columns.contains(c)) {
        d
      } else {
        d.withColumn(c, lit(0))
      }
    }
    val thresholdColumnRenameDF = columnsAddedDF.select(dqNewColumns.map(c => col(c)) : _*)
    val thresholdColumnDF = aliasColumn(thresholdColumnRenameDF, "threshold", shouldCapitalize = true, publisher).withColumn("thresholddummy", lit(1)).alias("thresholdColumnDF")

    thresholdColumnDF

  }

  private def deriveSelectExprForThreshold(diffDataFrame: DataFrame ,
                                           publisher: DataQualityPublisherGeneric
                                          ): DataFrame = {

    //This will evaluate the derived threshold values for later to set pass or fail in thresholdCheck
    val selectExpr = if (publisher.threshold > 0) {
      diffDataFrame.columns.map(x =>
        s"CASE WHEN abs($x) > $publisher.threshold  THEN $x ELSE 0 end as $x"
      )
    } else  {
      diffDataFrame.columns.map( x =>
        s"CASE WHEN $x > 0  THEN $x ELSE 0 end as $x"
      )
    }

    val diffSelectExprDF =  diffDataFrame.selectExpr(selectExpr:_*)

    //only for debugging
    log.info("Values after applying threshold calculation.If value is positive then diff of source and target"  +
      " for dq columns has crossed the threshold"  +
      " else if negative or 0 then it is within the threshold ")
    val diffSelectExprRenamed = diffSelectExprDF.columns.map(c => diffSelectExprDF(c).as(c.replaceAll("Flag","threshold")))
    val diffSelectExprRenamedDF = diffSelectExprDF.select(diffSelectExprRenamed:_*)
    diffSelectExprRenamedDF.show(false)

    diffSelectExprDF
  }

  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.TraversableLast"
  ))
  private def aliasColumn(inputDataFrame: DataFrame, prefix: String, shouldCapitalize: Boolean, publisher: DataQualityPublisherGeneric): DataFrame = {

    val inputOldColumn = inputDataFrame.columns
    val inputNewColumnList = inputOldColumn.
      flatMap(i => Seq(if (publisher.dqColumns.last == i && publisher.reportingDateColumnFlag && publisher.dqColumns.last != "Count") {
        prefix.take(3) + "Date"
      }
      else {
        prefix + (if (shouldCapitalize) {
          i.capitalize
        }
        else {
          i
        })
      }
      )
      )
    val inputRenameColumn = inputOldColumn.zip(inputNewColumnList).map(f => {
      col(f._1).as(f._2)
    })
    val inputRenameColumnDF = inputDataFrame.select(inputRenameColumn: _*)
    inputRenameColumnDF
  }


  private def aliasColumnSuffix(inputDataFrame: Dataset[Row]): DataFrame = {

    val inputOldColumn = inputDataFrame.columns
    val inputNewColumnList = inputOldColumn.
      flatMap(i => Seq( {i + "Flag"} ))
    val inputRenameColumn = inputOldColumn.zip(inputNewColumnList).map(f => {
      col(f._1).as(f._2)
    })
    val inputRenameColumnDF = inputDataFrame.select(inputRenameColumn: _*)
    inputRenameColumnDF
  }

  private def nullCheck(finalDataFrame: DataFrame, targetDataFrame: DataFrame, publisher: DataQualityPublisherGeneric
                       ): DataFrame = {

    if (publisher.nullsCheckKey.nonEmpty) {
      val condition = publisher.nullsCheckKey.map(v => col(v).isNull || col(v) === "").reduce(_ && _)
      val tgtAttrNullCheck = targetDataFrame
        .filter(condition)
      val tgtAttrNullCheckCount = tgtAttrNullCheck.count()
      PipeLineMetadataService.setLongMetricValue(MarketingConstants.NULL_COUNT, tgtAttrNullCheckCount, influxField = true)
      nullDupsErrorHandling(publisher.pipelineConfig, tgtAttrNullCheck, tgtAttrNullCheckCount, "Null", publisher.loggerName)
      val finalDataFrameNew = finalDataFrame.withColumn("nullRowsCountFlag", if (tgtAttrNullCheckCount == 0) lit("pass") else lit("fail"))
      finalDataFrameNew.withColumn("nullRowsCount", lit(tgtAttrNullCheckCount))

    } else {
      finalDataFrame.withColumn("nullRowsCountFlag", lit("nocheck"))
    }
  }

  private def dupCheck(finalDataFrame: DataFrame, targetDataFrame: DataFrame, publisher: DataQualityPublisherGeneric
                      ): DataFrame = {

    if (publisher.dupsCheckKey.nonEmpty) {
      val pkColumns = publisher.dupsCheckKey.map(col)
      val dupCheck = targetDataFrame.groupBy(pkColumns: _*).count().filter("count > 1")
      val dupCheckCount = dupCheck.count()
      DPPMetricUtils.setDuplicatedRowsCount(dupCheckCount)
      nullDupsErrorHandling(publisher.pipelineConfig, dupCheck, dupCheckCount, "Dups", publisher.loggerName)
      val finalDataFrameNew = finalDataFrame.withColumn("duplicateRowsCountFlag", if (dupCheckCount == 0) lit("pass") else lit("fail"))
      finalDataFrameNew.withColumn("duplicateRowsCount", lit(dupCheckCount))
    } else {
      finalDataFrame.withColumn("duplicateRowsCountFlag", lit("nocheck"))
    }
  }

  private def nullDupsErrorHandling(pipelineConfig: PipeLineConfig, NullDupsDF: Dataset[Row],
                                    tgtAttrNullDupsCheckCount: Long, prefix: String, loggerName: String
                                   ) = {

    if (tgtAttrNullDupsCheckCount > 0) {
      val nulldupsOutputLoc = MarketingCommonUtils.appendSlashInEnd(
        pipelineConfig.config.getString(MarketingConstants.APP_DATAQUALITYLOCATION)
      ) + prefix + "_check"
      log.error(s"ERROR! - DQ Check Failed - $prefix found in critical attribute for ${loggerName} data. Total Rows: $tgtAttrNullDupsCheckCount")
      FileWriterService.writeDQCheckData(NullDupsDF, nulldupsOutputLoc)
      PipeLineMetadataService.setStringMetricValue(MarketingConstants.DETAILS, s" - $prefix values in critical PK columns for ${loggerName} data!!")
    } else {
      log.info(s"DQ Check PASSED  - $prefix check on critical attributes for ${loggerName} data")
    }
  }

  private def publishMetrics(metrics: String): Unit = {

    var key = ""
    var value = ""
    val metricsSplit = metrics.split(",")
    for (metrics <- metricsSplit) {
      log.info(metrics)
      key = metrics.split("=")(0).trim
      value = metrics.split("=")(1).trim
      if (key.contains("Flag")) {
        PipeLineMetadataService.setStringMetricValue(key, value, influxField = true)
      } else {
        if (key.contains("Date")) {
          PipeLineMetadataService.setStringMetricValue(key, value, influxField = true)
        }
        else {
          if (key.contains("Count")) {
            //PipeLineMetadataService.setLongMetricValue(key, value.toLong, influxField = true)
            PipeLineMetadataService.setDoubleMetricValue(key, value.toDouble, influxField = true)
            if (key.contains("srcTgtDiffCount")) {
              //DPPMetricUtils.setDataQualityFailRowsCount(value.toDouble)
              PipeLineMetadataService.setDoubleMetricValue(DPPMetricsConstants.DATAQUALITYFAILROWSCOUNT,value.toDouble)
            }
            if (key.contains("srcTotalCount")) {
              DPPMetricUtils.setInputRowsCount(value.toLong)
            }
          }
          else {
            PipeLineMetadataService.setDoubleMetricValue(key, value.toDouble, influxField = true)
          }
        }
      }
    }
  }

  private def metricsCheck(metrics: String, metricsmap: Map[String, Any]) = {

    var key = ""
    var value = ""
    val failedArrayLists = new util.ArrayList[String]
    val diffFailedArrayLists = new util.ArrayList[String]
    var tgtTotalCount: Long = 0
    val metricsSplit = metrics.split(",")
    for (_metrics <- metricsSplit) {
      key = _metrics.split("=")(0).trim
      value = _metrics.split("=")(1).trim
      if (_metrics.contains("Flag")) {
        if (value.trim == "\"fail\"") {
          val metricsDiffKey = key.dropRight(4)
          val metricsDiffValue = metricsmap.getOrElse(metricsDiffKey, "noflagfound")
          log.info(s"Metric $metricsDiffKey do not match between source and target, difference is: $metricsDiffValue")
          failedArrayLists.add(_metrics)
          if (_metrics.contains("srcTgtDiff")) {
            diffFailedArrayLists.add(_metrics)
          }
        }
      }
      if (_metrics.contains("tgtTotalCount")) {
        tgtTotalCount = value.toLong
      }

    }
    MetricsCheck(failedArrayLists, diffFailedArrayLists, tgtTotalCount)
  }

  private def metricsCheckHandleError(metricCheck: MetricsCheck, publisher: DataQualityPublisherGeneric): Unit = {

    val errorMsg: String = s"DQ Check FAILED - metrics do not match between source and Target for ${publisher.loggerName} data"
    val successMsg: String = s"DQ Check PASSED - metrics match between source and Target for ${publisher.loggerName} data"
    if (metricCheck.failedArrayLists.isEmpty && metricCheck.diffFailedArrayLists.isEmpty) {
      log.info(successMsg)
      PipeLineMetadataService.setStringMetricValue(MarketingConstants.DETAILS, successMsg)
      DPPMetricUtils.setCDSRowsCount(metricCheck.tgtTotalCount)
    } else {
      log.error(errorMsg)
      PipeLineMetadataService.setStringMetricValue(MarketingConstants.DETAILS, errorMsg)
      DPPMetricUtils.setCDSRowsCount(metricCheck.tgtTotalCount)
      if (publisher.throwExceptionUponFailure) throw DataQualityException(errorMsg)
    }
  }

  private def generateMetricAndPublish(metricsDF:DataFrame, publisher: DataQualityPublisherGeneric,metricHandler:Boolean=false) = {
    val parseFinalDFToMap = DataFrameUtils.dataframeToMap(metricsDF)
    val metrics = iterateMap(parseFinalDFToMap)
    if (metricHandler) {
      val metricsCheckCaseClass = metricsCheck(metrics, parseFinalDFToMap)
      log.info("failed metrics " + metricsCheckCaseClass.failedArrayLists)
      log.info("tgtTotalCount " + metricsCheckCaseClass.tgtTotalCount)
      metricsCheckHandleError(metricsCheckCaseClass, publisher: DataQualityPublisherGeneric)
    }
    publishMetrics(metrics)
  }
}
