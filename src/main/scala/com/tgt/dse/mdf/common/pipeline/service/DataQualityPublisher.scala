package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.DataQualityException
import com.tgt.dse.mdf.common.pipeline.transform.RevenueTransformer
import com.tgt.dse.mdf.common.pipeline.util.{DPPMetricUtils, MarketingCommonUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

/**
 * The publisher settings for Data Quality Metrics
 * @param primaryKeyColumns A sequence of strings with the names of each of the columns which make up the Primary Key.
 *                          The inputDF and finalDF must both have the same column names for their primary key columns.
 * @param revenueColumn The name of the revenue column which is common to both the inputDF and finalDF and used to
 *                      sum and verify it matches between the two dataframes.
 * @param impressionsColumn The name of the impressions column which is common to both the inputDF and finalDF and used
 *                          to sum and verify it matches between the two dataframes.
 * @param clicksColumn The name of the clicks column which is common to both the inputDF and finalDF and used
 *                          to sum and verify it matches between the two dataframes.
 * @param reportingDateColumn The name of the reporting date column which is common to both the inputDF and finalDF and used
 *                          to count/group by both and verify it matches between the two dataframes.
 * @param loggerName The name used in log messages to customize the output, eg. "GAM Actuals", "FB Actuals", etc.
 * @param isRevenueMicro If set to true, it will apply RevenueTransformer.convertMicroToCurrency to
 *                       the revenue column in the inputDF before summing it. This assumes that the finalDF has already
 *                       had its revenue column transformed from micro currency to USD, and by performing this step
 *                       on the inputDF, should cause both to equal each other when summed. Otherwise, set to false to
 *                       not transform the revenue column on the inputDF from micro currency to USD.
 * @param throwExceptionUponFailure If set to true, if the end result of this method determines that there is a data
 *                                  quality issue, it will throw a DataQualityException. If set to false, it
 *                                  will only log the error, but not throw an exception.
 */
final case class DataQualityPublisher(primaryKeyColumns: Seq[String],
                                revenueColumn: String,
                                impressionsColumn: String,
                                clicksColumn: String,
                                reportingDateColumn: String,
                                loggerName: String,
                                isRevenueMicro: Boolean,
                                throwExceptionUponFailure: Boolean = true
                                      ) extends LoggingTrait {}

/**
 * The metrics gathered from source and target DataFrames
 * @param srcAggregatedMetrics The source DataFrame
 * @param tgtAggregatedMetrics The target DataFrame
 */
final case class DataQualityMetrics(srcAggregatedMetrics: DataFrame, tgtAggregatedMetrics: DataFrame) extends LoggingTrait {
  final val IMPRESSIONS_COLUMN_POSITION = 0
  final val CLICKS_COLUMN_POSITION = 1
  final val SPEND_COLUMN_POSITION = 2
  final val COUNT_COLUMN_POSITION = 3
  final val DATE_COLUMN_POSITION = 4
  //Source metrics
  val srcMetricsArray: Array[Row] = srcAggregatedMetrics.collect()
  log.info(s"Source metrics array: ${srcMetricsArray.mkString("Array(", ", ", ")")}")
  val srcTotalImpressions: Double = srcMetricsArray(0)(IMPRESSIONS_COLUMN_POSITION).toString.toDouble
  log.info(s"Source total impressions: $srcTotalImpressions")
  val srcTotalClicks: Double = srcMetricsArray(0)(CLICKS_COLUMN_POSITION).toString.toDouble
  log.info(s"Source total clicks: $srcTotalClicks")
  val srcTotalSpend: String = srcMetricsArray(0)(SPEND_COLUMN_POSITION).toString
  log.info(s"Source total spend: $srcTotalSpend")
  val srcTotalCount: Long = srcMetricsArray(0)(COUNT_COLUMN_POSITION).toString.toLong
  DPPMetricUtils.setInputRowsCount(srcTotalCount)
  log.info(s"Source total row count: $srcTotalCount")
  val srcDateOfData: String = srcMetricsArray(0)(DATE_COLUMN_POSITION).toString
  log.info(s"Source max date of data: $srcDateOfData")

  //Target metrics
  val tgtMetricsArray: Array[Row] = tgtAggregatedMetrics.collect()
  log.info(s"Target metrics array: ${srcMetricsArray.mkString("Array(", ", ", ")")}")
  val tgtTotalImpressions: Double = tgtMetricsArray(0)(IMPRESSIONS_COLUMN_POSITION).toString.toDouble
  log.info(s"Target total impressions: $tgtTotalImpressions")
  val tgtTotalClicks: Double = tgtMetricsArray(0)(CLICKS_COLUMN_POSITION).toString.toDouble
  log.info(s"Target total clicks: $tgtTotalClicks")
  val tgtTotalSpend: String = tgtMetricsArray(0)(SPEND_COLUMN_POSITION).toString
  log.info(s"Target total spend: $tgtTotalSpend")
  val tgtTotalCount: Long = srcMetricsArray(0)(COUNT_COLUMN_POSITION).toString.toLong
  log.info(s"Target total row count: $tgtTotalCount")
  val tgtDateOfData: String = tgtMetricsArray(0)(DATE_COLUMN_POSITION).toString
  log.info(s"Target max date of data: $tgtDateOfData")
}
/**
 * A publisher of Data Quality Metrics
 */
object DataQualityPublisher extends LoggingTrait {

  /**
   * Performs 3 checks: nullity on primary key columns, duplicate records, count difference between source and target.
   * For each of the checks performed, adds metrics to the {@link PipeLineMetadataService} for this session
   * @param pipelineConfig The {@link PipeLineConfig} for this session
   * @param inputDF The input {@link DataFrame} holding the initial source data
   * @param finalDF The final output {@link DataFrame} holding the final data to write to core history table
   * @param publisher The DataQualityMetricsPublisher which has all the settings needed to log and collect metrics
   * @param sparkSession The implicit {@link SparkSession}
   * @throws DataQualityException If any of the three checks (nullity, duplicates, mismatch) fails, this is thrown, but
   *                              only if the throwExceptionUponFailure parameter is true.
   */
  def dqCheck(pipelineConfig: PipeLineConfig, inputDF:DataFrame, finalDF:DataFrame, publisher: DataQualityPublisher): Unit = {
    log.info(s"Gathering Source Aggregated DataFrame for ${publisher.loggerName}...")
    val srcTotMetrics = generateAggregatedMetrics(inputDF, "source",publisher)

    log.info(s"Gathering Target Aggregated DataFrame for ${publisher.loggerName}...")
    val tgtTotMetrics = generateAggregatedMetrics(finalDF, "target",publisher)

    log.info(s"Initiating Metrics for ${publisher.loggerName}...")
    val metrics: DataQualityMetrics = DataQualityMetrics(srcAggregatedMetrics = srcTotMetrics, tgtAggregatedMetrics = tgtTotMetrics)

    log.info(s"Adding Source metrics for ${publisher.loggerName}...")
    generatePipelineMetrics(metrics.srcTotalImpressions, metrics.srcTotalSpend, metrics.srcTotalClicks,"source",publisher)
    log.info(s"Adding Target metrics for ${publisher.loggerName}...")
    generatePipelineMetrics(metrics.tgtTotalImpressions, metrics.tgtTotalSpend, metrics.tgtTotalClicks, "target",publisher)
    log.info(s"Adding Diff metrics for ${publisher.loggerName}...")
    generateDiffMetrics(metrics)

    log.info(s"Checking nulls in primary key columns for ${publisher.loggerName}...")
    val tgtAttrNullCheck: Boolean = nullCheck(pipelineConfig, finalDF, publisher.primaryKeyColumns, publisher)
    log.info(s"Checking for duplicates in primary key columns for ${publisher.loggerName}...")
    val dupCheck: Boolean = dupeCheck(pipelineConfig, finalDF, publisher.primaryKeyColumns,publisher)
    log.info(s"Checking for mismatches in impressions, clicks, revenue, date, and counts for ${publisher.loggerName}...")
    val srcTgtMismatchCheck: Boolean = diffCheck(metrics, publisher)

    DPPMetricUtils.setCDSRowsCount(metrics.tgtTotalCount)
    if ( !tgtAttrNullCheck || !dupCheck || !srcTgtMismatchCheck ){
      val errorMsg: String = "The Data Quality checks did not pass!"
      if (publisher.throwExceptionUponFailure) {
        throw DataQualityException(errorMsg)
      } else {
        log.error(errorMsg)
      }
    } else {
      log.info("The Data Quality checks passed successfully!")
    }
  }

  private def abbreviateSourceOrTarget(sourceOrTarget: String): String = {
    var ABBREVIATED_NAME = ""
    if (sourceOrTarget == "source") {
      ABBREVIATED_NAME = "src"
    } else if (sourceOrTarget == "target") {
      ABBREVIATED_NAME = "tgt"
    } else {
      throw new NotImplementedError("Given value for SourceOrTarget not supported: " + sourceOrTarget + ". Please pass only 'source' or 'target'")
    }
    ABBREVIATED_NAME
  }

  private def generateAggregatedMetrics(initialDF: DataFrame, sourceOrTarget: String,
                                        publisher: DataQualityPublisher): DataFrame = {
    val ABBREVIATED_NAME = abbreviateSourceOrTarget(sourceOrTarget)

    //Need to apply revenue transformation to the source DF before aggregation. This is because the
    // target DF will have its revenue already transformed, and if we attempt to reconcile the two, they
    // will always fail. Also, the transformation has to happen before aggregation: if this step is performed
    // after aggregation, then the loss in precision will also cause the final comparison on revenue to fail.
    val revenueTransformedDF: DataFrame = {
      if (sourceOrTarget.equals("source") && publisher.isRevenueMicro) {
        log.info("Casting revenue fields to decimal")
        log.info("Casting impressions and clicks to double")
        RevenueTransformer.convertMicroToCurrency(initialDF, Seq(publisher.revenueColumn))
      } else {
        initialDF
      }
    }
    val countOfRevenueTransformed: Long = revenueTransformedDF.count()
    //Now perform the initial aggregation to get total counts, sums for impressions, clicks, revenue, row count, date of data
    revenueTransformedDF.withColumn(s"${ABBREVIATED_NAME}TotRows",lit(countOfRevenueTransformed))
      .groupBy(s"${ABBREVIATED_NAME}TotRows")
      .agg(
        sum(publisher.impressionsColumn).alias(s"${ABBREVIATED_NAME}TotImpressions")
        ,sum(publisher.clicksColumn).alias(s"${ABBREVIATED_NAME}TotClicks")
        ,sum(publisher.revenueColumn).alias(s"${ABBREVIATED_NAME}TotSpend")
        ,max(publisher.reportingDateColumn).alias(s"${ABBREVIATED_NAME}DateOfLastAvailableData")
      )
      .select(s"${ABBREVIATED_NAME}TotImpressions", s"${ABBREVIATED_NAME}TotClicks",s"${ABBREVIATED_NAME}TotSpend",
        s"${ABBREVIATED_NAME}TotRows",s"${ABBREVIATED_NAME}DateOfLastAvailableData")
  }

  private def generatePipelineMetrics(totalImpressions: Double, totalSpend: String, totalClicks: Double, sourceOrTarget: String,
                                      publisher: DataQualityPublisher): Unit = {
    val ABBREVIATED_NAME = abbreviateSourceOrTarget(sourceOrTarget)
    log.info(s"Publishing $sourceOrTarget Total metrics into grafana for ${publisher.loggerName} data...")
    PipeLineMetadataService.setDoubleMetricValue(s"${ABBREVIATED_NAME}${MarketingConstants.TOTAL_IMPRESSIONS}",totalImpressions,influxField = true)
    PipeLineMetadataService.setDoubleMetricValue(s"${ABBREVIATED_NAME}${MarketingConstants.TOTAL_CLICKS}s",totalClicks,influxField = true)
    PipeLineMetadataService.setDoubleMetricValue(s"${ABBREVIATED_NAME}${MarketingConstants.TOTAL_SPEND}",totalSpend.toDouble,influxField = true)
    log.info(s"${ABBREVIATED_NAME}TotalImpressions...$totalImpressions")
    log.info(s"${ABBREVIATED_NAME}TotalClicks...$totalClicks")
    log.info(s"${ABBREVIATED_NAME}TotalSpend...$totalSpend")
  }

  private def generateDiffMetrics(metrics: DataQualityMetrics): Unit = {
    log.info("Publishing Source Target mismatch metric to grafana..")
    val srcTgtDiffImpressions = metrics.srcTotalImpressions - metrics.tgtTotalImpressions
    val srcTgtDiffSpend = metrics.srcTotalSpend.toDouble - metrics.tgtTotalSpend.toDouble
    val srcTgtDiffClicks = metrics.srcTotalClicks - metrics.tgtTotalClicks
    PipeLineMetadataService.setDoubleMetricValue(MarketingConstants.DIFF_IMPRESSIONS,srcTgtDiffImpressions,influxField = true)
    PipeLineMetadataService.setDoubleMetricValue(MarketingConstants.DIFF_SPEND,srcTgtDiffSpend,influxField = true)
    PipeLineMetadataService.setDoubleMetricValue(MarketingConstants.DIFF_CLICKS,srcTgtDiffClicks,influxField = true)
    log.info("srcTgtDiffImpressions..." + srcTgtDiffImpressions)
    log.info("srcTgtDiffSpend..." + srcTgtDiffSpend)
    log.info("srcTgtDiffClicks..." + srcTgtDiffClicks)
  }

  private def nullCheck(pipeLineConfig: PipeLineConfig, finalDataFrame: DataFrame, primaryKey: Seq[String],
                        publisher: DataQualityPublisher): Boolean = {
    log.info(s"Running checks for null value in critical attributes for ${publisher.loggerName} data...")

    val condition = primaryKey.map(v => col(v).isNull).reduce(_&&_)

    val tgtAttrNullCheck = finalDataFrame
      .filter(condition)
    val tgtAttrNullCheckCount = tgtAttrNullCheck.count()
    PipeLineMetadataService.setLongMetricValue(MarketingConstants.NULL_COUNT,tgtAttrNullCheckCount,influxField = true)

    if (tgtAttrNullCheckCount>0) {
      val nullOutputLoc = MarketingCommonUtils.appendSlashInEnd(
        pipeLineConfig.config.getString(MarketingConstants.APP_DATAQUALITYLOCATION)
      ) + "null_check"
      log.error(s"ERROR! - DQ Check Failed - null found in critical attribute for ${publisher.loggerName} data. Total Rows: $tgtAttrNullCheckCount")
      FileWriterService.writeDQCheckData(tgtAttrNullCheck, nullOutputLoc)
      PipeLineMetadataService.setStringMetricValue(MarketingConstants.DETAILS,s" - Null values in critical PK columns for ${publisher.loggerName} data!!")
      false
    } else {
      log.info(s"DQ Check PASSED  - null check on critical attributes for ${publisher.loggerName} data")
      true
    }
  }

  private def dupeCheck(pipeLineConfig: PipeLineConfig, finalDataFrame: DataFrame, primaryKey: Seq[String],
                        publisher: DataQualityPublisher): Boolean = {
    log.info("Running Duplicate Checks")
    val pkColumns = primaryKey.map(col)
    val dupCheck = finalDataFrame.groupBy(pkColumns:_*).count().filter("count > 1")
    val dupCheckCount = dupCheck.count()
    DPPMetricUtils.setDuplicatedRowsCount(dupCheckCount)

    if (dupCheckCount > 0) {
      val dupeOutputLoc = MarketingCommonUtils.appendSlashInEnd(
        pipeLineConfig.config.getString(MarketingConstants.APP_DATAQUALITYLOCATION)
      ) + "dupe_check"
      log.error(s"ERROR! - DQ Check Failed - duplicates found in critical attribute for ${publisher.loggerName} data. Total Rows: $dupCheckCount")
      FileWriterService.writeDQCheckData(dupCheck, dupeOutputLoc)
      PipeLineMetadataService.setStringMetricValue(MarketingConstants.DETAILS,
        s" - Duplicates found in the critical PK columns for ${publisher.loggerName} data!!")
      false
    } else {
      log.info(s"DQ Check PASSED  - Duplicate checks for ${publisher.loggerName} data")
      true
    }
  }

  private def diffCheck(metrics: DataQualityMetrics, publisher: DataQualityPublisher): Boolean = {

    var anyFail: Boolean = false
    // Critical DQ Checks to fail the DPP in case DQ check is failed as it may impact the end user reporting
    log.info(s"Running DQ Checks to see that source Total matches to Target Total for ${publisher.loggerName} data...")
    if (!metrics.srcTotalImpressions.equals(metrics.tgtTotalImpressions)) {
      log.error("Impressions do not match between source and target!")
      anyFail = true
    }
    if (!metrics.srcTotalClicks.equals(metrics.tgtTotalClicks)) {
      log.error("Clicks do not match between source and target!")
      anyFail = true
    }
    if (!metrics.srcTotalSpend.equals(metrics.tgtTotalSpend)) {
      log.error("Revenue does not match between source and target!")
      anyFail = true
    }
    if (!metrics.srcDateOfData.equals(metrics.tgtDateOfData)) {
      log.error("DateofData does not match between source and target!")
      anyFail = true
    }
    val countsDiff: Long = metrics.srcTotalCount - metrics.tgtTotalCount
    DPPMetricUtils.setDataQualityFailRowsCount(countsDiff)

    if (countsDiff > 0) {
      log.error(s"Total row counts do not match between source and target, difference is: $countsDiff")
      anyFail = true
    }

    if (anyFail) {
      log.error(s"ERROR! - DQ Check Failed - Source and Target metrics do not match for ${publisher.loggerName} data")
      PipeLineMetadataService.setStringMetricValue(MarketingConstants.DETAILS, s" - Source to Target Aggregate mismatch for ${publisher.loggerName} data!!")
      false
    }else {
      log.info(s"DQ Check PASSED - metrics match between source and Target for ${publisher.loggerName} data")
      true
    }
  }

}