package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dsc.kelsa.datapipeline.core.constants.{DPPMetricsConstants, ServiceConstants}
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.FileCopyException
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class DPPMetricUtilsTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {

  "The DPPMetricsUtilTest" should "do the following"

  it should "initialize Metrics for InfluxDB" in {
    DPPMetricUtils.initializeMetricCollector(pipeLineConfig)
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.CDSROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.INPUTROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.EXPLODEDINPUTROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DEDUPROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DATATYPEPASSROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DATAQUALITYPASSROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.TRANSFORMATIONROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DECORATIONROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.CDSSTAGINGCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.CORRUPTROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.MISSINGCOLUMNROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.NEWCOLUMNROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DUPLICATEDROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DATATYPEFAILROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DATAQUALITYFAILROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.TRANSFORMATIONFAILROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.ENRICHMENTFAILROWSCOUNT))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.PROCESSINGTHROUGHPUTPERSECOND))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.PROCESSINGRUNTIMEINSECONDS))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DATEOFDATA))
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(ServiceConstants.STARTEPOCHTIME))
    assertResult(true)(PipeLineMetadataService.isStringMetricExists(ServiceConstants.STAGE))
    assertResult(true)(PipeLineMetadataService.isStringMetricExists(ServiceConstants.STATUS))
    assertResult(true)(PipeLineMetadataService.existsTag(ServiceConstants.APPNAME))
    assertResult(true)(PipeLineMetadataService.existsTag(ServiceConstants.BLOSSOMID))
    assertResult(true)(PipeLineMetadataService.existsTag(ServiceConstants.TEAMNAME))
    assertResult(true)(PipeLineMetadataService.existsTag(ServiceConstants.CONTACTDETAILS))
  }

  it should "add application-specific metrics from config" in {
    val version="1.1"
    val core_version="2.0"
    val startEpochTime: Long=1234
    val workflowId="wfid_21"
    val totalExecutors : String = spark.conf.getOption(MarketingConstants.SPARK_EXECUTOR_INSTANCES).getOrElse("-1")
    val executorCores : String = spark.conf.getOption(MarketingConstants.SPARK_EXECUTOR_CORES).getOrElse("-1")
    val executorMemory : String = spark.conf.getOption(MarketingConstants.SPARK_EXECUTOR_MEMORY).getOrElse("-1")
    val driverMemory : String = spark.conf.getOption(MarketingConstants.SPARK_DRIVER_MEMORY).getOrElse("-1")
    val queueName : String = spark.conf.getOption(MarketingConstants.SPARK_YARN_QUEUE).getOrElse("-1")
    val applicationId : String = spark.conf.getOption(MarketingConstants.SPARK_APP_ID).getOrElse("-1")

    DPPMetricUtils.captureApplicationConfigMetrics(version,core_version,startEpochTime,workflowId,spark)

    assertResult(version)(PipeLineMetadataService.getStringMetricValue(MarketingConstants.VERSION))
    assertResult(core_version)(PipeLineMetadataService.getStringMetricValue(MarketingConstants.CORE_VERSION))
    assertResult("\"" + workflowId + "\"")(PipeLineMetadataService.getStringMetricValue(MarketingConstants.WORKFLOW_ID))
    assertResult("\"" + totalExecutors + "\"")(PipeLineMetadataService.getStringMetricValue(MarketingConstants.TOTAL_EXECUTORS))
    assertResult("\"" + executorCores + "\"")(PipeLineMetadataService.getStringMetricValue(MarketingConstants.EXECUTOR_CORES))
    assertResult("\"" + executorMemory + "\"")(PipeLineMetadataService.getStringMetricValue(MarketingConstants.EXECUTOR_MEMORY))
    assertResult("\"" + driverMemory + "\"")(PipeLineMetadataService.getStringMetricValue(MarketingConstants.DRIVER_MEMORY))
    assertResult("\"" + queueName + "\"")(PipeLineMetadataService.getStringMetricValue(MarketingConstants.QUEUE_NAME))
    assertResult("\"" + applicationId + "\"")(PipeLineMetadataService.getStringMetricValue(MarketingConstants.APPLICATION_ID))
  }

  it should "set the Error Metrics for InfluxDB" in {

    val errorMessage: String = "Throwable metric test"
    try {
      throw FileCopyException(errorMessage)
    } catch {
      case e: FileCopyException =>
        DPPMetricUtils.errorMetricDetails(e)
    }

    assertResult(true)(PipeLineMetadataService.isStringMetricExists(MarketingConstants.DETAILS))
    assertResult(true)(PipeLineMetadataService.isStringMetricExists(DPPMetricsConstants.DPPJOBSTATUS))
    assertResult(true)(PipeLineMetadataService.isStringMetricExists(MarketingConstants.ERRORMESSAGE))
  }

  it should "set transformation fail rows count" in {
    val count = 123456789L
    DPPMetricUtils.setTransformationFailRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.TRANSFORMATIONFAILROWSCOUNT))
  }

  it should "set enrichment fail rows count" in {
    val count = 123456789L
    DPPMetricUtils.setEnrichmentFailRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.ENRICHMENTFAILROWSCOUNT))
  }

  it should "set exploded input rows count" in {
    val count = 123456789L
    DPPMetricUtils.setExplodedInputRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.EXPLODEDINPUTROWSCOUNT))
  }

  it should "set dedupe input rows count" in {
    val count = 123456789L
    DPPMetricUtils.setDedupRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DEDUPROWSCOUNT))
  }

  it should "set datatype pass rows count" in {
    val count = 123456789L
    DPPMetricUtils.setDataTypePassRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DATATYPEPASSROWSCOUNT))
  }

  it should "set data quality pass rows count" in {
    val count = 123456789L
    DPPMetricUtils.setDataQualityPassRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DATAQUALITYPASSROWSCOUNT))
  }

  it should "set transformation pass rows count" in {
    val count = 123456789L
    DPPMetricUtils.setTransformationRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.TRANSFORMATIONROWSCOUNT))
  }

  it should "set decoration pass rows count" in {
    val count = 123456789L
    DPPMetricUtils.setDecorationRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DECORATIONROWSCOUNT))
  }

  it should "set the date of data" in {
    DPPMetricUtils.setDateOfData()
    assertResult(true)(PipeLineMetadataService.isLongMetricExists(DPPMetricsConstants.DATEOFDATA))
  }

  it should "set input rows count" in {
    val count = 123456789L
    DPPMetricUtils.setInputRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.INPUTROWSCOUNT))
  }

  it should "set CDS rows count" in {
    val count = 123456789L
    DPPMetricUtils.setCDSRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.CDSROWSCOUNT))
  }

  it should "set duplicated rows count" in {
    val count = 123456789L
    DPPMetricUtils.setDuplicatedRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DUPLICATEDROWSCOUNT))
  }

  it should "set Data Quality Fail rows count" in {
    val count = 123456789L
    DPPMetricUtils.setDataQualityFailRowsCount(count)
    assertResult(count)(PipeLineMetadataService.getLongMetricValue(DPPMetricsConstants.DATAQUALITYFAILROWSCOUNT))
  }
}
