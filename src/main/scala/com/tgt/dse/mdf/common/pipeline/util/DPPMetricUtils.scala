package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dsc.kelsa.datapipeline.core.config.{InitialMetadataParam, PipeLineConfig}
import com.tgt.dsc.kelsa.datapipeline.core.constants.DPPMetricsConstants
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import org.apache.spark.sql.SparkSession

import java.time.Instant

object DPPMetricUtils extends LoggingTrait {
  /**
   * Capture the fields to be displayed in metrics from config and spark session
   *
   * @param pipeLineConfig application configuration
   * @param version dip code version
   * @param core_version  code version of common utility
   * @param startEpochTime application start time
   * @param workflowID workflow id of the application if the job is scheduled from oozie
   * @param sparkSession spark session
   */
  def captureApplicationConfigMetrics(version :String,
                                      core_version : String,
                                      startEpochTime : Long,
                                      workflowID : String = "NA",
                                      sparkSession: SparkSession): Unit ={
    //Application start time and the stages
    logInfo(s"${MarketingConstants.MARKETING_CONSTANTS} Capture metrics from application configuration and sparksession $startEpochTime")
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.VERSION , version)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.CORE_VERSION , core_version)

    logInfo(s" ${MarketingConstants.MARKETING_CONSTANTS} Workflow id of the application ${workflowID}")
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.WORKFLOW_ID , workflowID , true)
    logInfo(s"${MarketingConstants.MARKETING_CONSTANTS} All the spark session configurations are ${sparkSession.conf.getAll.mkString("\n")}")
    val totalExecutors : String = sparkSession.conf.getOption(MarketingConstants.SPARK_EXECUTOR_INSTANCES).getOrElse("-1")
    val executorCores : String = sparkSession.conf.getOption(MarketingConstants.SPARK_EXECUTOR_CORES).getOrElse("-1")
    val executorMemory : String = sparkSession.conf.getOption(MarketingConstants.SPARK_EXECUTOR_MEMORY).getOrElse("-1")
    val driverMemory : String = sparkSession.conf.getOption(MarketingConstants.SPARK_DRIVER_MEMORY).getOrElse("-1")
    val queueName : String = sparkSession.conf.getOption(MarketingConstants.SPARK_YARN_QUEUE).getOrElse("-1")
    val applicationId : String = sparkSession.conf.getOption(MarketingConstants.SPARK_APP_ID).getOrElse("-1")
    logInfo( s"${MarketingConstants.MARKETING_CONSTANTS} totalExecutors $totalExecutors executorCores $executorCores executorMemory $executorMemory $driverMemory queueName $queueName applicationId $applicationId")
    //Set the spark params for monitoring, these configs can be used for tuning
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.APPLICATION_ID , applicationId , true)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.TOTAL_EXECUTORS , totalExecutors , true)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.EXECUTOR_CORES , executorCores, true)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.EXECUTOR_MEMORY , executorMemory, true)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.DRIVER_MEMORY , driverMemory, true)
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.QUEUE_NAME , queueName, true)

    logInfo(s"${MarketingConstants.MARKETING_CONSTANTS} Capture metrics from application configuration and sparksession completed")
  }

  /**
   * Initialize metadata parameters like app_name, blossomid,team name and contact details
   *
   * @param exception Exception
   * @return
   */

  def errorMetricDetails(exception: Exception): Unit ={
    PipeLineMetadataService.setStringMetricValue(
      DPPMetricsConstants.DPPJOBSTATUS,MarketingConstants.FAILURE, influxField = true)
    PipeLineMetadataService.setStringMetricValue(
      MarketingConstants.DETAILS,exception.getMessage)
    PipeLineMetadataService.setStringMetricValue(
      MarketingConstants.ERRORMESSAGE,exception.getMessage )
    log.error("Exception thrown and captured in Pipeline Metadata!")
  }

  /**
   * Initialize metadata parameters like app_name, blossomid,team name and contact details
   *
   * @param pipeLineConfig configurations
   * @return
   */
  def initializeMetricCollector(pipeLineConfig: PipeLineConfig): Unit = {
    PipeLineMetadataService.metadataInitiation(
      InitialMetadataParam(pipeLineConfig.config.getString(MarketingConstants.APP_NAME),
        pipeLineConfig.config.getString(MarketingConstants.APP_BLOSSOMID),
        pipeLineConfig.config.getString(MarketingConstants.APP_TEAMNAME),
        pipeLineConfig.config.getString(MarketingConstants.APP_CONTACTDETAILS)
      ))
    val env: String = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    log.info(s"${MarketingConstants.MARKETING_CONSTANTS} Initialized Pipeline run metadata for environment ${env}")
  }

  /**
   * Used as a wrapper class around the PipeLineMetadataService.setLongMetricValue() method
   * @param metricName The name of the metric to set in the PipeLineMetadataCache
   * @param count The count to set in the PipeLineMetadataCache
   * @return True if successful; false otherwise
   */
  def setLongMetric(metricName: String, count: Long): Boolean = {
    PipeLineMetadataService.setLongMetricValue(metricName,count)
  }

  /**
   * Sets the DPPMetricsConstants.TRANSFORMATIONFAILROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of failed transformation rows
   * @return True if successful; false otherwise
   */
  def setTransformationFailRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.TRANSFORMATIONFAILROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.ENRICHMENTFAILROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of failed enrichment rows
   * @return True if successful; false otherwise
   */
  def setEnrichmentFailRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.ENRICHMENTFAILROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.EXPLODEDINPUTROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of exploded rows
   * @return True if successful; false otherwise
   */
  def setExplodedInputRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.EXPLODEDINPUTROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.EXPLODEDINPUTROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of exploded rows
   * @return True if successful; false otherwise
   */
  def setInputRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.INPUTROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.DEDUPROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of deduplicated rows
   * @return True if successful; false otherwise
   */
  def setDedupRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.DEDUPROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.DATATYPEPASSROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of rows passing a data type dq check
   * @return True if successful; false otherwise
   */
  def setDataTypePassRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.DATATYPEPASSROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.DATAQUALITYPASSROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of rows passing a data quality check
   * @return True if successful; false otherwise
   */
  def setDataQualityPassRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.DATAQUALITYPASSROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.TRANSFORMATIONROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of rows after transformation stage
   * @return True if successful; false otherwise
   */
  def setTransformationRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.TRANSFORMATIONROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.DECORATIONROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of rows after decoration stage
   * @return True if successful; false otherwise
   */
  def setDecorationRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.DECORATIONROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.CDSROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of rows after final data quality metrics pass
   */
  def setCDSRowsCount(count: Long): Unit = {
    setLongMetric(DPPMetricsConstants.CDSROWSCOUNT,count)
    log.info("FINAL OUTPUTROWS added to grafana metric : " + count)
  }

  /**
   * Sets the DPPMetricsConstants.DUPLICATEDROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of rows after final data quality metrics pass
   */
  def setDuplicatedRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.DUPLICATEDROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.DATAQUALITYFAILROWSCOUNT metric in the PipeLineMetadataCache
   * @param count Count of rows after final data quality metrics pass
   */
  def setDataQualityFailRowsCount(count: Long): Boolean = {
    setLongMetric(DPPMetricsConstants.DATAQUALITYFAILROWSCOUNT,count)
  }

  /**
   * Sets the DPPMetricsConstants.DATEOFDATA to the current time from epoch in milliseconds
   * @return The current time from epoch in milliseconds
   */
  def setDateOfData(): Long = {
    val dateOfData : Long = Instant.now().toEpochMilli
    setLongMetric(DPPMetricsConstants.DATEOFDATA,dateOfData)
    dateOfData
  }

}
