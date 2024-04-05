package com.tgt.dse.mdf.common.pipeline


import java.time.LocalDateTime
import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants.{APP_BLOSSOMID, APP_CONTACTDETAILS, APP_ENV, APP_FNDEXTLOCATION, APP_JDBCURL, APP_LOCKLOCATION, APP_NAME, APP_QUEUENAME, APP_TEAMNAME, DATEFORMATTER}
import com.tgt.dse.mdf.common.pipeline.service.ExecuteCommonMain.deleteLock
import com.tgt.dse.mdf.common.pipeline.service.FileWriterService
import com.tgt.dse.mdf.common.pipeline.service.FileWriterService.makeDir
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils
import com.tgt.dse.mdf.common.pipeline.vo.CommonExecutePipelineParameterObjects
import org.apache.spark.sql.SparkSession

trait CommonExecutePipeLineHelpers extends LoggingTrait  {

  val JOB_START_TIME: String = LocalDateTime.now().format(DATEFORMATTER)

  implicit val spark = SparkSession.builder().master("local")
    .appName("Common-Execute-Pipeline-LocalTest")
    .getOrCreate()
  val CONFIG_SEQ: Seq[String] = Seq(
    APP_NAME,
    APP_BLOSSOMID,
    APP_TEAMNAME,
    APP_CONTACTDETAILS,
    APP_FNDEXTLOCATION,
    APP_LOCKLOCATION,
    APP_ENV,
    APP_JDBCURL,
    APP_QUEUENAME
  )
  val testConfFile: String = "src/test/resources/common_execute_pipeline/common_execute_pipeline_test.conf"
  val testConfFileArray: Array[String] = Array("src/test/resources/common_execute_pipeline/common_execute_pipeline_test.conf")
  val testConfig: PipeLineConfig = MarketingCommonUtils.loadConfigurationFile(testConfFile,CONFIG_SEQ)
  lazy val testMarketingPipelineConfig: MarketingPipelineConfigs = MarketingPipelineConfigs(testConfig)

  val errorConfigFile: String = "src/test/resources/common_execute_pipeline/common_execute_pipeline_error_test.conf"
  val errorConfFileArray: Array[String] = Array("src/test/resources/common_execute_pipeline/common_execute_pipeline_error_test.conf")
  val errorConfig: PipeLineConfig =  MarketingCommonUtils.loadConfigurationFile(errorConfigFile,CONFIG_SEQ)
  lazy val errorMarketingPipelineConfig: MarketingPipelineConfigs = MarketingPipelineConfigs(errorConfig)


  val pipeLineConfigTest = CommonExecutePipelineParameterObjects.CommonPipeLineHelper(testConfig)


  makeDir(pipeLineConfigTest.inputEphemeralPath)
  makeDir(pipeLineConfigTest.archivePath)
  makeDir(pipeLineConfigTest.archivePathPartition)
  makeDir(pipeLineConfigTest.fileLandingPath)
  makeDir(pipeLineConfigTest.pipelineFailures)
  makeDir(pipeLineConfigTest.lockLocation)



  val srcFilepath="src/test/resources/common_execute_pipeline/target_invoice_revenue_mocked-2022-08-22.csv"
  FileWriterService.copyFile(srcFilepath,pipeLineConfigTest.fileLandingPath)

  FileWriterService.emptyDir(pipeLineConfigTest.lockLocation)(spark)
  deleteLock(testMarketingPipelineConfig.lockPath,testMarketingPipelineConfig.appName)


}