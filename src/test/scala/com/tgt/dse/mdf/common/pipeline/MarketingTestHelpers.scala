package com.tgt.dse.mdf.common.pipeline

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.transformations.TransFunctions.renameColumns
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants._
import com.tgt.dse.mdf.common.pipeline.service.{FileReader, FileWriterService}
import com.tgt.dse.mdf.common.pipeline.service.FileWriterService.{deleteHiddenFile, emptyDir, makeDir, readDynamicJson}
import com.tgt.dse.mdf.common.pipeline.util.{DateTimeUtils, MarketingCommonUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.IOException
import java.time.LocalDateTime

trait MarketingTestHelpers extends LoggingTrait {

  implicit val spark: SparkSession = SparkSession.builder().master("local")
    //.appName("Marketing-Common-LocalTest")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val configPath: String = "src/test/resources/fb_api_actuals_pipeline_test.conf"
  System.setProperty("config.file", configPath)
  val pipeLineConfig: PipeLineConfig = MarketingCommonUtils.loadConfigurationFile(configPath)
  val JOB_START_TIME: String = LocalDateTime.now().format(DATEFORMATTER)
  lazy val testMarketingPipelineConfig: MarketingPipelineConfigs = MarketingPipelineConfigs(pipeLineConfig)

  lazy val errorConfigPath: String = "src/test/resources/fb_api_actuals_error_test.conf"
  lazy val errorPipeLineConfig: PipeLineConfig = MarketingCommonUtils.loadConfigurationFile(errorConfigPath)

  lazy val env: String = pipeLineConfig.config.getString(APP_ENV)
  lazy val lndCoreHistTable: String = pipeLineConfig.config.getString(APP_lndCoreHistTable)
  lazy val corruptRecordLocation: String =
    DateTimeUtils.appendPathWithTimeStamp(MarketingCommonUtils.appendSlashInEnd(
      pipeLineConfig.config.getString(APP_PIPELINEFAILURES)) + CORRUPTRECORDS,
      JOB_START_TIME)
  lazy val inputEphemeralPath: String = pipeLineConfig.config.getString(APP_INPUTLOCATION)
  lazy val archivePath: String = pipeLineConfig.config.getString(APP_EXISTINGRECORDSLOCATION)
  lazy val archivePathPartition: String = pipeLineConfig.config.getString(APP_ARCHIVEPARTITIONRECORDSLOCATION)
  lazy val filelandingPath: String = pipeLineConfig.config.getString(APP_FILEAPILANDINGLOCATION)
  lazy val templateLocation: String = pipeLineConfig.config.getString(APP_TEMPLATELOCATION)


  lazy val TEST_JOB_START_TIME: String = JOB_START_TIME

  lazy val testDmaZipcodeFile: String = "src/test/resources/dma_zip_map.csv"
  lazy val testDmaZipcodeFileRowCount: Long = 1040

  //DMA Zipcode Attributes
  lazy val FIPS = "fips"
  lazy val COUNTY = "county"
  lazy val STATE = "state"
  lazy val DMA_CODE = "dma_code"
  lazy val DMA_NAME = "dma_name"
  lazy val ZIP_CODE = "zipcode"
  lazy val DMAZIPCODETEMPLATESCHEMA: StructType = new StructType()
    .add(FIPS, "string", true)
    .add(COUNTY, "string", true)
    .add(STATE, "string", true)
    .add(DMA_CODE, "string", true)
    .add(DMA_NAME, "string", true)
    .add(ZIP_CODE, "string", true)

  lazy val testFacebookJsonFile: String = "src/test/resources/facebook_datafile.json"
  lazy val testFacebookJsonSchemaFile: String = "src/test/resources/facebook_schema_template.json"
  lazy val testFacebookJsonFileRowCount: Long = 20
  lazy val testFacebookJsonCorruptFile: String = "src/test/resources/facebook_corrupt_file.json"
  lazy val testFacebookCorruptRecordsDir: String = "src/test/data/data-processing-pipeline/corrupt_records/"
  lazy val testPinterestJsonFile: String = "src/test/resources/pinterest-report-2021-11-08_2021-11-08.json"

  lazy val inputPinterestDF: DataFrame = readDynamicJson(testPinterestJsonFile)
  lazy val validPinterestDF: DataFrame = FileReader.handleCorruptRecords(inputPinterestDF, corruptRecordLocation, "json")
  lazy val SRCCOLUMNRENAMEPINTERESTLIST: Seq[(String,String)] = Seq(("CPCV_IN_MICRO_DOLLAR","cpcv_in_dollar"),
    ("CPCV_P95_IN_MICRO_DOLLAR","cpcv_p95_in_dollar"),
    ("CPV_IN_MICRO_DOLLAR","cpv_in_dollar"),
    ("SPEND_IN_MICRO_DOLLAR","spend_in_dollar"),
    ("CAMPAIGN_ID","campaign_id"),
    ("CAMPAIGN_NAME","campaign_name"),
    ("CAMPAIGN_STATUS","campaign_status"),
    ("CLICKTHROUGH_1","clickthrough_1"),
    ("CLICKTHROUGH_1_GROSS","clickthrough_1_gross"),
    ("CLICKTHROUGH_2","clickthrough_2"),
    ("OUTBOUND_CLICK_1","outbound_click_1"),
    ("OUTBOUND_CLICK_2","outbound_click_2"),
    ("ENGAGEMENT_1","engagement_1"),
    ("ENGAGEMENT_2","engagement_2"),
    ("IMPRESSION_1","impression_1"),
    ("IMPRESSION_1_GROSS","impression_1_gross"),
    ("IMPRESSION_2","impression_2"),
    ("REPIN_1","repin_1"),
    ("REPIN_2","repin_2"),
    ("VIDEO_AVG_WATCHTIME_IN_SECOND_1","video_avg_watchtime_in_second_1"),
    ("VIDEO_AVG_WATCHTIME_IN_SECOND_2","video_avg_watchtime_in_second_2"),
    ("VIDEO_MRC_VIEWS_1","video_mrc_views_1"),
    ("VIDEO_MRC_VIEWS_2","video_mrc_views_2"),
    ("VIDEO_P0_COMBINED_1","video_p0_combined_1"),
    ("VIDEO_P0_COMBINED_2","video_p0_combined_2"),
    ("VIDEO_P100_COMPLETE_1","video_p100_complete_1"),
    ("VIDEO_P100_COMPLETE_2","video_p100_complete_2"),
    ("VIDEO_P25_COMBINED_1","video_p25_combined_1"),
    ("VIDEO_P25_COMBINED_2","video_p25_combined_2"),
    ("VIDEO_P50_COMBINED_1","video_p50_combined_1"),
    ("VIDEO_P50_COMBINED_2","video_p50_combined_2"),
    ("VIDEO_P75_COMBINED_1","video_p75_combined_1"),
    ("VIDEO_P75_COMBINED_2","video_p75_combined_2"),
    ("VIDEO_P95_COMBINED_1","video_p95_combined_1"),
    ("VIDEO_P95_COMBINED_2","video_p95_combined_2"),
    ("DATE","report_d") )

  lazy val inputPinterestDFcount: Long = validPinterestDF.count()
  lazy val inputRenamedPinterestDF: DataFrame = renameColumns(validPinterestDF, SRCCOLUMNRENAMEPINTERESTLIST)
  lazy val dqPinterestColumns = Seq("spend_in_dollar","report_d")

  val testConfFile: String = "src/test/resources/citrus_api_actuals_pipeline_test.conf"
  val testConfFileArray: Array[String] = Array("src/test/resources/citrus_api_actuals_pipeline_test.conf")
  val testConfig: PipeLineConfig = MarketingCommonUtils.loadConfigurationFile(testConfFile)

  val errorConfigFile: String = "src/test/resources/citrus_api_actuals_error_test.conf"
  val errorConfFileArray: Array[String] = Array("src/test/resources/citrus_api_actuals_pipeline_error_test.conf")
  val errorConfig: PipeLineConfig =  MarketingCommonUtils.loadConfigurationFile(errorConfigFile)

  lazy val configTest: MarketingPipelineConfigs = MarketingPipelineConfigs(testConfig)


  def makeOrEmptyDir(pathToDir: String): Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val cwd: Path = fs.getWorkingDirectory
    val fullOutPath = MarketingCommonUtils.appendSlashInEnd(new Path(cwd,pathToDir).toString)
    log.info(fullOutPath)
    val mkdirsSuccess: Boolean = fs.mkdirs(new Path(fullOutPath), new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ))
    if (mkdirsSuccess) {
      log.info("Successfully created new directory : " + fullOutPath)
    } else {
      throw new IOException("Failed to create new directory : " + fullOutPath)
    }
  }


  val SFTP_CONFIG_SEQ: Seq[String] = Seq(
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
  val sftpTestConfFile: String = "src/test/resources/impactradius_guest_status_DPP_test.conf"
  val sftpTestConfFileArray: Array[String] = Array("src/test/resources/impactradius_guest_status_DPP_test.conf")
  val sftpTestConfig: PipeLineConfig = MarketingCommonUtils.loadConfigurationFile(sftpTestConfFile,SFTP_CONFIG_SEQ)
  lazy val sftpTestMarketingPipelineConfig: MarketingPipelineConfigs = MarketingPipelineConfigs(sftpTestConfig)

  val sftpErrorConfigFile: String = "src/test/resources/impactradius_guest_status_DPP_error_test.conf"
  val sftpErrorConfFileArray: Array[String] = Array("src/test/resources/impactradius_guest_status_DPP_error_test.conf")
  val sftpErrorConfig: PipeLineConfig =  MarketingCommonUtils.loadConfigurationFile(sftpErrorConfigFile,SFTP_CONFIG_SEQ)
  lazy val sftpErrorMarketingPipelineConfig: MarketingPipelineConfigs = MarketingPipelineConfigs(sftpErrorConfig)



}
