package com.tgt.dse.mdf.common.pipeline

import com.tgt.dsc.kelsa.datapipeline.core.constants.DPPMetricsConstants
import com.tgt.dsc.kelsa.datapipeline.core.exceptions.LockFileException
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dsc.kelsa.datapipeline.core.util.LockServiceHelper
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{ContractValidationException, DataQualityException, DateFormatException, FileCopyException, FileMoveException, HiveWriteException, InvalidPipeLineOptionException, SchemaException}
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils
import com.typesafe.config.ConfigException
import org.scalatest.flatspec.AnyFlatSpec

class MarketingPipelineMainTraitTest extends AnyFlatSpec with MarketingTestHelpers with MarketingPipelineMainTrait {

  deleteLock("src/test/data/data-processing-pipeline/","FBApiDataPipeline")
  val config: MarketingPipelineConfigs = loadPipeLineConfigurations(Array(configPath))

  it should "Create and delete the lock file" in {
    val lockPath: String = "src/test/resources/"
    val appName: String = "testApp"
    val lockFile: String = createLock(lockPath,appName)
    assertResult(true)(LockServiceHelper.isLockPresent(lockPath,appName))
    log.info(s"Created lock file: $lockFile")
    deleteLock(lockPath,appName)
    assertResult(false)(LockServiceHelper.isLockPresent(lockPath,appName))
  }


  it should "Throw LockFileException if lock file already exists" in {
    val lockPath: String = "src/test/resources/"
    val appName: String = "testApp"
    createLock(lockPath,appName)
    assertThrows[LockFileException](createLock(lockPath, appName))
    deleteLock(lockPath, appName)
    assertResult(false)(LockServiceHelper.isLockPresent(lockPath,appName))
  }

  it should "Initialize MarketingPipelineConfigs case class" in {
    assertResult(pipeLineConfig.config.getString (MarketingConstants.APP_NAME))(config.appName)
    assertResult(pipeLineConfig.config.getString (MarketingConstants.APP_ENV))(config.env)
    assertResult(pipeLineConfig.config.getString (MarketingConstants.APP_BLOSSOMID))(config.blossomID)
    assertResult(pipeLineConfig.config.getString (MarketingConstants.APP_DQCHANNELID))(config.dqChannelId)
    assertResult(pipeLineConfig.config.getString (MarketingConstants.APP_ALERTCHANNELID))(config.alertChannelId)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_INFLUXDB_MEASUREMENTNAME))(config.influxDbMeasurementName)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_INFLUXDB_GRAFANAENDPOINT))(config.influxDbEndpoint)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_SLA))(config.sla)
    assertResult(pipeLineConfig.config.getString(MarketingConstants.APP_CONF_JCEKS_FILE_PATH))(config.jceksPath)
    val configLockPath = MarketingCommonUtils.appendSlashInEnd(pipeLineConfig.config.getString(MarketingConstants.APP_LOCKLOCATION))
    assertResult(configLockPath)(config.lockPath)
    log.info(s"Job start time is: ${config.JOB_START_TIME}")
    assertResult(true)(config.JOB_START_TIME.nonEmpty)
    deleteLock(config.lockPath,config.appName)
  }

  it should "Set Final Status and status to success" in {
    publishSuccess(config)
    assertResult("\"" + MarketingConstants.SUCCESS + "\"")(PipeLineMetadataService.getStringMetricValue(DPPMetricsConstants.DPPJOBSTATUS))
  }

  it should "Throw ConfigException if a config is missing" in {
    assertThrows[ConfigException](MarketingPipelineConfigs(errorPipeLineConfig))
  }

  it should "Set metric status to failure" in {
    exceptionError(DataQualityException("this is a test DQ exception"),"data quality",config)
    assertResult("\"" + MarketingConstants.FAILURE + "\"")(PipeLineMetadataService.getStringMetricValue(DPPMetricsConstants.DPPJOBSTATUS))
  }

  it should "handle Lock File exceptions" in {
    val exception = LockFileException("this is a test of handling a LockFileException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle Data Quality exceptions" in {
    val exception = DataQualityException("this is a test of handling a DataQualityException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle Date Formatting exceptions" in {
    val exception = DateFormatException("this is a test of handling a DateFormatException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle File Move exceptions" in {
    val exception = FileMoveException("this is a test of handling a FileMoveException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle File Copy exceptions" in {
    val exception = FileCopyException("this is a test of handling a FileCopyException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle Schema exceptions" in {
    val exception = SchemaException("this is a test of handling a SchemaException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle Contract Validation exceptions" in {
    val exception = ContractValidationException("this is a test of handling a ContractValidationException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle Invalid Pipeline Option exceptions" in {
    val exception = InvalidPipeLineOptionException("this is a test of handling an InvalidPipeLineOptionException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle Hive writing exceptions" in {
    val exception = HiveWriteException("this is a test of handling a HiveWriteException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "handle general exceptions" in {
    val exception = new NullPointerException("this is a test of handling a NullPointerException")
    exceptionHandler(exception,MarketingPipelineConfigs(pipeLineConfig))
  }

  it should "Set final count to 99, sla from configs, and load completion time to now" in {
    val MOCK_COUNT: Long = 99
    PipeLineMetadataService.setLongMetricValue(MarketingConstants.FINALOUTPUTROWS,MOCK_COUNT)
    finalizePipelineRun(config,test = true)
    assertResult(MOCK_COUNT)(PipeLineMetadataService.getLongMetricValue(MarketingConstants.FINALOUTPUTROWS))
  }

}
