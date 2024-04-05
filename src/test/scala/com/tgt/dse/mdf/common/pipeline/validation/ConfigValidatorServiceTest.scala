package com.tgt.dse.mdf.common.pipeline.validation

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils
import com.tgt.dse.mdf.common.pipeline.validation.ConfigValidatorService.validateConfigurations
import org.scalatest.flatspec.AnyFlatSpec

class ConfigValidatorServiceTest extends AnyFlatSpec with MarketingTestHelpers  {

  "The ConfigValidatorServiceTest object" should "do the following"

  it should "validate all the config specified" in {

    val configPath: String = "src/test/resources/fb_api_actuals_pipeline_test.conf"
    System.setProperty("config.file", configPath)

    val testpipelineConfig: PipeLineConfig = MarketingCommonUtils.loadConfigurationFile(configPath)
    val validTestConfig: Boolean = ConfigValidatorService.validateConfigProcessing (testpipelineConfig)

    assertResult (true)(validTestConfig)
  }

  it should "return true for a valid pipeline Config" in {
    val result = validateConfigurations(pipeLineConfig)
    assertResult(true)(result)
  }

}