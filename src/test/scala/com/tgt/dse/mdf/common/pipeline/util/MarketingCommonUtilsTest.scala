package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.rounding
import com.typesafe.config.ConfigFactory
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.util.{Failure, Success, Try}

class MarketingCommonUtilsTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {

  "The MarketingCommonUtilsTest object" should "do the following"

  it should "Append slash in the end when incoming path does not have slash" in {
    val newpath: String = MarketingCommonUtils.appendSlashInEnd("/user/SVMDEDMD/hive/fb_api_actuals/stage")
    val result: String = "/user/SVMDEDMD/hive/fb_api_actuals/stage/"
    assert (result matches newpath)
  }

  it should "Not append slash in the end when incoming path ends with a slash" in {
    val newpath: String = MarketingCommonUtils.appendSlashInEnd("/user/SVMDEDMD/hive/fb_api_actuals/stage/")
    val result: String = "/user/SVMDEDMD/hive/fb_api_actuals/stage/"
    assert (result matches newpath)
  }

  it should "Create a Spark Session in local env" in {
    MarketingCommonUtils.createSparkSession(pipeLineConfig,"local")
  }

  it should "Create a Spark Session in stg env" in {
    MarketingCommonUtils.createSparkSession(pipeLineConfig,env)
  }

  it should "Create a Spark Session in prod env" in {
    MarketingCommonUtils.createSparkSession(errorPipeLineConfig,"prod")
  }

  it should "load a configuration error file as expected" in {
    ConfigFactory.invalidateCaches()

    val emptyConfigPath: String = "src/test/resources1/fb_api_actuals_DPP_test.conf"
    System.setProperty("config.file", emptyConfigPath)

    val pipelineConfig = Try(MarketingCommonUtils.loadConfigurationFile(emptyConfigPath)) match {
      case Failure(_) => false
      case _          => true
    }

    assertResult(false)(pipelineConfig)
  }
  it should "exit gracefully when passed Array of String into Usage Function" in {
    val array: Array[String] = new Array[String](3)
    val intResult = Try(MarketingCommonUtils.usage(array))

    val result = intResult match {
      case _: Success[t] => Success
      case _             => Failure
    }

    assertResult(Success)(result)
  }

  it should "log error when passed Array of String with 0 elements into usage function" in {
    val array: Array[String] = new Array[String](0)
    val intResult = Try(MarketingCommonUtils.usage(array))

    val result = intResult match {
      case _: Success[t] => Success
      case _             => Failure
    }

    assertResult(Failure)(result)
  }

  it should "return true for a valid Usage" in {
    val result = Try(MarketingCommonUtils.usage(Array(configPath))) match {
      case Failure(_) => false
      case _ => true
    }
    println(result)
    assertResult(true)(result)
  }

  it should "Should round value based on roundConditionValue." +
    "If value is 2.4 that needs to be rounded and condition is 0.4, " +
    "then any decimal part of 2.4  is less than  0.4 will result is 3" in {

    val result = rounding(2.4, 0.3)

    assert(result.toString matches "3")

  }

  it should "Should round value based on roundConditionValue." +
    "If value is 2.4 that needs to be rounded and condition is 0.3, " +
    "then any decimal part of 2.4  is less than  0.6 will result is 3 else 2" in {

    val result = rounding(2.5, 0.6)

    assert(result.toString matches "2")

  }

}
