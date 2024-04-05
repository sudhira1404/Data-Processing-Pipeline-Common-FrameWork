package com.tgt.dse.mdf.common.pipeline.service


import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.transformations.TransFunctions.renameColumns
import com.tgt.dse.mdf.common.pipeline.exceptions.DataQualityException
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.service.DataQualityPublisherGeneric.dqCheck
import com.tgt.dse.mdf.common.pipeline.transform.RevenueTransformer
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers


class DataQualityPublisherGenericTest extends AnyFlatSpec with MarketingTestHelpers with Matchers with GivenWhenThen {


  val REVENUE_COLUMNS: Seq[String] = Seq(
    "CPCV_IN_MICRO_DOLLAR",
    "CPCV_P95_IN_MICRO_DOLLAR",
    "CPV_IN_MICRO_DOLLAR",
    "SPEND_IN_MICRO_DOLLAR"
  )
  val DUPSCHECKKEY: Seq[String] = Seq(
    "report_d","campaign_id","campaign_name","campaign_status")
  val NULLSCHECKKEY: Seq[String] = Seq(
    "report_d","campaign_id","campaign_name","campaign_status")
  val SCALE = 20
  val LOGGERNAME = "pinterest"
  val currencyConvertedDF: DataFrame =
    RevenueTransformer.convertMicroToCurrency(validPinterestDF, REVENUE_COLUMNS)
  val currencyConvertedRenamedDF = renameColumns(currencyConvertedDF, SRCCOLUMNRENAMEPINTERESTLIST)


  it should "not throw DataQualityException when match is true" in {

  val finalDF = dqCheck(inputRenamedPinterestDF, inputRenamedPinterestDF, DataQualityPublisherGeneric(pipeLineConfig,
      dqPinterestColumns,
      DUPSCHECKKEY,
      NULLSCHECKKEY,
      LOGGERNAME,
      reportingDateColumnFlag = true,
      scale = SCALE,
      throwExceptionUponFailure = false,
      nullCheckFlag = true,
      dupCheckFlag = true)
    )

    val result = if(finalDF.columns.contains("nullRowsCount") &&
      finalDF.columns.contains("duplicateRowsCount") &&
      finalDF.columns.contains("srcTgtDiffCount") &&
      finalDF.columns.contains("srcTgtDiffSpend_in_dollar"))
      { true }
    else
      {false }

    assertResult(true)(result)
  }

  it should "throw DataQualityException when match is false" in {
    assertThrows[DataQualityException](dqCheck(inputRenamedPinterestDF, currencyConvertedRenamedDF, DataQualityPublisherGeneric(pipeLineConfig,
      dqPinterestColumns,
      DUPSCHECKKEY,
      NULLSCHECKKEY,
      LOGGERNAME,
      reportingDateColumnFlag = true,
      scale = SCALE,
      throwExceptionUponFailure = true,
      nullCheckFlag = true,
      dupCheckFlag = true)
    )
    )
  }

  it should "not throw DataQualityException when match is false" in {
    dqCheck(inputRenamedPinterestDF, currencyConvertedRenamedDF, DataQualityPublisherGeneric(pipeLineConfig,
      dqPinterestColumns,
      DUPSCHECKKEY,
      NULLSCHECKKEY,
      LOGGERNAME,
      reportingDateColumnFlag = true,
      scale = SCALE,
      throwExceptionUponFailure = false,
      nullCheckFlag = true,
      dupCheckFlag = true)
    )
  }

  it should "thresholdpercent as 2% and Clicks difference between source and target is 1(200-199) and " +
    "thresholdClicks is 2% of source clicks(200) which is 4,so 1 < 4,result should be set to pass" in {
    import spark.implicits._
    //case2with thresholdpercent as 2% and diff is 200-199 < 4(threshold-2*200/100)..pass
    val src =
      """[
        {"campaign_id":"123", "clicks":"100","impressions":"2","spend":"2.5687","report_d":"2021-01-08"},{"campaign_id":"124","clicks":"100","impressions":"2","spend":"3.5687","report_d":"2021-01-08"}
        ]"""


    val tgt =
      """[
         {"campaign_id":"123","clicks":"100","totimpressions":"2","spend":"2.5687","report_d":"2021-01-08"},{"campaign_id":"124","clicks":"99","totimpressions":"2","spend":"3.5688","report_d":"2021-01-08"}
         ]"""
    val dqColumns = Seq("clicks", "impressions", "spend", "report_d")

    val SRCCOLUMNRENAMELIST: Seq[(String, String)] = Seq(("clicks", "clicks"),
      ("impressions", "impressions")
    )

    val TGTCOLUMNRENAMELIST: Seq[(String, String)] = Seq(("clicks", "clicks"),
      ("totimpressions", "impressions")
    )

    val srcInputDF: DataFrame = spark.read.json(Seq(src).toDS()).alias("srcDF")

    val tgtInputDF: DataFrame = spark.read.json(Seq(tgt).toDS()).alias("tgtDF")

    val errorConfigFile: String = "src/test/resources/fb_api_actuals_error_test.conf"

    val pipelineConfig: PipeLineConfig = MarketingCommonUtils.loadConfigurationFile(errorConfigFile)

    val loggerName = "FB"

    val dqThreshold = "{\"clicks\":\"2\",\"impressions\":\"2\",\"spend\":\"2\"}"

    dqCheck(srcInputDF, tgtInputDF, DataQualityPublisherGeneric(pipelineConfig,
      dqColumns = dqColumns,
      loggerName = loggerName,
      reportingDateColumnFlag = true,
      srcColumnrenameList = SRCCOLUMNRENAMELIST,
      tgtColumnrenameList = TGTCOLUMNRENAMELIST,
      scale = SCALE,
      throwExceptionUponFailure = false,
      nullCheckFlag = false,
      dupCheckFlag = false,
      targetTablename = "prd_mdf_fnd.citrus_api_actuals",
      thresholdPercentJson=dqThreshold,
      dqCheckResultShowToLog = true)
    )
  }
}
