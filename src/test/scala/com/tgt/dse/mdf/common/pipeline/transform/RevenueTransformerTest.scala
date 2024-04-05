package com.tgt.dse.mdf.common.pipeline.transform

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.service.FileReader
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class RevenueTransformerTest extends AnyFlatSpec with MarketingTestHelpers {
  //GAM Dimensions
  val REPORT_DATE = "report_d"
  val LINE_ITEM_ID = "line_item_id"
  val LINE_ITEM_NAME = "line_item_name"
  val LINE_ITEM_TYPE = "line_item_type"
  val POSTAL_CODE_CRITERIA_ID = "postal_code_criteria_id"
  val POSTAL_CODE = "postal_code"
  val ORDER_ID = "order_id"
  val ORDER_NAME = "order_name"
  //GAM DimensionAttributes
  val LINE_ITEM_START_DATETIME = "line_item_start_datetime"
  val LINE_ITEM_END_DATETIME = "line_item_end_datetime"
  val LINE_ITEM_LIFETIME_IMPRESSIONS = "line_item_lifetime_impressions"
  val LINE_ITEM_LIFETIME_CLICKS = "line_item_lifetime_clicks"
  val LINE_ITEM_DELIVERY_INDICATOR = "line_item_delivery_indicator"
  val LINE_ITEM_DELIVERY_PACING = "line_item_delivery_pacing"
  val ORDER_PO_NUMBER = "order_po_number"
  val ORDER_START_DATETIME = "order_start_datetime"
  val ORDER_END_DATETIME = "order_end_datetime"
  val ORDER_LIFETIME_IMPRESSIONS = "order_lifetime_impressions"
  val ORDER_LIFETIME_CLICKS = "order_lifetime_clicks"
  //GAM Metrics aka Columns
  val AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS = "ad_exchange_line_item_level_impressions"
  val AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS = "ad_exchange_line_item_level_clicks"
  val AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE = "ad_exchange_line_item_level_revenue"
  val AD_SERVER_IMPRESSIONS = "ad_server_impressions"
  val AD_SERVER_CLICKS = "ad_server_clicks"
  val AD_SERVER_WITHOUT_CPD_AVERAGE_ECPM = "ad_server_without_cpd_average_ecpm"
  val AD_SERVER_CTR = "ad_server_ctr"
  val AD_SERVER_CPM_AND_CPC_REVENUE = "ad_server_cpm_and_cpc_revenue"
  val AD_SERVER_CPM_AND_CPC_REVENUE_GROSS = "ad_server_cpm_and_cpc_revenue_gross"
  val TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS = "total_line_item_level_impressions"
  val TOTAL_LINE_ITEM_LEVEL_CLICKS = "total_line_item_level_clicks"
  val TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE = "total_line_item_level_cpm_and_cpc_revenue"
  val TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM = "total_line_item_level_without_cpd_average_ecpm"
  val TOTAL_LINE_ITEM_LEVEL_CTR = "total_line_item_level_ctr"

  val INPUTGAMRENAMEDHEADER: Seq[String] = Seq(
    REPORT_DATE,
    LINE_ITEM_ID,
    LINE_ITEM_NAME,
    LINE_ITEM_TYPE,
    POSTAL_CODE_CRITERIA_ID,
    POSTAL_CODE,
    ORDER_ID,
    ORDER_NAME,
    LINE_ITEM_START_DATETIME,
    LINE_ITEM_END_DATETIME,
    LINE_ITEM_LIFETIME_IMPRESSIONS,
    LINE_ITEM_LIFETIME_CLICKS,
    LINE_ITEM_DELIVERY_INDICATOR,
    LINE_ITEM_DELIVERY_PACING,
    ORDER_PO_NUMBER,
    ORDER_START_DATETIME,
    ORDER_END_DATETIME,
    ORDER_LIFETIME_IMPRESSIONS,
    ORDER_LIFETIME_CLICKS,
    AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS,
    AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS,
    AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE,
    AD_SERVER_IMPRESSIONS,
    AD_SERVER_CLICKS,
    AD_SERVER_WITHOUT_CPD_AVERAGE_ECPM,
    AD_SERVER_CTR,
    AD_SERVER_CPM_AND_CPC_REVENUE,
    AD_SERVER_CPM_AND_CPC_REVENUE_GROSS,
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS,
    TOTAL_LINE_ITEM_LEVEL_CLICKS,
    TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE,
    TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM,
    TOTAL_LINE_ITEM_LEVEL_CTR)

  val REVENUE_COLUMNS: Seq[String] = Seq(
    AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE,
    AD_SERVER_WITHOUT_CPD_AVERAGE_ECPM,
    AD_SERVER_CPM_AND_CPC_REVENUE,
    AD_SERVER_CPM_AND_CPC_REVENUE_GROSS,
    TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE,
    TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM
  )

  it should "Convert listed fields in DataFrame from whole numbers to floats" in {
    val testGamPartFile: String = "src/test/resources/gam-report-2021-05-02_2021-05-02_2.csv"
    val testGamPartFileRowCount: Long = 99

    val testGamDF: DataFrame = FileReader
      .readFileToDataFrame(
        testGamPartFile,
        fileFormat = "csv",
        hasHeader = true,
        renameColumns = INPUTGAMRENAMEDHEADER)

    testGamDF.printSchema()
    testGamDF.show(10)
    val newDF: DataFrame = RevenueTransformer.convertMicroToCurrency(
      testGamDF, REVENUE_COLUMNS
    )

    newDF.printSchema()
    newDF.show(10)
    assertResult(testGamPartFileRowCount)(newDF.count())
  }
}
