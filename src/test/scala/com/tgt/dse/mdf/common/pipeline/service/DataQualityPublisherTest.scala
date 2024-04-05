package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.DataQualityException
import com.tgt.dse.mdf.common.pipeline.transform.RevenueTransformer
import com.tgt.dse.mdf.common.pipeline.util.DPPMetricUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class DataQualityPublisherTest extends AnyFlatSpec with MarketingTestHelpers {
  val INPUT_HEADER: Seq[String] = Seq(FIPS,COUNTY,STATE,DMA_CODE,DMA_NAME,ZIP_CODE)
  val testDmaZipDF: DataFrame = FileReader
    .readFileToDataFrame(
      testDmaZipcodeFile,
      fileFormat = "csv",
      hasHeader = true,
      renameColumns = INPUT_HEADER)
  val PRIMARY_KEY_COLUMNS: Seq[String] = Seq(STATE,DMA_CODE,ZIP_CODE)
  val REVENUE_COLUMN: String = FIPS
  val transformedDF: DataFrame = RevenueTransformer.convertMicroToCurrency(testDmaZipDF, Seq(REVENUE_COLUMN))

  it should "Send timeliness, lineage, & quality to DataPortal/DataTrust" in {
    DPPMetricUtils.initializeMetricCollector (pipeLineConfig)
    val fndTable: String = pipeLineConfig.config.getString(MarketingConstants.APP_coreHistTable)
    log.info(s"Foundation table is $fndTable")
    DPPMetricUtils.setCDSRowsCount(99)
  }

  it should "not throw DataQualityException when micro-currency revenue is true" in {

    DataQualityPublisher.dqCheck(pipeLineConfig, testDmaZipDF, transformedDF,
      DataQualityPublisher(PRIMARY_KEY_COLUMNS, REVENUE_COLUMN, DMA_CODE, ZIP_CODE, DMA_NAME, "test", isRevenueMicro = true))
  }

  it should "throw DataQualityException when micro-currency revenue is false and yet targetDF is micro-currency" in {
    assertThrows[DataQualityException](DataQualityPublisher.dqCheck(pipeLineConfig, testDmaZipDF, transformedDF,
      DataQualityPublisher(PRIMARY_KEY_COLUMNS, REVENUE_COLUMN,DMA_CODE, ZIP_CODE, DMA_NAME, "test", throwExceptionUponFailure = true, isRevenueMicro = false)))
  }

  it should "not throw DataQualityException when micro-currency revenue is false and yet targetDF is micro-currency, but throw option is disabled" in {
    DataQualityPublisher.dqCheck(pipeLineConfig, testDmaZipDF, transformedDF,
      DataQualityPublisher(PRIMARY_KEY_COLUMNS, REVENUE_COLUMN, DMA_CODE, ZIP_CODE, DMA_NAME, "test", throwExceptionUponFailure = false, isRevenueMicro = false))
  }

  it should "not throw DataQualityException when micro-currency revenue is true and yet targetDF is not micro-currency but throw option is disabled" in {
    DataQualityPublisher.dqCheck(pipeLineConfig, testDmaZipDF, testDmaZipDF,
      DataQualityPublisher(PRIMARY_KEY_COLUMNS, REVENUE_COLUMN, DMA_CODE, ZIP_CODE, DMA_NAME, "test", throwExceptionUponFailure = false, isRevenueMicro = true))
  }

}
