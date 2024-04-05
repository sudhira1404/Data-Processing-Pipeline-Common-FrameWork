package com.tgt.dse.mdf.common.pipeline.transform

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.transform.DmaCodeTransformer.decorateWithDmaCode
import org.scalatest.flatspec.AnyFlatSpec

class DmaCodeTransformerTest extends AnyFlatSpec with MarketingTestHelpers{

  it should "Input Dataset should join to DMA file based on DMA CODE and get DMA NAME" in {

    import spark.implicits._
    val dmaPath="src/test/resources/dma_zip_map.csv"
    val options = Map("header"->"true")
    val inputDmaDF = Seq(
      ("501", "2018-04-01", "p"),
      ("501", "2018-04-01", "q"),
      ("501", "2018-05-01", "r")
    ).toDF("dma_code", "date", "value")

    val joinConditionDma=  "dma_code = right_dma_code"
    val decorateWithDmaCodeTuple = decorateWithDmaCode(inputDmaDF,dmaPath,"csv",joinConditionDma,options,zipCodeLevel = false)
    val decorateWithDmaCodeDf = decorateWithDmaCodeTuple._1
    decorateWithDmaCodeDf.show(false)
    val decorateWithDmaCodeName = decorateWithDmaCodeDf.select("dma_name").distinct().collect().map(_.getString(0)).mkString("")


    assert (decorateWithDmaCodeName matches "NEW YORK")
  }

  it should "Input Dataset should join to DMA file based on ZIP CODE and get DMA NAME" in {

    import spark.implicits._
    val dmaPath="src/test/resources/dma_zip_map.csv"
    val options = Map("header"->"true")
    val inputZipDF = Seq(
      ("00501", "2018-04-01", "p"),
      ("00501", "2018-04-01", "q"),
      ("00501", "2018-05-01", "r")
    ).toDF("zip_code", "date", "value")

    val joinConditionZip=  "zip_code = zipcode"
    val decorateWithDmaCodeZipTuple = decorateWithDmaCode(inputZipDF,dmaPath,"csv",joinConditionZip,options)
    val decorateWithDmaCodeZipDf = decorateWithDmaCodeZipTuple._1
    val decorateWithZipCodeValue = decorateWithDmaCodeZipDf.select("dma_name").distinct().collect().map(_.getString(0)).mkString("")

    assert (decorateWithZipCodeValue matches "NEW YORK")
  }

}
