package com.tgt.dse.mdf.common.pipeline.cleanse

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.service.FileReader
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class MessageCleanserTest extends AnyFlatSpec with MarketingTestHelpers {

  it should "Remove duplicates" in {
    val testDmaZipcodeDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv",hasHeader = true)

    val duplicatedDF: DataFrame = testDmaZipcodeDF.union(testDmaZipcodeDF)

    val distinctDF:DataFrame = MessageCleanser.removeDuplicates(duplicatedDF)

    assertResult(testDmaZipcodeFileRowCount)(distinctDF.count())
  }
}
