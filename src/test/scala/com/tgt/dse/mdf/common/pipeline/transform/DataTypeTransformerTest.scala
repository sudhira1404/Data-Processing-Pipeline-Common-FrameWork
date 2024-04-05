package com.tgt.dse.mdf.common.pipeline.transform

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.service.FileReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.scalatest.flatspec.AnyFlatSpec

class DataTypeTransformerTest extends AnyFlatSpec with MarketingTestHelpers {

  it should "Properly cast specified fields to ints" in {
    val columns = Seq(FIPS,DMA_CODE,ZIP_CODE)
    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true,
      predeterminedSchema = DMAZIPCODETEMPLATESCHEMA)
    val dataType = IntegerType
    val castDF: DataFrame = DataTypeTransformer.castToDataType(testCsvDF, columns, dataType)
    assertResult(true)(castDF.schema(FIPS).dataType.typeName == "integer")
    assertResult(true)(castDF.schema(COUNTY).dataType.typeName == "string")
    assertResult(true)(castDF.schema(STATE).dataType.typeName == "string")
    assertResult(true)(castDF.schema(DMA_CODE).dataType.typeName == "integer")
    assertResult(true)(castDF.schema(DMA_NAME).dataType.typeName == "string")
    assertResult(true)(castDF.schema(ZIP_CODE).dataType.typeName == "integer")
  }
}
