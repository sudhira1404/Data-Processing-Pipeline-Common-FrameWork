package com.tgt.dse.mdf.common.pipeline.validation

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.exceptions.ContractValidationException
import com.tgt.dse.mdf.common.pipeline.service.FileReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec

class ContractValidatorServiceTest extends AnyFlatSpec with MarketingTestHelpers {
  val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true,
    predeterminedSchema = DMAZIPCODETEMPLATESCHEMA)

  it should "Validate schema against test file dataframe" in {

    val schemaValidation = ContractValidatorService.validateSchema(
      testCsvDF,
      DMAZIPCODETEMPLATESCHEMA
    )
    val validatedSchema: StructType = schemaValidation._1
    val dataFrameWithSchema: DataFrame = schemaValidation._2
    dataFrameWithSchema.printSchema()

    assertResult(DMAZIPCODETEMPLATESCHEMA)(validatedSchema)
  }

  it should "Throw ContractValidationException when schema doesn't match" in {

    val testDMASchema: StructType = new StructType()
      .add("zip_code", "string",true)
      .add("dma_code", "string", true)

    assertThrows[ContractValidationException](ContractValidatorService.validateSchema(
      testCsvDF,
      testDMASchema))
  }

  it should "skip ContractValidationException for extra columns in source" in {

    val DMAZIPCODETEMPLATESCHEMA: StructType = new StructType()
      .add(FIPS, "string", true)
      .add(COUNTY, "string", true)
      .add(STATE, "string", true)
      .add(DMA_CODE, "string", true)
      .add(DMA_NAME, "string", true)

    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true,
      predeterminedSchema = DMAZIPCODETEMPLATESCHEMA)

    val schemaValidation = ContractValidatorService.validateSchema(
      testCsvDF,
      DMAZIPCODETEMPLATESCHEMA,extraColumnsThrowExceptionUponFailure = false
    )
    val validatedSchema: StructType = schemaValidation._1
    val dataFrameWithSchema: DataFrame = schemaValidation._2
    dataFrameWithSchema.printSchema()

    assertResult(DMAZIPCODETEMPLATESCHEMA)(validatedSchema)

  }


  it should "Pass when also checking datatypes" in {

    val schemaValidation = ContractValidatorService.validateSchema(
      testCsvDF,
      DMAZIPCODETEMPLATESCHEMA,
      dataTypeCheck = true
    )
    val validatedSchema: StructType = schemaValidation._1
    val dataFrameWithSchema: DataFrame = schemaValidation._2
    dataFrameWithSchema.printSchema()

    assertResult(DMAZIPCODETEMPLATESCHEMA)(validatedSchema)
  }

  it should "Throw ContractValidationException when datatype mismatch occurs" in {

    val castDF: DataFrame = testCsvDF.withColumn("fips", col("fips").cast("integer").as("fips"))

    assertThrows[ContractValidationException](ContractValidatorService.validateSchema(
      castDF,
      DMAZIPCODETEMPLATESCHEMA,
      dataTypeCheck = true
    ))
  }

  it should "Pass when also checking nulls" in {

    val schemaValidation = ContractValidatorService.validateSchema(
      testCsvDF,
      DMAZIPCODETEMPLATESCHEMA,
      nullableCheck = true
    )
    val validatedSchema: StructType = schemaValidation._1
    val dataFrameWithSchema: DataFrame = schemaValidation._2
    dataFrameWithSchema.printSchema()

    assertResult(DMAZIPCODETEMPLATESCHEMA)(validatedSchema)
  }

  it should "Throw ContractValidationException when nullable mismatch occurs" in {

    val notNullDF: DataFrame = testCsvDF.withColumn("fips", col("fips").isNotNull.as("fips"))

    assertThrows[ContractValidationException](ContractValidatorService.validateSchema(
      notNullDF,
      DMAZIPCODETEMPLATESCHEMA,
      nullableCheck = true
    ))
  }
}
