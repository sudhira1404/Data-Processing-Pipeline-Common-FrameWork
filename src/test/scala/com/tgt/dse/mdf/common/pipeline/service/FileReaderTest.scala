package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.service.FileReader.handleCorruptRecords
import org.apache.spark.sql.DataFrame
import org.scalatest.Outcome
import org.scalatest.flatspec.AnyFlatSpec

class FileReaderTest extends AnyFlatSpec with MarketingTestHelpers{
  override def withFixture(test: NoArgTest): Outcome = {

    //Setup
    makeOrEmptyDir(testFacebookCorruptRecordsDir)

    //Execute tests
    try test()

    //Cleanup
    finally {
      //makeOrEmptyDir(testFacebookCorruptRecordsDir)
    }
  }



  it should "Read a csv to dataframe with inferred schema and no header" in {
    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv")
    testCsvDF.printSchema()
    assertResult(testDmaZipcodeFileRowCount + 1)(testCsvDF.count())
  }

  it should "Read a csv to dataframe with inferred schema and header" in {
    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true)
    testCsvDF.printSchema()
    assertResult(testDmaZipcodeFileRowCount)(testCsvDF.count())
  }

  it should "Read a csv to dataframe with pre-defined schema and header" in {

    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true,
      predeterminedSchema = DMAZIPCODETEMPLATESCHEMA)
    testCsvDF.printSchema()
    assertResult(testDmaZipcodeFileRowCount)(testCsvDF.count())
  }

  it should "Read a csv to dataframe with pre-defined schema and header, and select a subset of columns" in {

    val columnList: Seq[String] = Seq(ZIP_CODE,DMA_CODE)

    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true,
      predeterminedSchema = DMAZIPCODETEMPLATESCHEMA, selectColumns = columnList)
    testCsvDF.printSchema()
    assertResult(testDmaZipcodeFileRowCount)(testCsvDF.count())
  }

  it should "Read a csv to dataframe with pre-defined schema and header, and filter on a condition" in {

    val filterPhrase: String = "state='MN'"
    val testCsvDF: DataFrame = FileReader.readFileToDataFrame(testDmaZipcodeFile,"csv", hasHeader = true,
      predeterminedSchema = DMAZIPCODETEMPLATESCHEMA, filterCondition = filterPhrase)
    assertResult(1031)(testCsvDF.count())
  }

  it should "Read a json to dataframe with schema template" in {
    val testJsonDF: DataFrame = FileReader.readFileToDataFrame(testFacebookJsonFile, "json",
      templateLocation = testFacebookJsonSchemaFile)
    assertResult(testFacebookJsonFileRowCount)(testJsonDF.count())
  }

  it should "Read a json to dataframe with inferred schema, and filter corrupt records" in {
    val testJsonDF: DataFrame = FileReader.readFileToDataFrame(testFacebookJsonCorruptFile, "json",
      corruptRecordLocation = testFacebookCorruptRecordsDir)
    testJsonDF.printSchema()
    assertResult(9)(testJsonDF.count())
  }

  it should "Read a json to dataframe with schema template, and filter corrupt records" in {
    val testJsonDF: DataFrame = FileReader.readFileToDataFrame(testFacebookJsonCorruptFile, "json",
      templateLocation = testFacebookJsonSchemaFile, corruptRecordLocation = testFacebookCorruptRecordsDir)
    testJsonDF.printSchema()
    assertResult(9)(testJsonDF.count())
  }

  it should "handle corrupt records" in {
    val count = 2
    makeOrEmptyDir(corruptRecordLocation)
    val validPinterestDF: DataFrame = handleCorruptRecords(inputPinterestDF, corruptRecordLocation, "json")
    val countValidPinterestDF = validPinterestDF.count()
    assertResult(count)(countValidPinterestDF)
  }
}
