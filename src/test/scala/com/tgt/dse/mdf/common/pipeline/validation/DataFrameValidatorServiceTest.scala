package com.tgt.dse.mdf.common.pipeline.validation

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.exceptions.DateFormatException
import com.tgt.dse.mdf.common.pipeline.validation.DataFrameValidatorService.{validateDataTypes, validateDateFormat, validateNOTNULLColumns}
import org.apache.spark.sql.DataFrame
import org.scalatest.Outcome
import org.scalatest.flatspec.AnyFlatSpec

class DataFrameValidatorServiceTest extends AnyFlatSpec with MarketingTestHelpers {

  override def withFixture(test: NoArgTest): Outcome = {

    //Setup
    makeOrEmptyDir(corruptRecordLocation)

    //Execute tests
    try test()

      //Cleanup
    finally {
      //makeOrEmptyDir(testFacebookCorruptRecordsDir)
    }
  }


  it should "validate not null columns " in {
    val count = 2
    val APP_NOTNULLSOURCECOLUMNS = Seq("CAMPAIGN_ID", "CAMPAIGN_NAME", "DATE")
    //makeOrEmptyDir(corruptRecordLocation)
    val notnullValiddDF: DataFrame = validateNOTNULLColumns(validPinterestDF, corruptRecordLocation, APP_NOTNULLSOURCECOLUMNS)
    val countNotnullValiddDF = notnullValiddDF.count()

    assertResult(count)(countNotnullValiddDF)
  }

  it should "validate datatypes  " in {
    val count = 2
    val COLUMNS_WITH_TYPE_FOR_DATATYPE_VALIDATION: Seq[(String, String)] = Seq(("CLICKTHROUGH_1", "bigint"),
      ("CLICKTHROUGH_2", "bigint"), ("IMPRESSION_1", "bigint"), ("IMPRESSION_2", "bigint"),
      ("ENGAGEMENT_1", "bigint"), ("ENGAGEMENT_2", "bigint"))
    //makeOrEmptyDir(corruptRecordLocation)
    val datatypeValiddDF: DataFrame = validateDataTypes(validPinterestDF, corruptRecordLocation, COLUMNS_WITH_TYPE_FOR_DATATYPE_VALIDATION)
    val countDatatypeValiddDF = datatypeValiddDF.count()
    assertResult(count)(countDatatypeValiddDF)
  }

  it should "validate data format" in {
    val count = 2
    val VALIDATEDATECOLUMNS: Seq[String] = Seq("DATE")
    //makeOrEmptyDir(corruptRecordLocation)
    val dateFormatDF = validateDateFormat(inputPinterestDF, VALIDATEDATECOLUMNS, "yyyy-MM-dd")
    val countValidPinterestDF = dateFormatDF.count()
    assertResult(count)(countValidPinterestDF)
  }

  it should "validate date format" in {
    import spark.implicits._
    val inputDf = Seq(
      (1, "2018-04-01", "p"),
      (1, "2018-04-01", "q"),
      (2, "2018-05-10", "r")
    ).toDF("id", "date", "value")

    val dateFormatDF = validateDateFormat(inputDf, Seq("date"), "yyyy-MM-dd")

    dateFormatDF.show(false)
  }

  it should "validate date format and throw exception" in {

    assertThrows[DateFormatException] {
      import spark.implicits._
      val inputDf = Seq(
        (1, "2018-04-01", "p"),
        (1, "2018-04-01", "q"),
        (2, "2018-05-101", "r")
      ).toDF("id", "date", "value")

      validateDateFormat(inputDf, Seq("date"), "yyyy-MM-dd")
    }
  }




}
