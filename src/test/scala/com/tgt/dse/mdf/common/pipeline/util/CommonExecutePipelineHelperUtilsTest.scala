package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dse.mdf.common.pipeline.CommonExecutePipeLineHelpers
import com.tgt.dse.mdf.common.pipeline.constants.CommonExecutePipelineApplicationConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{GeneralException, ZeroRecordInDataframeException}
import com.tgt.dse.mdf.common.pipeline.util.CommonExecutePipelineUtils.{addDataframeToListAndHandleEmptyTopicAndShowResult, addDataframeToListAndHandleZeroRecordAndShowResult, addDataframeToListAndShowResult, addDatePartitionColumn, convertTimestampFromLocalToUtc, convertTimestampFromUtcToLocal, convertTimestampToDate, getBooleanValueFromPipeLineConfig, getDoubleValueFromPipeLineConfig, getIntValueFromPipeLineConfig, getStringValueFromPipeLineConfig, getValueFromPipeLineConfigAndConvertToListOfStringTuple, getValueFromPipeLineConfigAndConvertToMap, getValueFromPipeLineConfigAndConvertToSeq, handleNullsInPKColumns, handleZeroRecordInDataframe, joinCal, pickDFFromListOfDFBasedOnOrderOfExecution, showResultsToLog, stringToDataFrameColumns}
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.dropDup
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.csvStructSchema
import com.tgt.dse.mdf.common.pipeline.vo.CommonExecutePipelineParameterObjects.{CommonPipeLineHelper, JoinCal, ShowResultsToLog}
import com.tgt.dse.mdf.common.pipeline.wrapper.CommonExecutePipelineWrapper.joinCalWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable.ArrayBuffer

class CommonExecutePipelineHelperUtilsTest extends AnyFlatSpec with CommonExecutePipeLineHelpers with Matchers with GivenWhenThen {

  it should "convert timestamp from local to UTC format" in {
    import spark.implicits._
    val inputDF = Seq(
      (1, "2019-06-24T15:36:16.000")
    ).toDF("id", "lastModified")


    val convertTimestampFromLocalToUtcDF = convertTimestampFromLocalToUtc(inputDF,Seq("lastModified"),"yyyy-MM-dd'T'HH:mm:ss.SSS","yyyy-MM-dd HH:mm:ss","America/Chicago")
    val convertTimestampFromLocalToUtcValue = convertTimestampFromLocalToUtcDF.select("lastModified").distinct().collect().map(_.getString(0)).head

    assert(convertTimestampFromLocalToUtcValue matches "2019-06-24 20:36:16")
  }


  it should "convert timestamp from UTC to local format" in {
    import spark.implicits._
    val inputDF = Seq(
      (1, "2019-06-24T15:36:16.000")
    ).toDF("id", "lastModified")


   val convertTimestampFromUtcToLocalDF =  convertTimestampFromUtcToLocal(inputDF,Seq("lastModified"),"yyyy-MM-dd'T'HH:mm:ss.SSS","yyyy-MM-dd HH:mm:ss","America/Chicago")

    val convertTimestampFromUtcToLocalValue = convertTimestampFromUtcToLocalDF.select("lastModified").distinct().collect().map(_.getString(0)).head
    assert(convertTimestampFromUtcToLocalValue matches "2019-06-24 10:36:16")
  }

  it should "convert timestamp to date" in {
    import spark.implicits._
    val inputDF = Seq(
      (1, "2019-06-24T15:36:16.000")
    ).toDF("id", "lastModified")


    val convertTimestampToDateDF = convertTimestampToDate(inputDF,Seq("lastModified"),"yyyy-MM-dd")

    val convertTimestampToDateValue = convertTimestampToDateDF.select("lastModified").distinct().collect().map(_.getString(0)).head

    assert(convertTimestampToDateValue matches "2019-06-24")
  }

  it should "Should remove dups" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","",0.00,1),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","",0.00,1)
    ).toDF("action_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    val dropDupDF =  dropDup(sourceDF,"src/test/Data",
      Seq("action_date", "order_id", "tcin","profile_id","revenue_a","quantity"),"action_date")


    val actualResult = if (dropDupDF.count() == 1) {"pass"} else "fail"

    assert(actualResult matches "pass")

  }

  it should "Join Calendar" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","",0.00,1),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","",0.00,1)
    ).toDF("report_d", "order_id", "tcin","profile_id","revenue_a","quantity")

    val initializeJoinCal = JoinCal(testConfig)
    val initializeCommonPipeLineHelper = CommonPipeLineHelper(testConfig)

    val joinCalDF = joinCalWrapper(sourceDF,initializeJoinCal,initializeCommonPipeLineHelper)
    val calendarColumns: Seq[String] =getValueFromPipeLineConfigAndConvertToSeq(testConfig, CommonExecutePipelineApplicationConstants.APP_JOINCAL_CALENDARCOLUMNS)
    val joinCalSelectDF =joinCalDF.select(calendarColumns.map(c => col(c)):_*)

    val calendarSchemaString = "calendar_d string,day_n string,fiscal_week_n string ,fiscal_week_end_d string,fiscal_week_begin_d string,calendar_fiscal_week string,fiscal_month_id string,fiscal_month_abbreviation_n string,fiscal_month_n string,fiscal_quarter_id string,fiscal_quarter_n string,fiscal_year_id string,fiscal_year_week_id string"
    val csvSchema:StructType = csvStructSchema(calendarSchemaString)
    val calDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],csvSchema)

    val result = if (joinCalSelectDF.schema == calDF.schema) "pass" else "fail"

    assert(result matches "pass")

  }

  it should "pick dataframe from list of dataframes based on order of execution" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","",0.00,1),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","",0.00,1)
    ).toDF("file_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    val listOfDF =  ArrayBuffer[DataFrame]()
    listOfDF += sourceDF;
    val output = pickDFFromListOfDFBasedOnOrderOfExecution("sourceDF,targetDF", "sourceDF", "sourceDF", listOfDF)
    val outputDF = output._1

    val result = if (sourceDF.schema == outputDF.schema) "pass" else "fail"

    assert(result matches "pass")

  }

  it should "create empty dataframe with columns from input comma separated columns" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1"),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1")
    ).toDF("file_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    val outputDF = stringToDataFrameColumns("file_date, order_id,tcin,profile_id,revenue_a,quantity")
    outputDF.printSchema()

    val result = if (sourceDF.schema == outputDF.schema) "pass" else "fail"

    assert(result matches "pass")

  }

  it should "add partition date column to dataframe from column from input dataframe" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1"),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1")
    ).toDF("file_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    val outputDF = addDatePartitionColumn(sourceDF,false,"file_date","report_d")
    outputDF.printSchema()

    val outputValue = outputDF.select("report_d").distinct().collect().map(_.getString(0)).head

    assert(outputValue matches "2022-11-01")

  }


  it should "show dataframe result with select columns" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1"),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1")
    ).toDF("file_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    showResultsToLog(sourceDF,Seq("file_date", "order_id", "tcin","profile_id","revenue_a","quantity"),true,"Showing results",10)


  }

  it should "show dataframe result" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1"),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1")
    ).toDF("file_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    showResultsToLog(sourceDF,true,"Showing results",10)


  }

  it should "get double value from the conf file" in {

    val threshold: Double = getDoubleValueFromPipeLineConfig(testConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_THRESHOLD)
    assert(threshold.toString matches "0.0")

  }

  it should "get int value from the conf file" in {

    val scale: Int = getIntValueFromPipeLineConfig(testConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_SCALE)
    assert(scale.toString matches "0")

  }

  it should "get boolean value from the conf file" in {

    val booleanValue: Boolean =getBooleanValueFromPipeLineConfig(testConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_REPORTINGDATECOLUMNFLAG)

    assert(booleanValue.toString matches "true")

  }

  it should "get string value from the conf file" in {

    val stringValue: String =getStringValueFromPipeLineConfig(testConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_PARTITIONDATECOLNAME)

    assert(stringValue matches "report_d")

  }

  it should "convert json string value from the conf file to list of string tuple" in {

    val listOfStringTuple: Seq[(String, String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(testConfig, CommonExecutePipelineApplicationConstants.APP_VALIDATEDATATYPE_INPUTCOLUMNS)
    val firstValue = listOfStringTuple(0).toString()
    val expectedResult = "(CampaignBudget,decimal(38,2))"
    assert(firstValue == expectedResult)

  }

  it should "convert map string value from the conf file to Map of string" in {

    val mapOfString: Map[String, String] = getValueFromPipeLineConfigAndConvertToMap(testConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_OPTIONS)
    val headerValue = mapOfString.getOrElse("header","nothing")
    assert(headerValue == "true")

  }

  it should "convert string value to sequence of string" in {

    val seqValue: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(testConfig, CommonExecutePipelineApplicationConstants.APP_DROPDUPS_PARTITIONCOLUMNS)
    println(seqValue)

    val expectedResult=Seq("Source", "CompanyName", "AccountId", "CampaignName", "CampaignId", "CampaignStartDate", "CampaignEndDate", "ClassId", "DepartmentId", "VendorNumber", "TCIN", "CalendarYear", "CalendarMonth")
     assert(seqValue == expectedResult)

  }

  it should "put default values to columns" in {
    import spark.implicits._
    val handleNullsInPKColumnsInputDF = Seq(
      (null,null,null)
      ,("456","abc","2023-10-21")
      ,(null,null,"2023-10-21")
      ,(null,"abc","2023-10-21")
    ).toDF("campaign_id", "campaign_name","campaign_date")

    val expectedDF =  Seq(
      ("123","NA","9999-12-31")
        ,("456","abc","2023-10-21")
        ,("123","NA","2023-10-21")
       , ("123","abc","2023-10-21")
    ).toDF("campaign_id", "campaign_name","campaign_date")

    val handleNullsInPKColumnsOutputDF = handleNullsInPKColumns(handleNullsInPKColumnsInputDF, Seq(("campaign_id","123"),("campaign_name","NA"),("campaign_date","9999-12-31")))

    //handleNullsInPKColumnsOutputDF.show(false)
    val actualResultDF = handleNullsInPKColumnsOutputDF.except(expectedDF)

    val actualResult = if (actualResultDF.count() == 0) {
      "pass"
    } else "fail"

    assert(actualResult matches "pass")

  }

  it should "should output the input dataframe while calling handleZeroRecordInDataframe if input dataframe count is greater than zero" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1"),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1")
    ).toDF("file_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)

    val outputDF =  handleZeroRecordInDataframe(sourceDF,"handleZeroRecordInDataframe",false,false,"",commonPipeLineHelper)

    outputDF.show(false)
    //assertThrows[ZeroRecordInDataframeException](handleZeroRecordInDataframe(sourceDF,"handleZeroRecordInDataframe",false,false,"",commonPipeLineHelper))

  }

  it should "should throw ZeroRecordInDataframeException from handleZeroRecordInDataframe method if input dataframe count is equal to zero and abortOnZeroRecords is not set" in {
    import spark.implicits._

    val csvSchemaString = "file_date string,order_id string,tcin string ,profile_id string,revenue_a string,quantity string"
    val csvSchema:StructType = csvStructSchema(csvSchemaString)
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],csvSchema)


    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)

    assertThrows[ZeroRecordInDataframeException](handleZeroRecordInDataframe(emptyDF,"handleZeroRecordInDataframe",false,false,"",commonPipeLineHelper))

  }

  it should "should throw GeneralException from handleZeroRecordInDataframe method if input dataframe count is equal to zero and abortOnZeroRecords is set " in {
    import spark.implicits._

    val csvSchemaString = "file_date string,order_id string,tcin string ,profile_id string,revenue_a string,quantity string"
    val csvSchema:StructType = csvStructSchema(csvSchemaString)
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],csvSchema)


    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)

    assertThrows[GeneralException](handleZeroRecordInDataframe(emptyDF,"handleZeroRecordInDataframe",true,false,"",commonPipeLineHelper))

  }

  it should "should add Dataframe To List And HandleZeroRecord without abort And pick right dataframe from the list and  ShowResult" in {
    import spark.implicits._

    val sourceInputDF = Seq(
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1"),
      ("2022-11-01", "4f5a9ca4n5a5d11ed9195edd67f796c65", "","","0.00","1")
    ).toDF("file_date", "order_id", "tcin","profile_id","revenue_a","quantity")

    val anotherInputDF = Seq(
      ("2022-11-01"),
      ("2022-11-01")
    ).toDF("file_date")

    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)
    val showResultsToLog = ShowResultsToLog(testConfig)
    val listOfDF = ArrayBuffer[DataFrame]()
    listOfDF+=anotherInputDF
    addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF,sourceInputDF,"sourceInput",true,commonPipeLineHelper,showResultsToLog)

    log.info("size of list is=>" + listOfDF.size)
    val anotherOutputDF = listOfDF(0)
    val sourceOutputDF = listOfDF(1)

    anotherOutputDF.show(10)
    sourceOutputDF.show(10)


    val anotherExceptDF = anotherInputDF.except(anotherOutputDF)

    val anotherActualResult = if (anotherExceptDF.count() == 0) {
      "pass"
    } else "fail"

    val sourceExceptDF = sourceInputDF.except(sourceOutputDF)

    val sourceActualResult = if (sourceExceptDF.count() == 0) {
      "pass"
    } else "fail"


    assert(anotherActualResult matches "pass")
    assert(sourceActualResult matches "pass")


  }

  it should "should add Dataframe To List And HandleZeroRecord by throwing GeneralException" in {
    import spark.implicits._

    val csvSchemaString = "file_date string,order_id string,tcin string ,profile_id string,revenue_a string,quantity string"
    val csvSchema:StructType = csvStructSchema(csvSchemaString)
    val sourceInputDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],csvSchema)
    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)
    val showResultsToLog = ShowResultsToLog(testConfig)

    val listOfDF = ArrayBuffer[DataFrame]()
    listOfDF+=sourceInputDF

    assertThrows[GeneralException](addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF,sourceInputDF,"sourceInput",true,commonPipeLineHelper,showResultsToLog)
    )


  }

  it should "should add Dataframe To List And HandleZeroRecord by throwing ZeroRecordInDataframeException" in {
    import spark.implicits._

    val csvSchemaString = "file_date string,order_id string,tcin string ,profile_id string,revenue_a string,quantity string"
    val csvSchema:StructType = csvStructSchema(csvSchemaString)
    val sourceInputDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],csvSchema)
    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)
    val showResultsToLog = ShowResultsToLog(testConfig)

    val listOfDF = ArrayBuffer[DataFrame]()
    listOfDF+=sourceInputDF

    assertThrows[ZeroRecordInDataframeException](addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF,sourceInputDF,"sourceInput",false,commonPipeLineHelper,showResultsToLog)
    )

  }


  it should "should add Dataframe To List and source has zero records and no handling of zero records in dataframe and pick source input dataframe from the list and  ShowResult" in {
    import spark.implicits._

    val csvSchemaString = "file_date string,order_id string,tcin string ,profile_id string,revenue_a string,quantity string"
    val csvSchema:StructType = csvStructSchema(csvSchemaString)
    val sourceInputDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],csvSchema)
    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)
    val showResultsToLog = ShowResultsToLog(testConfig)

    val listOfDF = ArrayBuffer[DataFrame]()
    listOfDF+=sourceInputDF

    addDataframeToListAndShowResult(listOfDF,sourceInputDF,"sourceInput",commonPipeLineHelper,showResultsToLog)

    val sourceOutputDF = listOfDF(0)

    val sourceActualResult = if (sourceOutputDF.count() == 0) {
      "pass"
    } else "fail"


    assert(sourceActualResult matches "pass")


  }

  it should "should add Dataframe To List And HandleEmptyTopic by throwing GeneralException with abortOnZeroRecord is set" in {
    import spark.implicits._

    val csvSchemaString = "file_date string,order_id string,tcin string ,profile_id string,revenue_a string,quantity string"
    val csvSchema: StructType = csvStructSchema(csvSchemaString)
    val sourceInputDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], csvSchema)
    val commonPipeLineHelper = CommonPipeLineHelper(testConfig)
    val showResultsToLog = ShowResultsToLog(testConfig)

    val listOfDF = ArrayBuffer[DataFrame]()
    listOfDF += sourceInputDF


    assertThrows[GeneralException](addDataframeToListAndHandleEmptyTopicAndShowResult(listOfDF, sourceInputDF, "sourceInput", true, false, "", commonPipeLineHelper, showResultsToLog)
    )
  }
}