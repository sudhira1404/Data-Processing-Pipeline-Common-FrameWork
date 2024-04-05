package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.service.FileReader.readFileToDataFrame
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.{addFileDateElseDefaultToDataframe, addFileDateToDataframe, addFirstOfMonthFromFileDateToDataframe, addFirstOfMonthInEpochFromFileDateToDataframe, calculateFileSplit, commonColumnNameChangeRightDataframe, convertDateFormat, convertNullToDefault, convertToSparkDateTypeFormat, countByFilePartitionDateAndCollect, countByPartitionDateAndCollect, dataFrameToEdgeNodeAndConsolidateAddHeader, dropDup, executeInsertOverWriteFromDF, extractDateFromTimestamp, getCountOfDistinctFileDateFromInputFile, getLatestByPartitionDateArray, getLatestByPartitionDateFileDate, getLatestByPartitionWithTwoOrderByCols, intColumnDiff, regexExtractDateToDataframe, removeDollarCharacter, removeNonUtfCharacter, renameAllColumnsToLowerCase, replaceNullWithDefault, subtractDateFromDaysAttribute, subtractDaysFromFileDateInEpoch, subtractDaysFromFileDateToDataframe, validateSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.scalatest.flatspec.AnyFlatSpec

class DataFrameUtilsTest extends AnyFlatSpec with MarketingTestHelpers {
  it should "Adds file date from inputDF based on input_file_name()( with input dateformat YYYY-MM-DD or YYYYMMDD),if match" in {
    val sourceFilePath = "src/test/resources/snapchat-stats-report.json"
    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val dateFormat = "YYYY-MM-DD"

    val actualResultDF = addFileDateElseDefaultToDataframe(inputDF, dateFormat, "file_date")

    val actualResult = actualResultDF.select("file_date").distinct().collect().map(_.getString(0)).mkString("")

    val expectResult = java.time.LocalDate.now.toString

    assert(actualResult matches expectResult)

  }

  it should "Adds file date from inputDF based on input_file_name()( with input dateformat YYYY-MM-DD or YYYYMMDD).If no match, defaulted to YYYY-MM-DD" in {
    val sourceFilePath = "src/test/resources/snapchat-stats-report-2021-11-30_2021-11-30.json"
    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val dateFormat = "YYYY-MM-DD"

    val actualResultDF = addFileDateElseDefaultToDataframe(inputDF, dateFormat, "file_date")

    val actualResult = actualResultDF.select("file_date").distinct().collect().map(_.getString(0)).mkString("")

    assert(actualResult matches "2021-11-30")

  }

  it should "First derives file date from the source files used to load inputDF and" +
    "then derives a column derived_date which contains derivation,from file date go back X days." in {

    val sourceFilePath = "src/test/resources/snapchat-stats-report-2021-11-30.json"

    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val INPUTFILEDATEPATTERN = "(\\d{4})-(\\d{2})-(\\d{2})"

    val INPUTFILEDATEFFORMATTER = "yyyy-MM-dd"

    val NoOfDaysToGoBackFromFileDate = -2

    val ActualResultDF = subtractDaysFromFileDateToDataframe(inputDF: DataFrame, INPUTFILEDATEPATTERN, INPUTFILEDATEFFORMATTER, NoOfDaysToGoBackFromFileDate)

    val actualResult = ActualResultDF.select("derived_date").distinct().collect().map(_.getString(0)).mkString("")

    assert(actualResult matches "2021-11-28")

  }

  it should "First derives file date from the source files used to load inputDF and Derives a " +
    "column derived_file_date_unix_timestamp which contains derivation, from file date go back to 1st of month by going back X noOfMonthsToGoBack." in {

    val sourceFilePath = "src/test/resources/snapchat-stats-report-2021-11-30.json"

    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val INPUTFILEDATEPATTERN = "(\\d{4})-(\\d{2})-(\\d{2})"

    val INPUTFILEDATEFFORMATTER = "yyyy-MM-dd"

    val NoOfDaysToGoBackFromFileDate = -2

    val NOOFMONTHSTOGOBACK = -2

    val resultDF = addFirstOfMonthInEpochFromFileDateToDataframe(inputDF, INPUTFILEDATEPATTERN,
      INPUTFILEDATEFFORMATTER, NOOFMONTHSTOGOBACK,
      NoOfDaysToGoBackFromFileDate)

    val ActualResultDF = resultDF.withColumn("new_file_date", from_unixtime(col("derived_file_date_unix_timestamp"), "yyyy-MM-dd"))

    val actualResult = ActualResultDF.select("new_file_date").distinct().collect().map(_.getString(0)).mkString("")

    assert(actualResult matches "2021-10-01")


  }

  it should "First derives file date from the source files used to load inputDF and" +
    "Derives a column derived_file_date which contains derivation, from file date go back to 1st of month by going back X noOfMonthsToGoBack." in {

    val sourceFilePath = "src/test/resources/snapchat-stats-report-2021-11-30.json"

    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val INPUTFILEDATEPATTERN = "(\\d{4})-(\\d{2})-(\\d{2})"

    val INPUTFILEDATEFFORMATTER = "yyyy-MM-dd"

    val NoOfDaysToGoBackFromFileDate = -2

    val NOOFMONTHSTOGOBACK = -2

    val outputDataFormat = "yyyy-MM-dd"

    val ActualResultDF = addFirstOfMonthFromFileDateToDataframe(inputDF, INPUTFILEDATEPATTERN,
      INPUTFILEDATEFFORMATTER, NOOFMONTHSTOGOBACK, outputDataFormat, NoOfDaysToGoBackFromFileDate)

    val actualResult = ActualResultDF.select("derived_file_date").distinct().collect().map(_.getString(0)).mkString("")

    assert(actualResult matches "2021-10-01")

  }

  it should "Adds file date from inputDF based on input_file_name().If no match then empty and returns count of distinct file date used to load inputDF" in {
    val sourceFilePath = "src/test/resources/snapchat-stats-report-2021-11-30_2021-11-30.json"
    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val dateFormat = "YYYY-MM-DD"

    val actualResult = getCountOfDistinctFileDateFromInputFile(inputDF, dateFormat, "file_date")

    assert(actualResult.toString matches "1")

  }


  it should "Reads input Dataframe and derives file date(file_date) based on the source dataframe and groups by partition date and file date" in {
    val sourceFilePath = "src/test/resources/snapchat-stats-report-2021-11-30_2021-11-30.json"
    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val dateFormat = "YYYY-MM-DD"

    val inputNewDF = addFileDateToDataframe(inputDF, dateFormat, "report_d")

    val actualResult = countByFilePartitionDateAndCollect(inputNewDF, "report_d")

    val expectedResult = "{\"report_d\":\"2021-11-30\",\"file_date\":\"2021-11-30\",\"count\":5}"

    val result = if (actualResult == expectedResult) {
      "pass"
    } else "fail"

    assert(result matches "pass")

  }

  it should "should find difference between columns" in {

    val finalDF = intColumnDiff(inputRenamedPinterestDF, inputRenamedPinterestDF,
      dqPinterestColumns,
      "srcTgtDiff")

    val result = if (finalDF.columns.contains("srcTgtDiffSpend_in_dollar") && finalDF.columns.contains("srcTgtDiffSpend_in_dollar")) {
      true
    }
    else {
      false
    }

    assertResult(true)(result)
  }


  it should "join two tables by aliasing the common columns between two tables in the right table" +
    " and final dataframe should drop the alias" in {

    import spark.implicits._
    val llist = Seq(("100", "2021-01-01", 4), ("200", "2021-01-01", 10))

    val left = llist.toDF("campaign_id", "report_d", "impressions")

    val right = Seq(("100", "2021-01-01", "campaign1"), ("200", "2021-01-01", "campaign2")).toDF("campaign_id", "report_d", "campaign_name")

    val flist = Seq(("100", "2021-01-01", 4, "campaign1"), ("200", "2021-01-01", 10, "campaign2"))

    val expected = flist.toDF("campaign_id", "report_d", "impressions", "campaign_name")

    val renameRightCommonCol = commonColumnNameChangeRightDataframe(left, right, "cmp")
    val renameRightCommonColDF = renameRightCommonCol._1
    val dropRightList = renameRightCommonCol._2

    val finalDF =
      left.join(renameRightCommonColDF, left
        .col("campaign_id") === renameRightCommonColDF.col("cmp_campaign_id") &&
        left.col("report_d") === renameRightCommonColDF.col("cmp_report_d"), "left")
        .drop(dropRightList: _*)

    val finalSchema = finalDF.schema.fields.map(f => (f.name, f.nullable))
    val expectedSchema = expected.schema.fields.map(f => (f.name, f.nullable))

    val res = (finalSchema diff expectedSchema).length == 0

    assertResult(res)(true)

  }

  it should "convert date format" in {
    import spark.implicits._
    val inputDf = Seq(
      (1, "04/04/21", "p"),
      (1, "04/04/21", "q"),
      (2, "04/04/21", "r")
    ).toDF("id", "date", "value")
    val VALIDATEDATECOLUMNS = Seq("date")
    val convertDateFormatDf = convertDateFormat(inputDf, VALIDATEDATECOLUMNS, "MM/dd/yy", "yyyy-MM-dd")
    convertDateFormatDf.show(false)
    val convertDateFormatValue = convertDateFormatDf.select("date").distinct().collect().map(_.getString(0)).mkString("")

    assert(convertDateFormatValue matches "2021-04-04")
  }

  it should "convert timestamp date part to default yyyy-mm-dd date format" in {
    import spark.implicits._
    val inputDf = Seq(
      (1, "04/04/21 01:01", "p"),
      (1, "04/04/21 01:01", "q"),
      (2, "04/04/21 01:01", "r")
    ).toDF("id", "date", "value")
    val VALIDATEDATECOLUMNS = Seq("date")
    val convertTsFormatDf = convertToSparkDateTypeFormat(inputDf, VALIDATEDATECOLUMNS, "MM/dd/yy HH:mm")
    convertTsFormatDf.show(false)
    val convertDateFormatValue = convertTsFormatDf.select("date").distinct().collect().map(_.getString(0)).mkString("")

    assert(convertDateFormatValue matches "2021-04-04 01:01:00")
  }

  it should "should extract date from filename(YYYY-MM-DD) and add to new column in the input dataframe" in {
    val sourceFilePath = "src/test/resources/snapchat-stats-report-2021-11-30_2021-11-30.json"
    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val dateFormat = "YYYY-MM-DD"

    val newDf = addFileDateToDataframe(inputDF, dateFormat, "report_d")

    val newDateCol = newDf.select("report_d").distinct().collect().map(_.getString(0)).mkString("")

    assert(newDateCol matches "2021-11-30")
  }

  it should "should extract date from filename(YYYYMMDD) and add to new column in the input dataframe" in {
    val sourceFilePath = "src/test/resources/snapchat-stats-report-20211130_20211130.json"
    val inputDF: DataFrame =
      readFileToDataFrame(
        sourceFilePath,
        fileFormat = "json"
      )

    val dateFormat = "YYYYMMDD"

    val newDf = addFileDateToDataframe(inputDF, dateFormat, "report_d")

    val newDateCol = newDf.select("report_d").distinct().collect().map(_.getString(0)).mkString("")

    assert(newDateCol matches "20211130")
  }

  it should "get row counts in the dataframe by partition date column and collapse the row " in {
    import spark.implicits._
    val sourceDF = Seq(
      ("2010-10-10", "1"),
      ("2010-10-11", "2")
    ).toDF("report_d", "id")

    val tgtDfPartitionDateCount = countByPartitionDateAndCollect(sourceDF, "report_d")

    val tgtDfPartitionDate = tgtDfPartitionDateCount._1

    assert(tgtDfPartitionDate matches "2010-10-10,2010-10-11")
  }

  it should "Renames all the columns in the schema of input dataframe to lower case" in {

    import spark.implicits._
    val inputDF = Seq(
      ("2022-08-10", "2022-08-10")
    ).toDF("REPORT_d", "FILE_DATE")

    val expectedDF = Seq(
      ("2022-08-10", "2022-08-10")
    ).toDF("report_d", "file_date")

    val expectedSchema = expectedDF.schema

    val ActualResultDF = renameAllColumnsToLowerCase(inputDF, inputDF.schema)

    val actualSchema = ActualResultDF.schema

    val result = if (actualSchema == expectedSchema) "pass" else "fail"

    assert(result matches "pass")
  }

  it should "get row counts in the dataframe by partition date column and file date  and collapse the row " in {
    import spark.implicits._
    val sourceDF = Seq(
      ("2010-10-10", "2022-08-10"),
      ("2012-10-10", "2022-08-11"),
      ("2010-10-10", "2022-08-12")
    ).toDF("report_d", "file_date")

    val tgtDfPartitionDateCount = countByFilePartitionDateAndCollect(sourceDF, "report_d", "file_date")

    val expectedResult = "{\"report_d\":\"2010-10-10\",\"file_date\":\"2022-08-10\",\"count\":1},{\"report_d\":\"2010-10-10\",\"file_date\":\"2022-08-12\",\"count\":1},{\"report_d\":\"2012-10-10\",\"file_date\":\"2022-08-11\",\"count\":1}"

    val actualResult = if (tgtDfPartitionDateCount == expectedResult) {
      "pass"
    } else "fail"
    assert(actualResult matches "pass")
  }

  it should "Get latest record partition by report_d and ordered by file date desc" in {
    import spark.implicits._
    val sourceDF = Seq(
      ("2022-08-10", "2022-08-10"),
      ("2022-08-10", "2022-08-11"),
      ("2022-08-10", "2022-08-12"),
      ("2022-08-11", "2022-08-13")
    ).toDF("report_d", "file_date")

    val expectedDF = Seq(
      ("2022-08-10", "2022-08-12"),
      ("2022-08-11", "2022-08-13")
    ).toDF("report_d", "file_date")

    val LatestByPartitionDateFileDateDF = getLatestByPartitionDateFileDate(sourceDF, "report_d", "file_date")

    val actualResultDF = LatestByPartitionDateFileDateDF.except(expectedDF)

    val actualResult = if (actualResultDF.count() == 0) {
      "pass"
    } else "fail"

    assert(actualResult matches "pass")

  }

  it should "Should derives a column derived_date_unix_timestamp(unix epoch) which contains derivation, from file date go back  X days." in {
    import spark.implicits._
    val sourceDF = Seq(
      ("2022-08-10", "2022-08-10")
    ).toDF("report_d", "file_date")

    val INPUTFILEDATEFFORMATTER = "yyyy-MM-dd"

    val NoOfDaysToGoBackFromFileDate = -2

    val inputDerivedDF = subtractDaysFromFileDateInEpoch(sourceDF: DataFrame, col("file_date"), INPUTFILEDATEFFORMATTER, NoOfDaysToGoBackFromFileDate)

    val resultDF = inputDerivedDF.withColumn("new_file_date", from_unixtime(col("derived_date_unix_timestamp"), "yyyy-MM-dd"))

    val actualResult = resultDF.select("new_file_date").distinct().collect().map(_.getString(0)).mkString("")

    assert(actualResult matches "2022-08-08")

  }

  it should "Should execute sql against DF using spark sql" in {
    import spark.implicits._
    val sourceDF = Seq(
      ("2022-08-10", "2022-08-10")
    ).toDF("report_d", "file_date")

    val outputDf = executeInsertOverWriteFromDF(sourceDF,
      inputQuery = "select * from sourceDF",
      selectFlag=true

    )
    assertResult(outputDf.count())(1)

  }

  it should "Should remove non utf-8 characters" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("Randa Accessories √¢¬Ä¬ì Target+","xxxx","xxxx")
    ).toDF("id","name","position")

    val removeNonUtfCharacterValue =  removeNonUtfCharacter(sourceDF,Seq("id")).select("id").distinct().collect().map(_.getString(0)).mkString("")

    val result = if (removeNonUtfCharacterValue == "Randa Accessories  Target+") {
      "pass"
    } else {"fail"}

    assert(result matches "pass")

  }

  it should "Should extract date from the input column containing the date" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("/test/sample_2023-01-01.csv")
    ).toDF("filename")

    val addFileDateDF = regexExtractDateToDataframe(sourceDF,"YYYY-MM-DD","filename","file_date")

    val newDateCol = addFileDateDF.select("file_date").distinct().collect().map(_.getString(0)).mkString("")

    assert(newDateCol matches "2023-01-01")

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

  it should "Should extract date from timestamp.If not then will populate current date" in {
    import spark.implicits._

    val sourceDF = Seq(
      ("2023-01-10T12:01:34-08:00")
    ).toDF("action_date")

    val convertTsFormatDf = extractDateFromTimestamp(sourceDF, Seq("action_date"),"yyyy-MM-dd")

    val newDateCol = convertTsFormatDf.select("action_date").distinct().collect().map(_.get(0)).mkString("")

    assert(newDateCol matches "2023-01-10")

  }

  it should "Should get coalesce value based on count in the input dataframe and inputSplit value" in {
    import spark.implicits._

    val inputDF = Seq(
      ("2023-01-10T12:01:34-08:00"),("2023-01-10T12:01:34-08:00"),("2023-01-10T12:01:34-08:00"),("2023-01-10T12:01:34-08:00")
    ).toDF("action_date")

    val actualResult = calculateFileSplit(inputDF, 2)

    assert(actualResult.toString matches "3")

  }

  it should "Should get validate csv schema" in {
    import spark.implicits._

    val inputDF = Seq(
      ("aaa","123")
    ).toDF("name","id")

    val  schemaString = "name string,id string"

    val outputDF =validateSchema(inputDF, inputFileFormat="csv", inputCsvSchemaString=schemaString)

    val result = if (outputDF.schema == inputDF.schema) "pass" else "fail"

    assert(result matches "pass")

  }

  it should "Gets difference between the  date and the days attributes" in {
    import spark.implicits._

    val inputDF = Seq(
      ("2023-03-20","5")
    ).toDF("set_date","milestones_days_out")


    val derivedMilestoneDueDateDF: DataFrame =  subtractDateFromDaysAttribute(inputDF,"set_date","milestones_days_out","milestone_due_date")

    val derivedMilestoneDueDate = derivedMilestoneDueDateDF.select("milestone_due_date").collect().map(_.get(0)).mkString("")

    assert(derivedMilestoneDueDate matches "2023-03-15")


  }

  it should "Converts the null value in column to default value else will retain input column value in the original column" in {
    import spark.implicits._

    val inputDF = Seq(
      (null,null)
    ).toDF("milestones_risk_days_out","milestones_days_out")


    val defaultRiskDaysOutToZeroDF = convertNullToDefault(inputDF,Seq("milestones_risk_days_out","milestones_days_out"),0)
    val defaultRiskDaysOutToZero = defaultRiskDaysOutToZeroDF.select("milestones_risk_days_out").collect().map(_.get(0)).mkString("")


    assert(defaultRiskDaysOutToZero matches "0")


  }

  it should "Remove dollar from the input column in the input dataframe" in {
    import spark.implicits._

    val inputDF = Seq(
      ("$100","$100")
    ).toDF("ActionCost","Cpa")

    val removeDollarCharacterDF = removeDollarCharacter(inputDF,Seq("ActionCost", "Cpa"))

    val removeDollarCharacterValue = removeDollarCharacterDF.select("ActionCost").collect().map(_.get(0)).mkString("")


    assert(removeDollarCharacterValue matches "100")


  }

  it should "Replace null or empty value with default value in the input column in the input dataframe" in {
    import spark.implicits._

    val inputDF = Seq(
      (null,null)
    ).toDF("ActionCost","Cpa")

    val replaceNullWithDefaultDF = replaceNullWithDefault(inputDF,Seq("ActionCost", "Cpa"),"0",true)

    val replaceNullWithDefaultValue = replaceNullWithDefaultDF.select("ActionCost").collect().map(_.get(0)).mkString("")

    assert(replaceNullWithDefaultValue matches "0")


  }

  it should "Get latest record partition by report_d and ordered by order by 2 cols" in {
    import spark.implicits._
//    val sourceDF = Seq(
//      ("2022-08-10", "2022-08-10", "2022-08-10"),
//      ("2022-08-10", "2022-08-10", "2022-08-10"),
//      ("2022-08-10", "2022-08-10", "2022-08-12"),
//      ("2022-08-11", "2022-08-13", "2022-08-11")
//    ).toDF("report_d", "file_date","update_date")

    val sourceDF = Seq(
      ("2022-08-10", "2022-08-10", "2022-08-10"),
      ("2022-08-10", "2022-08-11", "2022-08-11"),
      ("2022-08-10", "2022-08-12", "2022-08-12"),
      ("2022-08-11", "2022-08-13", "2022-08-13")
    ).toDF("report_d", "file_date","update_date")

    val expectedDF = Seq(
      ("2022-08-10", "2022-08-12", "2022-08-12"),
      ("2022-08-11", "2022-08-13","2022-08-13")
    ).toDF("report_d", "file_date","update_date")


    val LatestByPartitionDateDF =getLatestByPartitionWithTwoOrderByCols(sourceDF, Seq("report_d"),   "file_date", "update_date","dense_rank")

    LatestByPartitionDateDF.show(false)
    val actualResultDF = LatestByPartitionDateDF.except(expectedDF)

    val actualResult = if (actualResultDF.count() == 0) {
      "pass"
    } else "fail"

    assert(actualResult matches "pass")

  }

  it should "Get latest record partition by report_d and ordered by order by 1 cols" in {
    import spark.implicits._
    val sourceDF = Seq(
      ("2022-08-10", "2022-08-10", "2022-08-10"),
      ("2022-08-10", "2022-08-11", "2022-08-11"),
      ("2022-08-10", "2022-08-12", "2022-08-12"),
      ("2022-08-11", "2022-08-13", "2022-08-13")
    ).toDF("report_d", "file_date","update_date")

    val expectedDF = Seq(
      ("2022-08-10", "2022-08-12", "2022-08-12"),
      ("2022-08-11", "2022-08-13","2022-08-13")
    ).toDF("report_d", "file_date","update_date")


    val LatestByPartitionDateDF =getLatestByPartitionDateArray(sourceDF, Seq("report_d"),   "file_date","dense_rank")

    LatestByPartitionDateDF.show(false)
    val actualResultDF = LatestByPartitionDateDF.except(expectedDF)

    val actualResult = if (actualResultDF.count() == 0) {
      "pass"
    } else "fail"

    assert(actualResult matches "pass")

  }

}
