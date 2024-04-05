package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.util.DateTimeUtils.{appendPathWithTimeStamp, findMinDateFromDateListAndSubtractDays, getDaysBetweenStartAndEndDate, getTimestamp, subDaysFromDate}
import com.tgt.dse.mdf.common.pipeline.util.HdfsUtils.{extractMaxDateFromListOfHdfsFilePathsContainingDate, getHdfsFileSize, getListOfHdfsFiles, listOfHdfsFiles, listOfQualifiedHdfsFiles}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

class DateTimeUtilsTest extends AnyFlatSpec with MarketingTestHelpers {

  it should "Append path with timestamp" in {
    val newpath: String = appendPathWithTimeStamp("/user/SVMDEDMD/hive/fb_api_actuals/stage", TEST_JOB_START_TIME)
    val cal = Calendar.getInstance()
    val year = cal.get(Calendar.YEAR)
    assert(newpath contains year.toString)
  }

  it should "Append path with timestamp without slash" in {
    val newpath: String = appendPathWithTimeStamp("/user/SVMDEDMD/hive/fb_api_actuals/stage/", TEST_JOB_START_TIME)
    val cal = Calendar.getInstance()
    val year = cal.get(Calendar.YEAR)
    assert(newpath contains year.toString)
  }

  it should "Append path with both slash and timestamp" in {
    val newpath: String = appendPathWithTimeStamp("/user/SVMDEDMD/hive/fb_api_actuals/stage", TEST_JOB_START_TIME)
    val cal = Calendar.getInstance()
    val year = cal.get(Calendar.YEAR)
    assert(newpath contains "/" + year.toString)
  }

  it should "Should iterate over list of dates and gets min data and subtracts from daysToSubtract parameter" in {

    val distinctDatesList = "2022-08-10,2022-08-11,2022-08-12,2022-08-13"

    val actualResult = findMinDateFromDateListAndSubtractDays(
      distinctDatesList,
      "yyyy-MM-dd",
      1)

    assert(actualResult matches "2022-08-09")

  }

  it should "return timestamp in specific format" in {

    getTimestamp("America/Los_Angeles", DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm"))

  }

  it should "Should subtract number of days from the input date " in {

    val distinctDatesList = "2022-08-10"

    val actualResult = subDaysFromDate("2022-08-10",2,"yyyy-MM-dd")

    assert(actualResult matches "2022-08-08")

  }

  it should "Should get number of days between two date string " in {


    val daysBetween = getDaysBetweenStartAndEndDate("yyyy-MM-dd","2023-01-01","2023-01-06")

    assert(daysBetween.toString matches "5")

  }

  it should "Should get number of days between two date" in {

    val startDate="2023-01-01"
    val endDate="2023-01-06"

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val parsedStartDate = LocalDate.parse(startDate, formatter)
    val parsedEndDate = LocalDate.parse(endDate, formatter)
    val daysBetween = getDaysBetweenStartAndEndDate(parsedStartDate,parsedEndDate)

    assert(daysBetween.toString matches "5")

  }

}
