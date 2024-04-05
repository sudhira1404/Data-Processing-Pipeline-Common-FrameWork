package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dsc.kelsa.datapipeline.core.exceptions.ConfigException
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{DateSequenceException, FileMoveException}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.text.SimpleDateFormat
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util
import java.util.{Collections, Date, Locale, TimeZone}
import scala.util.matching.Regex

object DateTimeUtils extends LoggingTrait {

  /**
   * Append path with timestamp
   *
   * @param inputPath       incoming path
   * @param timeStampColumn folder name to be appended
   * @return
   */


  def appendPathWithTimeStamp(inputPath: String, timeStampColumn: String): String = {
    var timeStampPath: String = ""
    if (inputPath.endsWith("/")) {
      timeStampPath = inputPath + timeStampColumn
    } else {
      timeStampPath = inputPath + "/" + timeStampColumn
    }
    timeStampPath
  }

  /**
   * Append / in the end
   *
   * @param timezone timezone
   * @return formatter
   *
   */
  def getTimestamp(timezone: String,
                   formatter: DateTimeFormatter): String = {
    LocalDateTime
      .now(ZoneId.of(timezone))
      .format(formatter)
  }

  /**
   * Append / in the end
   *
   * @param inputDate inputDate
   *
   */
  def convertToUTCFormat(inputDate: Date): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat(MarketingConstants.DATE_FORMAT_WITH_ZONE_FOR_DATAPORTAL)
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone(MarketingConstants.TIME_ZONE))
    val utcFormatDate: String = simpleDateFormat.format(inputDate)
    log.info("convert to UTC: " + simpleDateFormat.format(inputDate))
    utcFormatDate
  }

  /**
   * Append / in the end
   *
   * @param epochTimeinSeconds epochTimeinSeconds
   *
   */
  def epochTimeToUTC(epochTimeinSeconds: Long): String = {
    val date: Date = new Date(epochTimeinSeconds * 1000)
    log.info("date : " + date + "\n")
    convertToUTCFormat(date)
  }

  /**
   * Converts from string to timestamp format.
   *
   * @param inputDF              The DataFrame containing one or more columns to be converted from strings to timestamps
   * @param timestampColumnNames The Seq of column names in the inputDF which need to be converted
   * @return DataFrame with converted timestamp columns.
   */
  def convertToTimestamp(inputDF: DataFrame, timestampColumnNames: Seq[String]): DataFrame = {
    val timestampMapColumns: Seq[Column] = timestampColumnNames.map(name => to_timestamp(col(name)) as name)
    convertColumnsToTimestamp(inputDF, timestampColumnNames, timestampMapColumns)
  }

  /**
   * extracts date from the filename based on fileDateRegexPattern pattern.If fileDate is not set then current_date is considered
   *
   * @param fileDateRegexPattern defaulted to empty string.If not set and param fileDate is true,
   *                             will work for media stats files which looks for report-yyyy-mm-dd format.
   *                             If not set and param fileDate is false,will appends current date along with partitionColName to destpath to get partition path.
   *                             If set,will be used to look up pattern in the filename to extract the date and
   *                             appends the date along with partitionColName to destpath to get partition path.
   *                             This should be used along with other parameters that are used create partition
   *                             on the hive table after file are moved from src to dest
   * @param fileDate             defaulted to true indicating filename contains date.
   *                             If not set,will work for files which does not have date in the filename.It considers current date for deriving partition date.
   * @param fileDateFormatter    defaulted to empty string.If not set will work for media stats files which looks for report-yyyy-mm-dd format.
   * @param partitionColName     defaulted to report_d string.
   * @return A tuple of partitionFolder like report_d=2022-01-01 and partitionDate
   */
  @SuppressWarnings(Array("MaxParameters"))
  def fileDateExtraction(filename: String, fileDateRegexPattern: String = "", fileDate: Boolean = true,
                         fileDateFormatter: String = "", partitionColName: String = "report_d"): (String, String) = {

    if (fileDate && fileDateRegexPattern != "" && (partitionColName == "" || fileDateFormatter == "")) {
      log.error("Provide partitionColName and fileDateFormatter parameter when used with regexPattern parameter  ..app exiting")
      log.error("refer fileformatter https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html")
      throw ConfigException("Invalid parameter provided to the method fileDateExtraction used in hdfsMove method for archiving" +
        " to partition folder ..app exiting")
    }

    if (fileDate) {
      if (fileDateRegexPattern != "") {
        val patternMatch = new Regex(fileDateRegexPattern)
        val partitionMatch = patternMatch.findAllIn(filename).mkString(",").replace("_", "-")
        val partitionDate = partitionMatch
        if (partitionDate == "") {
          throw FileMoveException(s"Not able to parse the file $filename to get the report date which needs to be added " +
            "to the archive partition path.File date format should match with the parameter regexPattern")
        }
        try {
          convertDateFormat(partitionDate, fileDateFormatter)
        }
        catch {
          case e: DateTimeParseException =>
            log.error("Provide valid fileDateFormatter parameter value used in the method hdfsMove for archiving partition " +
              s" ....$e.getMessage")
            log.error("refer fileformatter https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html")
            log.error("Exception thrown by the method convertDateFormat while trying to format the file date")
            System.exit(1)
        }
        val partitionConvertedDate = convertDateFormat(partitionDate, fileDateFormatter)
        val partitionFolder = partitionColName + "=" + partitionConvertedDate

        (partitionFolder, partitionConvertedDate)

      }
      else {
        val pattern = new Regex("(report-)(\\d{4})-(\\d{2})-(\\d{2})")
        val partitionFolder = pattern.findAllIn(filename).mkString(",").replace("report-", "report_d=")
        val partitionDate = partitionFolder.replace("report_d=", "").trim
        if (partitionDate == "") {
          throw FileMoveException(s"Not able to parse the file $filename to get the report date which needs to be added " +
            "to the archive partition path.File format should contain the format report-d{4}d{2}d{2}")
        }
        (partitionFolder, partitionDate)
      }
    }
    else {
      val partitionDate = java.time.LocalDate.now.toString
      val partitionFolder = partitionColName + "=" + partitionDate
      if (partitionDate == "") {
        throw FileMoveException(s"Not able to parse the file $filename to get the report date which needs to be added " +
          "to the archive partition path")
      }

      (partitionFolder, partitionDate)
    }
  }

  /**
   * udf to extract date from the filename based on fileDateRegexPattern pattern.If fileDate is not set then current_date is considered
   * fileDateRegexPattern If set to empty string and param fileDate is true ,will work for media stats files which looks for report-yyyy-mm-dd format.
   * If set to empty string and param fileDate is false,will appends current date along
   * with partitionColName to destpath to get partition path.
   * If set to valid format,will be used to look up pattern in the filename to extract the date and
   * appends the date along with partitionColName to destpath to get partition path.
   * This should be used along with other parameters that are used create partition on
   * the hive table after file are moved from src to dest
   * fileDate  If set to empty string,will work for files which does not have date in the filename.It considers current date for deriving partition date.
   * fileDateFormatter  If set to empty string, will work for media stats files which looks for report-yyyy-mm-dd format.
   *
   * @return A return formatted date(yyyy-mm-dd) from the filename
   */

  def fileDateExtractionUdf: (String, String, Boolean, String) => String = (filename: String, fileDateRegexPattern: String, fileDate: Boolean,
                                                                            fileDateFormatter: String) => {


    if (fileDate && fileDateRegexPattern != "" && (fileDateFormatter == "")) {
      log.error("Provide fileDateFormatter parameter when used with regexPattern parameter  ..app exiting")
      log.error("refer fileformatter https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html")
      throw ConfigException("Invalid parameter provided to the udf fileDateExtractionUdf")
    }

    if (fileDate) {
      if (fileDateRegexPattern != "") {
        val patternMatch = new Regex(fileDateRegexPattern)
        val partitionMatch = patternMatch.findAllIn(filename).mkString(",").replace("_", "-")
        val partitionDate = partitionMatch
        if (partitionDate == "") {
          throw FileMoveException(s"Not able to parse the file $filename to get the date." +
            "File date format should match with the parameter regexPattern")
        }
        try {
          convertDateFormat(partitionDate, fileDateFormatter)
        }
        catch {
          case e: DateTimeParseException =>
            log.error("Provide valid fileDateFormatter parameter value" +
              s" ....$e.getMessage")
            log.error("refer fileformatter https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html")
            log.error("Exception thrown by the method convertDateFormat while trying to format the file date")
            System.exit(1)
        }
        val partitionConvertedDate = convertDateFormat(partitionDate, fileDateFormatter)

        partitionConvertedDate

      }
      else {
        val pattern = new Regex("(report-)(\\d{4})-(\\d{2})-(\\d{2})")
        val partitionFolder = pattern.findAllIn(filename).mkString(",").replace("report-", "report_d=")
        val partitionDate = partitionFolder.replace("report_d=", "").trim
        if (partitionDate == "") {
          throw FileMoveException(s"Not able to parse the file $filename to get the date." +
            "File format should contain the format report-d{4}d{2}d{2}")
        }
        partitionDate
      }
    }
    else {
      val partitionDate = java.time.LocalDate.now.toString
      if (partitionDate == "") {
        throw FileMoveException(s"Not able to parse the file $filename to get the date")
      }

      partitionDate
    }


  }

  /**
   * converts date from input format to default format(yyyy-mm-dd)
   *
   */

  def convertDateFormat(dateString: String, inputDataFormat: String): String = {

    val formatter = DateTimeFormatter.ofPattern(inputDataFormat, Locale.ENGLISH)
    val ConvertedDate = LocalDate.parse(dateString, formatter)

    ConvertedDate.toString
  }

  /**
   * converts date from input format to output format
   *
   */

  def convertDateFormatToOutputFormat(dateString: String, inputDataFormat: String, outputDataFormat: String): String = {

    val inputFormat = new SimpleDateFormat(inputDataFormat)
    val outputFormat = new SimpleDateFormat(outputDataFormat)
    val formattedDate = outputFormat.format(inputFormat.parse(dateString))

    formattedDate
  }

  /**
   * udf to convert date from input format to output format
   */

  def convertDateFormatToOutputFormatUdf: (String, String, String) => String = (dateString: String, inputDataFormat: String, outputDataFormat: String) => {

    convertDateFormatToOutputFormat(dateString, inputDataFormat, outputDataFormat)

  }

  /**
   * Converts from Unix epoch to timestamp format.
   *
   * @param inputDF              The DataFrame containing one or more columns with Unix epoch timestamps
   * @param timestampColumnNames The Seq of column names in the inputDF which need to be converted
   * @return A new DataFrame with the specified columns converted to timestamps
   */
  def convertUnixTimestampToTimestamp(inputDF: DataFrame, timestampColumnNames: Seq[String]): DataFrame = {
    val unixTimestampColumns: Seq[Column] = timestampColumnNames.map(name => to_timestamp(dropMillisecondsFromUnixEpochForTimestampConversion(name)) as name)
    convertColumnsToTimestamp(inputDF, timestampColumnNames, unixTimestampColumns)
  }

  /**
   * Takes the specified columns in the timestampColumnNames sequence, and applies the columnConversions to each one,
   * producing a new DataFrame with all the original columns, plus the newly converted columns.
   *
   * @param inputDF              The DataFrame containing one or more columns to be converted to Timestamp
   * @param timestampColumnNames The Seq containing string names of all columns to be converted in the DataFrame
   * @param columnConversions    The Seq containing the Columns with conversion formulas (eg to_date, to_timestamp, etc)
   * @return A new DataFrame with the specified columns having the timestamp conversions in columnConversions applied to each
   */
  def convertColumnsToTimestamp(inputDF: DataFrame, timestampColumnNames: Seq[String], columnConversions: Seq[Column]): DataFrame = {
    val allOtherCols: Array[Column] = inputDF.columns.filterNot(columnName => timestampColumnNames.contains(columnName))
      .map(columnName => col(columnName) as columnName)
    val allCols: Array[Column] = allOtherCols ++ columnConversions

    logInfo("Timestamp conversion successful.")
    inputDF.select(allCols: _*)
  }

  def dateFormatRegexMatcher(dateFormat: String): String = {
    dateFormat.toUpperCase match {
      case "YYYY-MM-DD" => "\\d{4}-\\d{2}-\\d{2}"
      case "YYYYMMDD" => "\\d{4}\\d{2}\\d{2}"
      case _ => "\\d{4}-\\d{2}-\\d{2}"
    }
  }

  /**
   * Generates a conversion to Column for a string column name that would effectively drop the milliseconds precision
   * from the Unix epoch value in order to cast it to ISO standard timestamp format.If input is in epcoh the output will be
   * timestamp converted value.If input is a date then output will still be retained as date
   *
   * @param inputColumnName The name of the column to have this conversion applied to
   * @return A Column containing the conversion steps from Unix epoch with milliseconds to timestamp without milliseconds
   */
  def dropMillisecondsFromUnixEpochForTimestampConversion(inputColumnName: String): Column = {
    //val outputColumn: Column = from_unixtime((col(inputColumnName).cast("bigint") / lit(1000)).cast("bigint"))
    val outputColumn: Column = when ((from_unixtime(col(inputColumnName) / lit(1000))).isNull ,(col(inputColumnName)))
      .otherwise((from_unixtime(col(inputColumnName) / lit(1000))))
    outputColumn
  }

  /**
   * Finds the unique dates within a DataFrame. Returns a tuple containing the sequence of dates in order from earliest to latest,
   * as well as the earliest date within the sequence. Should there be dates missing in between the sequence of dates derived
   * from the input DataFrame, it will produce a sequence of dates from the min and max date within the input DataFrame so
   * the final sequence returned would not have any gaps within that date sequence. There are a number of options that
   * also would determine the min and max dates to derive the final sequence from:
   *
   *   - In a scenario when the `inputRecordCount > 0`, if addPriorDay is set to true (default) it also adds one day prior to
   *     the returned sequence, which also becomes the date string returned in the second position of the Tuple. Otherwise
   *     if addPriorDay is set to false, it will only derive the min date from the inputDF.
   *
   *   - In a scenario when the `inputRecordCount = 0`, if priorEphemeralCheckpoint is populated, it will use that date as the
   *     starting date and generate the sequence of dates from there until today's date. This value should be derived from
   *     the Kelsa DIP's checkpoint value, with the format of `"yyyyMMddHHmm"`
   *
   *   - In a scenario when the `inputRecordCount = 0`, if priorEphemeralCheckpoint is empty (default), it generates a
   *     sequence of dates where the starting date is yesterday and the ending date is today.
   *
   * @param inputDF
   * @param addPriorDay
   * @param priorEphemeralCheckpoint
   * @param inputRecordCount
   * @return A tuple containing the sequence of dates in order from earliest to latest, as well as the earliest date within the sequence.
   */
  def getDatesFromDataFrame(inputDF: DataFrame, inputRecordCount: Long, partitionColumn: String, addPriorDay: Boolean = true,
                            priorEphemeralCheckpoint: String = ""): (Seq[String], String) = {

    val dateFormatString = "yyyy-MM-dd"
    val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(dateFormatString)
    log.debug(s"Date format: ${dateFormatString}")

    if (inputRecordCount > 0) {
      val distinctDatesDF: DataFrame = inputDF
        .select(partitionColumn)
        .as(partitionColumn)
        .distinct()

      val minMaxDates: DataFrame = if (addPriorDay) {
        distinctDatesDF
          .select(date_format(date_sub(functions.min(col(partitionColumn)), 1), dateFormatString).as("min_load_date"),
            date_format(functions.max(col(partitionColumn)), dateFormatString).as("max_load_date")
          )
      } else {
        distinctDatesDF
          .select(date_format(functions.min(col(partitionColumn)), dateFormatString).as("min_load_date"),
            date_format(functions.max(col(partitionColumn)), dateFormatString).as("max_load_date")
          )
      }

      val minMaxDatesString: String = minMaxDates.collect().map(row => row.mkString(",")).toSeq.headOption
        .getOrElse(throw DateSequenceException("Unable to retrieve min and max dates from DataFrame"))
      val minMaxDatesSeq: Seq[String] = minMaxDatesString.split(",").toSeq
      val minDate: LocalDate = LocalDate.parse(minMaxDatesSeq.headOption.getOrElse(throw DateSequenceException("Unable to get min date from sequence")))
      val maxDate: LocalDate = LocalDate.parse(minMaxDatesSeq.apply(1))
      val dateRange: Seq[String] = getDayDateRange(minDate, maxDate, dateFormat)
      log.debug(s"List of distinct dates: ${dateRange.mkString(",")}")

      (dateRange, minDate.format(dateFormat))
    } else {
      val currentDate: LocalDate = LocalDate.now(ZoneId.of("UTC"))
      val dateRangeAndMinDate: (Seq[String], String) = if (priorEphemeralCheckpoint.nonEmpty) {
        log.info("No raw data from ephemeral, will use last Ephemeral checkpoint to start from to current date")
        val ephemeralCheckpointFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
        val minDate: LocalDate = LocalDate.parse(priorEphemeralCheckpoint, ephemeralCheckpointFormat)
        log.info(s"Ephemeral was last checkpointed on date : $minDate")
        val dateRange = getDayDateRange(minDate, currentDate, dateFormat)
        (dateRange, minDate.format(dateFormat))
      } else {
        log.info("No new data from ephemeral, will use yesterday's date to start from to current date")
        val minDate: String = currentDate.minusDays(1).format(dateFormat)
        val dateRange: Seq[String] = Seq(minDate, currentDate.format(dateFormat))
        (dateRange, minDate)
      }
      log.debug(s"List of distinct dates: ${dateRangeAndMinDate._1.mkString(",")}")
      dateRangeAndMinDate
    }
  }

  /**
   * Generates a sequence of string dates following the format specified in dateFormat from the starting to ending LocalDate.
   *
   * @param starting   The earliest date in the sequence
   * @param ending     The latest date in the sequence
   * @param dateFormat The format of the date string
   * @return A sequence of formatted date strings, in order, from starting to ending
   */
  def getDayDateRange(starting: LocalDate, ending: LocalDate, dateFormat: DateTimeFormatter): Seq[String] = {
    Iterator.iterate(starting)(d => d.plus(1, ChronoUnit.DAYS))
      .takeWhile(_.until(ending, ChronoUnit.DAYS) >= 0).map(ld => ld.format(dateFormat)).toSeq
  }

  /**
   * Will iterate over list of dates and gets min data and subtracts from daysToSubtract parameter
   *
   * @param distinctDatesList comma separated list of dates
   * @param dateFormatter     format of the date in the distinctDatesList like yyyy-MM-dd
   * @param daysToSubtract    number of days to subtract after identifying earliest dates in the date list
   * @return minimum date string from distinctDatesList after subtracting from daysToSubtract
   */
  def findMinDateFromDateListAndSubtractDays(
                                              distinctDatesList: String,
                                              dateFormatter: String,
                                              daysToSubtract: Long
                                            ): String = {

    val formatter = new SimpleDateFormat(dateFormatter)
    val deltaDistinctDatesList = new util.ArrayList[Date]
    distinctDatesList.split(",").map(a => deltaDistinctDatesList.add(formatter.parse(a)))
    val earliestIncrementalDateString = formatter.format(Collections.min(deltaDistinctDatesList))
    val dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormatter)
    val earliestIncrementalDate = LocalDate.parse(earliestIncrementalDateString, dateTimeFormatter)
    val earliestFndDate = earliestIncrementalDate.minusDays(daysToSubtract).format(dateTimeFormatter)

    earliestFndDate
  }

  /**
   * Will subtract number of days from inputDate
   *
   * @param inputDate inputDate
   * @param minusDays Number of days to subtract
   * @param inputDateFormat    number of days to subtract after identifying earliest dates in the date list
   * @return minimum date string from distinctDatesList after subtracting from daysToSubtract
   */

  def subDaysFromDate(inputDate:String,minusDays:Int,inputDateFormat:String): String = {

    val formatter = DateTimeFormatter.ofPattern(inputDateFormat)
    val dt = LocalDate.parse(inputDate, formatter)
    dt.minusDays(minusDays).format(formatter)
  }


  /**
   * Gets days between start and end
   *
   * @param startDate : start date as LocalDate
   * @param endDate :   end date  as LocalDate
   * @return days between start and end
   */

  def getDaysBetweenStartAndEndDate(startDate:LocalDate ,endDate:LocalDate): Long = {
    import java.time.temporal.ChronoUnit.DAYS
    //val formatter1 = DateTimeFormatter.ofPattern(dateFormat)
    //val parsedStartDate = LocalDate.parse(startDate, formatter)
    //val parsedEndDate = LocalDate.parse(endDate, formatter)
    val daysBetween = DAYS.between(startDate, endDate)
    log.info(s"Total days between start $startDate and end date $endDate=$daysBetween")
    daysBetween
  }

  /**
   * Gets days between start and end
   *
   * @param startDate : start date as String
   * @param endDate :   end date  as String
   * @return long value (days between start and end)
   */

  def getDaysBetweenStartAndEndDate(dateFormat: String, startDate:String ,endDate:String): Long = {
    import java.time.temporal.ChronoUnit.DAYS
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val parsedStartDate = LocalDate.parse(startDate, formatter)
    val parsedEndDate = LocalDate.parse(endDate, formatter)
    val daysBetween = DAYS.between(parsedStartDate, parsedEndDate)
    log.info(s"Total days between start $startDate and end date $endDate=$daysBetween")
    daysBetween
  }

  /**
   * Converts epoch to timestamp
   * @return   DateTime
   */
  def epochToDateTime(epoch:Long): String = {

    val start=0
    val end =19
    val instant = Instant.ofEpochSecond(epoch + 60)//60 seconds added to complete common dpp process
    val dateTime = instant.atZone(ZoneId.of("America/Chicago")).toString.substring(start,end)
    dateTime
  }

}
