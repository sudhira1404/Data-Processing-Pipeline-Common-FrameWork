package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dsc.kelsa.datapipeline.core.service.DataReadWriteHelper.readFromHiveManagedTableWith2StepProcess
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dsc.kelsa.datapipeline.core.transformations.TransFunctions.renameColumns
import com.tgt.dsc.kelsa.datapipeline.core.types.HiveTableReadConfigDetails
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{ContractValidationException, GeneralException, ReadFromTableAsPrevUnionIncrementalException}
import com.tgt.dse.mdf.common.pipeline.service.FileWriterService
import com.tgt.dse.mdf.common.pipeline.util.FileUtils.shellCmdRun
import com.tgt.dse.mdf.common.pipeline.util.HdfsUtils.hdfsSaveFromDF
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.{alpacaRead, csvStructSchema, hiveTableType, withRetryAndThrowException}
import com.tgt.dse.mdf.common.pipeline.validation.ContractValidatorService
import org.apache.commons.math3.util.FastMath.round
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.{Collections, Date, Properties}
import scala.annotation.tailrec


object DataFrameUtils extends LoggingTrait {

  /**
   * Converts dataframe to a MAP */

  @SuppressWarnings(Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf"
  ))
  def dataframeToMap(InputDataFrame: DataFrame): Map[String, Any] = {

    val jsonDataset = InputDataFrame.toJSON
    val jsonDF: DataFrame = jsonDataset.toDF
    val inputRows = jsonDF.select("value").collect().map(_.getString(0)).mkString(" ")
    val parseInputToMap = JsonMethods.parse(inputRows).values.asInstanceOf[Map[String, Any]]
    parseInputToMap
  }

  /**
   * Finds difference between integers columns from source and target dataframes
   */
  def intColumnDiff(SourceDataFrame: DataFrame, TargetDataframe: DataFrame, dqColumns: Seq[String], prefix: String)(implicit spark: SparkSession): DataFrame = {

    val srcInputDF: DataFrame = SourceDataFrame.select(dqColumns.map(m => col(m).cast(IntegerType)): _*)
    val tgtInputDF: DataFrame = TargetDataframe.select(dqColumns.map(m => col(m).cast(IntegerType)): _*)
    val columns = srcInputDF.columns
    val rdd = tgtInputDF.rdd.zip(srcInputDF.rdd).map {
      x =>
        val arr = columns.map(column =>
          x._1.getAs[Int](column) - x._2.getAs[Int](column))
        Row(arr: _*)
    }
    val diffDF = spark.createDataFrame(rdd, srcInputDF.schema)
    val diffOldColumn = diffDF.columns
    val diffNewColumnList = diffOldColumn.flatMap(i => Seq(prefix + i.capitalize))
    val diffRenameColumn = diffOldColumn.zip(diffNewColumnList).map(f => {
      col(f._1).as(f._2)
    })
    val diffRenameColumnDF = diffDF.select(diffRenameColumn: _*)
    diffRenameColumnDF.show(1, false)
    diffRenameColumnDF
  }

  /**
   * Finds common column name between left and right Dataframe and adds prefix with underscore to common columns in
   * the right Dataframe and returns new dataframe and common column names with prefix which could be used to drop
   */

  def commonColumnNameChangeRightDataframe(leftDF: DataFrame, rightDF: DataFrame, prefixRight: String):
  (DataFrame, Array[String]) = {

    @tailrec
    def changeColumnName(dataFrame: DataFrame, columns: Array[String], prefix: String): DataFrame = {
      if (columns.size == 0) {
        dataFrame
      } else {
        changeColumnName(dataFrame.withColumnRenamed(columns.head, prefix + "_" + columns.head), columns.tail, prefix)
      }
    }

    val commonColumn = leftDF.columns.intersect(rightDF.columns)
    val newRightDF = changeColumnName(rightDF, commonColumn, prefixRight)
    val commonColumnPrefixRight = commonColumn.map(c => s"$prefixRight" + "_" + s"$c")

    (newRightDF, commonColumnPrefixRight)

  }

  /**
   * Converts the source date format of the provided input date columns to target date format.
   * If not able to convert to target format,date column will retain the source date format
   *
   * @param inputDF          The initial DataFrame
   * @param validateColumns  sequence of columns for which we need to convert the format of the date
   * @param inputDateformat  Source Date format
   * @param outputDateformat Target Date format
   * @return returns date converted dataframe
   */

  def convertDateFormat(inputDF: DataFrame, validateColumns: Seq[String], inputDateformat: String, outputDateformat: String
                       ): DataFrame = {

    val outputDF = validateColumns.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(
        colName + "convertdateformat",
        when(date_format(to_date(col(colName), inputDateformat), outputDateformat).isNull, col(colName))
          .otherwise(date_format(to_date(col(colName), inputDateformat), outputDateformat))
      )
    }

    val outputOriginalDF = validateColumns.foldLeft(outputDF) { (outputDF, colName) =>
      outputDF.withColumn(
        colName + "original", col(colName))
    }
    val outputDropDF = outputOriginalDF.drop(validateColumns: _*)
    val outputRenamedDF = validateColumns.foldLeft(outputDropDF) { (outputDropDF, colName) =>
      outputDropDF.withColumn(
        colName, col(colName + "convertdateformat"))
    }

    val validateConvertedColumns = validateColumns.map(s => s + "convertdateformat")
    outputRenamedDF.drop(validateConvertedColumns: _*)

  }

  /**
   * Convert source TS  date part to spark default YYYY-MM-DD
   *
   * @param inputDF         The initial DataFrame
   * @param validateColumns sequence of columns for which we need to convert the format of the date
   * @param inputDateformat Source Date format
   * @return returns date converted dataframe
   */

  def convertToSparkDateTypeFormat(inputDF: DataFrame, validateColumns: Seq[String], inputDateformat: String
                                  ): DataFrame = {

    val outputDF = validateColumns.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(
        colName + "converttsformat",
        //to_timestamp(col(colName),inputDateformat)
        when(to_timestamp(col(colName), inputDateformat).isNull, col(colName)).otherwise(to_timestamp(col(colName), inputDateformat))
      )
    }
    val outputOriginalDF = validateColumns.foldLeft(outputDF) { (outputDF, colName) =>
      outputDF.withColumn(
        colName + "original", col(colName))
    }
    val outputDropDF = outputOriginalDF.drop(validateColumns: _*)
    val outputRenamedDF = validateColumns.foldLeft(outputDropDF) { (outputDropDF, colName) =>
      outputDropDF.withColumn(
        colName, col(colName + "converttsformat"))
    }
    outputRenamedDF.printSchema()
    val validateConvertedColumns = validateColumns.map(s => s + "converttsformat")
    outputRenamedDF.drop(validateConvertedColumns: _*)
  }


  /**
   * joins two Dataframe based on join condition provided as string value
   *
   * @param leftDF                 Left DataFrame
   * @param rightDF                Right DataFrame
   * @param joinType               type of join
   * @param joinCondition          join condition to join Left with Right as a string value.If for eg joining based on dma_code,
   *                               if dma_code column exists in left as well as right table, dma_code column in right table
   *                               should be prefixed with "right_" like   val joinConditionDma=  "dma_code = right_dma_code"
   * @param selectColumns          optional sequence of columns to be selected
   * @param prefixRightDFCommonCol Defaulted to "right".If join column in the left and right table are same then right table
   *                               column will be prefixed with prefixRightDFCommonCol + underscore like
   *                               if dma_code column exists in left as well as right table, dma_code column in right table
   *                               will be prefixed with default "right_" like right_dma_code"
   * @return returns tuple of joined dataframe and renamed common column between the right and left table
   *         (it prefixes  prefixRightDFCommonCol + underscore to the column which are common in right and left table
   *         in the right table)
   *         which could be used to drop
   *
   */

  def joinTwoDataFrames(leftDF: DataFrame, rightDF: DataFrame, joinType: String, joinCondition: String,
                        prefixRightDFCommonCol: String = "right",
                        selectColumns: Seq[String] = Seq.empty[String]
                       ): (DataFrame, Array[String])
  = {
    val renameRightCommonCol = commonColumnNameChangeRightDataframe(leftDF, rightDF, prefixRightDFCommonCol)
    val renameRightCommonColDF = renameRightCommonCol._1
    val dropRightList = renameRightCommonCol._2
    val joinedDf = leftDF.join(renameRightCommonColDF, expr(joinCondition), joinType = joinType)
    val selectDF: DataFrame = if (selectColumns.nonEmpty) joinedDf.select(selectColumns.map(name => col(name)): _*) else joinedDf

    (selectDF, dropRightList)
  }

  /**
   * Add date column to the input dataframe based on the date(YYYY-MM-DD or YYYYMMDD) in the file
   *
   * @param inputDF        The input DataFrame
   * @param dateFormat     date format in the filename(values are YYYY-MM-DD or YYYYMMDD)
   * @param newDateColName new date column added to input dataframe with date value from filename
   */
  def addFileDateToDataframe(inputDF: DataFrame, dateFormat: String, newDateColName: String): DataFrame = {

    val dateFormatMatch = DateTimeUtils.dateFormatRegexMatcher(dateFormat)
    inputDF.withColumn(newDateColName, regexp_extract(input_file_name(), dateFormatMatch, 0))
  }

  /**
   * Reads input Dataframe and groups by partition date and gets the count and collapse the partition date and
   * count columns into single comma separated values
   *
   * @param sourceDF             Input DataFrame
   * @param partitionDateColName Partition Date column
   * @return A tuple of comma separated values of partition date and counts associated to the partition date,min max values of partition and totalcount
   */

  def countByPartitionDateAndCollect(sourceDF: DataFrame, partitionDateColName: String): (String, String, String, String, String) = {

    val sourceConvertDF = sourceDF.withColumn(partitionDateColName + "new", col(partitionDateColName).cast(StringType))

    val sourceDropDF = sourceConvertDF.drop(partitionDateColName)

    val sourceRenameDF = renameColumns(sourceDropDF, Seq((partitionDateColName + "new", partitionDateColName)))

    val countDF = sourceRenameDF.groupBy(partitionDateColName).agg(count("*").as("count")
    )
      .agg(concat_ws(",", collect_list(partitionDateColName)).as(partitionDateColName)
        , concat_ws(",", collect_list("count")).as("count")
        // , concat_ws(",", collect_list("min")).as("min")
        // , concat_ws(",", collect_list("max")).as("max")
        , functions.min(partitionDateColName).as("min")
        , functions.max(partitionDateColName).as("max")
        , functions.sum("count").as("sum")
        , count(partitionDateColName).as("partitiondatecount"))

    countDF.cache().count
    //countDF.show(false)
    val firstRow = countDF.head()
    val tgtDfPartitionDate = firstRow.getString(0)
    val tgtDfCount = firstRow.getString(1)
    val tgtDfMinMax = firstRow.getString(2) + "-to-" + countDF.head().getString(3)
    val tgtDfSum = firstRow.getLong(4*1).toString
    val tgtDfPartitionDateCount = firstRow.getLong(5*1).toString

    log.info(s"Partition date and its count in the provided dataframe as part of current load run is $tgtDfPartitionDate and $tgtDfCount")

    (tgtDfPartitionDate, tgtDfCount, tgtDfMinMax, tgtDfSum, tgtDfPartitionDateCount)
  }

  /**
   * Renames all the columns in the schema of input dataframe to lower case
   *
   * @param inputDF       input dataframe
   * @param inputDfSchema schema of the input dataframe
   * @return returns new dataframe with all columns in lower case
   */

  def renameAllColumnsToLowerCase(inputDF: DataFrame, inputDfSchema: StructType)(implicit spark: SparkSession): DataFrame = {
    def recurRename(_inputDfSchema: StructType): Seq[StructField] =
      _inputDfSchema.fields.map {
        case StructField(name, dtype: StructType, nullable, meta) =>
          StructField(name.toLowerCase(), StructType(recurRename(dtype)), nullable, meta)
        case StructField(name, dtype: ArrayType, nullable, meta) => dtype.elementType match {
          //case structString: StringType => StructField(name.toLowerCase(), ArrayType(structString), nullable, meta)
          case struct: StructType => StructField(name.toLowerCase(), ArrayType(StructType(recurRename(struct)), true), nullable, meta)
          case other => StructField(name.toLowerCase(), ArrayType(other), nullable, meta)
        }

        case StructField(name, dtype, nullable, meta) =>
          StructField(name.toLowerCase(), dtype, nullable, meta)
      }

    val newSchema = StructType(recurRename(inputDfSchema))
    log.info("new schema after converting to lower case")
    newSchema.printTreeString()
    spark.createDataFrame(inputDF.rdd, newSchema)

  }

  /**
   * First derives file date from the source files used to load inputDF and Derives a column
   * derived_file_date_unix_timestamp which contains derivation, from file date go back to 1st of month by going back X noOfMonthsToGoBack.
   * If file date is 2022-04-07 and noOfMonthsToGoBack is 1 then this returns unix timestamp representation of 2022-04-01
   *
   * @param inputDF                      input dataframe
   * @param inputFileDatePattern         Regex pattern of file date like "(\\d{4})-(\\d{2})-(\\d{2})"
   * @param inputFileDateFormatter       File date format like "yyyy-MM-dd"
   * @param noOfMonthsToGoBack           number of months to go back like 1
   * @param NoOfDaysToGoBackFromFileDate defaulted to 0.Indicates number of days to go back from file date and then calculate first of month
   * @return A return  dataframe with new attribute derived_file_date_unix_timestamp
   */

  def addFirstOfMonthInEpochFromFileDateToDataframe(inputDF: DataFrame,
                                                    inputFileDatePattern: String,
                                                    inputFileDateFormatter: String,
                                                    noOfMonthsToGoBack: Int, NoOfDaysToGoBackFromFileDate: Int = 0): DataFrame = {

    log.info("derive first of month from file date by going back X months")
    val fileDateExtractionUdf1: UserDefinedFunction = udf(DateTimeUtils.fileDateExtractionUdf)

    val convertDateFormatToOutputFormatUdf1 = udf(DateTimeUtils.convertDateFormatToOutputFormatUdf)
    //M/d/yy
    val inputDerivedDF = inputDF.withColumn("derived_file_date_unix_timestamp",
      unix_timestamp(convertDateFormatToOutputFormatUdf1(date_add(last_day(add_months(
        date_add(fileDateExtractionUdf1(input_file_name(), lit(inputFileDatePattern), lit(true), lit(inputFileDateFormatter)), NoOfDaysToGoBackFromFileDate),
        noOfMonthsToGoBack)), 1), lit(inputFileDateFormatter), lit(inputFileDateFormatter)), inputFileDateFormatter)
    )

    inputDerivedDF
  }

  /**
   * First derives file date from the source files used to load inputDF and Derives a column derived_file_date
   * which contains derivation, from file date go back to 1st of month by going back X noOfMonthsToGoBack.
   * If file date is 2022-04-07 and noOfMonthsToGoBack is 1 then this returns 2022-04-01
   *
   * @param inputDF                      input dataframe
   * @param inputFileDatePattern         Regex pattern of file date like "(\\d{4})-(\\d{2})-(\\d{2})"
   * @param inputFileDateFormatter       File date format like "yyyy-MM-dd"
   * @param noOfMonthsToGoBack           number of months to go back like 1
   * @param outputDataFormat             output format of the date in the new derived column
   * @param NoOfDaysToGoBackFromFileDate defaulted to 0.Indicates number of days to go back from file date and then calculate first of month
   * @return A return dataframe with new attribute derived_file_date
   */

  def addFirstOfMonthFromFileDateToDataframe(inputDF: DataFrame,
                                             inputFileDatePattern: String,
                                             inputFileDateFormatter: String,
                                             noOfMonthsToGoBack: Int, outputDataFormat: String,
                                             NoOfDaysToGoBackFromFileDate: Int = 0): DataFrame = {

    val fileDateExtractionUdf1: UserDefinedFunction = udf(DateTimeUtils.fileDateExtractionUdf)

    val convertDateFormatToOutputFormatUdf1 = udf(DateTimeUtils.convertDateFormatToOutputFormatUdf)

    val inputDerivedDF = inputDF.withColumn("derived_file_date",
      convertDateFormatToOutputFormatUdf1(date_add(last_day(add_months(
        date_add(fileDateExtractionUdf1(input_file_name(), lit(inputFileDatePattern), lit(true),
          lit(inputFileDateFormatter)), NoOfDaysToGoBackFromFileDate), noOfMonthsToGoBack)), 1), lit(inputFileDateFormatter), lit(outputDataFormat))
    )

    inputDerivedDF
  }

  /**
   * First derives file date from the source files used to load inputDF and Derives a column
   * derived_date_unix_timestamp which contains derivation, from file date go back  X days.
   * If file date is 2022-04-07 and NoOfDaysToGoBackFromFileDate is 30 then this returns unix timestamp representation of 2022-03-07
   * File date is derived based on inputDF(from input_file_name()).Refer fileDateExtractionUdf
   *
   * @param inputDF                      input dataframe without containing file date column
   * @param inputFileDatePattern         Regex pattern of file date like "(\\d{4})-(\\d{2})-(\\d{2})"
   * @param inputFileDateFormatter       File date format like "yyyy-MM-dd"
   * @param NoOfDaysToGoBackFromFileDate defaulted to 0.Indicates number of days to go back from file date
   * @return A return  dataframe with new attribute derived_date_unix_timestamp
   */

  def subtractDaysFromFileDateInEpochToDataframe(inputDF: DataFrame,
                                                 inputFileDatePattern: String,
                                                 inputFileDateFormatter: String,
                                                 NoOfDaysToGoBackFromFileDate: Int = 0): DataFrame = {

    log.info("Derive new date in epoch from file date by going back X days")
    val fileDateExtractionUdf1: UserDefinedFunction = udf(DateTimeUtils.fileDateExtractionUdf)

    val convertDateFormatToOutputFormatUdf1 = udf(DateTimeUtils.convertDateFormatToOutputFormatUdf)
    //M/d/yy
    val inputDerivedDF = inputDF.withColumn("derived_date_unix_timestamp",
      unix_timestamp(convertDateFormatToOutputFormatUdf1(
        date_add(fileDateExtractionUdf1(input_file_name(), lit(inputFileDatePattern), lit(true),
          lit(inputFileDateFormatter)), NoOfDaysToGoBackFromFileDate), lit(inputFileDateFormatter), lit(inputFileDateFormatter)), inputFileDateFormatter)
    )
    inputDerivedDF
  }

  /**
   * Derives a column derived_date which contains derivation,from file date go back X days..
   * If file date is 2022-04-07 and NoOfDaysToGoBackFromFileDate is 30 then this returns 2022-03-07
   *
   * @param inputDF                      input dataframe
   * @param inputFileDatePattern         Regex pattern of file date like "(\\d{4})-(\\d{2})-(\\d{2})"
   * @param inputFileDateFormatter       File date format like "yyyy-MM-dd"
   * @param NoOfDaysToGoBackFromFileDate defaulted to 0.Indicates number of days to go back from file date
   * @return A return dataframe with new attribute derived_date
   */

  def subtractDaysFromFileDateToDataframe(inputDF: DataFrame,
                                          inputFileDatePattern: String,
                                          inputFileDateFormatter: String,
                                          NoOfDaysToGoBackFromFileDate: Int = 0): DataFrame = {

    log.info("Derive new date from file date  by going back X days")
    val fileDateExtractionUdf1: UserDefinedFunction = udf(DateTimeUtils.fileDateExtractionUdf)

    val convertDateFormatToOutputFormatUdf1 = udf(DateTimeUtils.convertDateFormatToOutputFormatUdf)
    //M/d/yy
    val inputDerivedDF = inputDF.withColumn("derived_date",
      convertDateFormatToOutputFormatUdf1(date_add(fileDateExtractionUdf1(input_file_name(), lit(inputFileDatePattern),
        lit(true), lit(inputFileDateFormatter)), NoOfDaysToGoBackFromFileDate), lit(inputFileDateFormatter), lit(inputFileDateFormatter))
    )
    inputDerivedDF
  }

  /**
   * Adds file date from  the source file used to load inputDF based on input_file_name()
   * (with input dateformat YYYY-MM-DD or YYYYMMDD).If no match, defaulted to YYYY-MM-DD
   *
   * @param inputDF        Input DataFrame
   * @param dateFormat     date format of file being used to load to inputDF(YYYY-MM-DD or YYYYMMDD)
   * @param newDateColName file Date column name
   * @return dataframe with file date
   */

  def addFileDateElseDefaultToDataframe(inputDF: DataFrame, dateFormat: String, newDateColName: String): DataFrame = {

    val dateFormatMatch = DateTimeUtils.dateFormatRegexMatcher(dateFormat)
    val outputDf = inputDF.withColumn(newDateColName + 1, regexp_extract(input_file_name(), dateFormatMatch, 0))

    outputDf.withColumn(newDateColName,
      when(col(newDateColName + 1) === "", current_date())
        .otherwise(col(newDateColName + 1))).drop(col(newDateColName + 1))

  }

  /**
   * Adds file date from the source file used to load inputDF based on input_file_name()( with input dateformat YYYY-MM-DD or YYYYMMDD).If no match then empty
   *
   * @param inputDF        Input DataFrame
   * @param dateFormat     date format of file being used to load to inputDF(YYYY-MM-DD or YYYYMMDD)
   * @param newDateColName file Date column name
   * @return count with  distinct file date used to load the input dataframe
   */

  def getCountOfDistinctFileDateFromInputFile(inputDF: DataFrame, dateFormat: String, newDateColName: String): Int = {

    val dateFormatMatch = DateTimeUtils.dateFormatRegexMatcher(dateFormat)
    val outputDf = inputDF.withColumn(newDateColName, regexp_extract(input_file_name(), dateFormatMatch, 0))
    val distinctFileDateCount = outputDf.where(col(newDateColName).isNotNull and !(col(newDateColName) <=> lit("")))
      .agg(countDistinct(newDateColName)).collect()(0)(0).toString

    distinctFileDateCount.toInt
  }

  /**
   * Reads input Dataframe  and groups by partition date and file date and gets the count and collapse and coverts this to key/value pair
   *
   * @param sourceDF             Input DataFrame
   * @param partitionDateColName Partition Date column name
   * @param fileDateColName      file Date column name
   * @return A collapsed key value pair of partition date,file date and counts associated to the
   *         partition date and post to grafana metric tgtDfPartitionFileDateCountJson and
   *         different file_date processed in current run and posted to grafana metric tgtDfFileDate
   */

  def countByFilePartitionDateAndCollect(sourceDF: DataFrame, partitionDateColName: String, fileDateColName: String): String = {

    val countByReportDateFileDateDF = sourceDF.select(partitionDateColName, fileDateColName)
      .groupBy(partitionDateColName, fileDateColName).agg(count("*").as("count"))
    val countByFileDateDF = countByReportDateFileDateDF
      .groupBy(fileDateColName)
      .agg(concat_ws(",", collect_list(fileDateColName)).as(fileDateColName))

    val firstRow = countByFileDateDF.head()
    val tgtDfFileDate = firstRow.getString(0)
    log.info("Setting influx metric for FileDate=>\"tgtDfFileDate\" ")
    PipeLineMetadataService.setStringMetricValue("tgtDfFileDate", tgtDfFileDate, influxField = true)

    val countByReportDateFileDateJsonDF = countByReportDateFileDateDF.toJSON
    val countByReportDateFileDateJsonAggDF = countByReportDateFileDateJsonDF.select("value").agg(concat_ws(",", collect_list("value")).as("concat"))
    val tgtDfPartitionFileDateCountJson = countByReportDateFileDateJsonAggDF.head().getString(0)
    log.info(s"File date ,Partition date and its count in the provided dataframe as part of current load run is $tgtDfPartitionFileDateCountJson")
    PipeLineMetadataService.setStringMetricValue("tgtDfPartitionFileDateCountJson", tgtDfPartitionFileDateCountJson, influxField = true)

    tgtDfPartitionFileDateCountJson
  }

  /**
   * Reads input Dataframe and derives file date(file_date) based on the source dataframe from input_file_name()("YYYY-MM-DD" or YYYYMMDD format)
   * and groups by partition date and file date and gets the count and collapse and coverts this to key/value pair
   *
   * @param sourceDF             Input DataFrame
   * @param partitionDateColName Partition Date column name
   * @return A collapsed key value pair of partition date,file date and counts associated to the partition date
   */


  def countByFilePartitionDateAndCollect(sourceDF: DataFrame, partitionDateColName: String): String = {

    val addFileDateDF = addFileDateElseDefaultToDataframe(sourceDF, "YYYY-MM-DD", "file_date")
    val countByReportDateFileDateDF = addFileDateDF.select(partitionDateColName, "file_date")
      .groupBy(partitionDateColName, "file_date").agg(count("*").as("count"))
    val countByReportDateFileDateJsonDF = countByReportDateFileDateDF.toJSON
    val countByReportDateFileDateJsonAggDF = countByReportDateFileDateJsonDF.select("value").agg(concat_ws(",", collect_list("value")).as("concat"))
    val tgtDfPartitionFileDateCountJson = countByReportDateFileDateJsonAggDF.head().getString(0)
    log.info(s"File date ,Partition date and its count in the provided dataframe as part of current load run is $tgtDfPartitionFileDateCountJson")

    tgtDfPartitionFileDateCountJson
  }

  /**
   * Groups by PartitionDate and FileDate and gets the PartitionDate from the latest file date
   *
   * @param sourceDF             Input DataFrame
   * @param partitionDateColName Partition Date column name
   * @param OrderByColName       file Date or order by column column name
   * @return Dataframe
   */

  def getLatestByPartitionDateFileDate(sourceDF: DataFrame, partitionDateColName: String, OrderByColName: String): DataFrame = {

    val windowSpec = Window.partitionBy(partitionDateColName).orderBy(desc(OrderByColName))
    val outputDF = sourceDF.withColumn("dense_rank", dense_rank().over(windowSpec))
    val outputFilterDF = outputDF.filter("dense_rank=1")
    //log.debug("getLatestByPartitionDateFileDate" + outputDF.select(partitionDateColName,fileDateColName,"dense_rank")
    // .groupBy(partitionDateColName,fileDateColName,"dense_rank").agg(count("*").as("count")).show(false))
    val resultDF = outputFilterDF.drop("dense_rank")
    resultDF
  }

  /**
   * Derives a column derived_date_unix_timestamp which contains derivation, from file date go back  X days.
   * If file date is 2022-04-07 and NoOfDaysToGoBackFromFileDate is 30 then this returns unix timestamp
   * representation of 2022-03-07
   *
   * @param inputDF                      input dataframe containing file date
   * @param fileDate                     column name
   * @param inputFileDateFormatter       File date format like "yyyy-MM-dd"
   * @param NoOfDaysToGoBackFromFileDate defaulted to 0.Indicates number of days to go back from file date
   * @return A return  dataframe with new attribute derived_date_unix_timestamp
   */
  @SuppressWarnings(Array("UnusedMethodParameter"))
  def subtractDaysFromFileDateInEpoch(inputDF: DataFrame, fileDate: Column, inputFileDateFormatter: String,
                                      NoOfDaysToGoBackFromFileDate: Int = 0): DataFrame = {

    val convertDateFormatToOutputFormatUdf1 = udf(DateTimeUtils.convertDateFormatToOutputFormatUdf)
    //M/d/yy
    val inputDerivedDF = inputDF.withColumn("derived_date_unix_timestamp",
      unix_timestamp(convertDateFormatToOutputFormatUdf1(date_add(fileDate, NoOfDaysToGoBackFromFileDate),
        lit(inputFileDateFormatter), lit(inputFileDateFormatter)), inputFileDateFormatter)
    )
    inputDerivedDF
  }

  /**
   * Given a sequence of all column names in a given table/dataframe, remove the one column that
   * is the partition column name from that sequence to return a comma separated list of column names which are
   * not partitioned.
   * @param columns A Sequence of all column names on a table. This should include the partitionColumn.
   * @param partitionColumn The name of the partition column. Defaults to "report_d".
   * @return A comma separated list of column names without the partition column.
   */
  def getNonPartitionColumns(columns: Seq[String], partitionColumn: String = "report_d"): String = {
    columns.filterNot(a => a.eq(partitionColumn)).mkString(",")
  }

  /**
   * Gets latest record partitioned by keyColName(sequence) and order by OrderByColName in desc based on row_number/dense_rank/rank
   *
   * @param sourceDF          temp hdfs location for 2 step read
   * @param keyColName        Sequence of partition column name
   * @param OrderByColName    order by column name
   * @param windowingStrategy defaulted to dense_rank.possible values row_number/dense_rank/rank
   * @return Dataframe
   */

  /**
   * Gets latest record partitioned by keyColName and order by OrderByColName in desc based on row_number/dense_rank/rank
   *
   * @param sourceDF          temp hdfs location for 2 step read
   * @param keyColName        comma separated partition column name
   * @param OrderByColName    order by column name
   * @param windowingStrategy defaulted to dense_rank.possible values row_number/dense_rank/rank
   * @return Dataframe
   */

  def getLatestByPartitionDate(sourceDF: DataFrame, keyColName: String, OrderByColName: String, windowingStrategy: String = "dense_rank"): DataFrame = {

    val partitionsColumnsList = keyColName.split(",").toList
    getLatestByPartitionDateArray(sourceDF, partitionsColumnsList, OrderByColName, windowingStrategy)
  }

  /**
   * Gets latest record partitioned by keyColName and order by OrderByColName in desc based on row_number/dense_rank/rank
   *
   * @param sourceDF          temp hdfs location for 2 step read
   * @param keyColName        Sequence of  partition column name
   * @param OrderByColName    order by column name
   * @param windowingStrategy defaulted to dense_rank.possible values row_number/dense_rank/rank
   * @return Dataframe
   */

  def getLatestByPartitionDateArray(sourceDF: DataFrame, keyColName: Seq[String], OrderByColName: String,
                                    windowingStrategy: String = "dense_rank"): DataFrame = {

    val windowSpec = Window.partitionBy(keyColName.map(col(_)): _*).orderBy(desc(OrderByColName))
    val outputDF = windowingStrategy match {
      case "row_number" => sourceDF.withColumn("rowid", row_number().over(windowSpec))
      case "dense_rank" => sourceDF.withColumn("rowid", dense_rank().over(windowSpec))
      case "rank" => sourceDF.withColumn("rowid", rank().over(windowSpec))
      case _ => throw new Exception("Incorrect windowingStrategy parameter to getLatestByPartitionDate method")
    }
    val outputFilterDF = outputDF.filter("rowid=1")
    val resultDF = outputFilterDF.drop("rowid")
    resultDF
  }

  /**
   * Gets latest record partitioned by keyColName and ordered by OrderByColName1,OrderByColName2
   * based on row_number/dense_rank/rank
   *
   * @param sourceDF          temp hdfs location for 2 step read
   * @param keyColName        Sequence of keyColName
   * @param OrderByColName1   order by column name1 Desc
   * @param OrderByColName2   order by column name2 Desc
   * @param windowingStrategy defaulted to dense_rank.possible values row_number/dense_rank/rank
   * @return Dataframe
   */

  def getLatestByPartitionWithTwoOrderByCols(sourceDF: DataFrame, keyColName: Seq[String], OrderByColName1: String, OrderByColName2: String,
                                    windowingStrategy: String = "dense_rank"): DataFrame = {

    val windowSpec = Window.partitionBy(keyColName.map(col(_)): _*).orderBy(desc(OrderByColName1),desc(OrderByColName2))
    val outputDF = windowingStrategy match {
      case "row_number" => sourceDF.withColumn("rowid", row_number().over(windowSpec))
      case "dense_rank" => sourceDF.withColumn("rowid", dense_rank().over(windowSpec))
      case "rank" => sourceDF.withColumn("rowid", rank().over(windowSpec))
      case _ => throw new Exception("Incorrect windowingStrategy parameter to getLatestByPartitionDate method")
    }
    val outputFilterDF = outputDF.filter("rowid=1")
    val resultDF = outputFilterDF.drop("rowid")
    resultDF
  }

  /**
   * Removes prefix "temp_json_struct" from the attributes which are added by the readJsonKafkaInputFiles method .
   *
   * @param inputDF    Dataframe
   * @param tempPrefix prefix temp_json_struct to be removed from the schema of inputDF
   * @return DataFrame
   */
  def removeTempColumnPrefix(inputDF: DataFrame, tempPrefix: String = """temp_json_struct\_\_*"""): DataFrame = {
    val columns: Array[String] = inputDF.columns
    val replacingColumns: Array[String] = columns.map(tempPrefix.r.replaceAllIn(_, ""))
    val renamedDF: DataFrame = replacingColumns.zip(columns).foldLeft(inputDF) { (tempdf, name) => tempdf.withColumnRenamed(name._2, name._1) }
    renamedDF
  }

  /**
   * Gets distinct partition date from source dataframe based on partitionDateColName
   * @param incrementalDF incremental/source dataset
   * @param partitionDateColName partition column name
   * @return comma separated values of partition dates
   */

  def getDistinctPartitionDateFromIncremental(incrementalDF: DataFrame,partitionDateColName:String): String = {

    if (partitionDateColName == "") throw ReadFromTableAsPrevUnionIncrementalException("PartitionDateColName is missing.Need to provide as input")
    val deltaDistinctDatesDF = incrementalDF.select(col(partitionDateColName)).distinct()

    val deltaDistinctDatesCollapseDF = deltaDistinctDatesDF
      .agg(concat_ws(",", collect_list(partitionDateColName)).as(partitionDateColName))

    val firstRow = deltaDistinctDatesCollapseDF.head()

    val deltaDistinctDates = firstRow.getString(0)

    log.info(s"DistinctPartitionDate in the incremental dataset are $deltaDistinctDates")

    deltaDistinctDates
  }

  /**
   * Will iterate over date column in the inputDF to get distinct list of dates and gets min data and subtracts from daysToSubtract parameter
   *
   * @param inputDF         inputDF
   * @param DateColNameInDF Data column in inputDF that will be used to get distinct list of dates available in the inputDF
   * @param dateFormatter   format of the date in the distinctDatesList like yyyy-MM-dd
   * @param daysToSubtract  number of days to subtract after identifying earliest dates in the date list
   * @return minimum date string from distinctDatesList after subtracting from daysToSubtract
   */
  def findMinDateFromDataFrameAndSubtractDays(
                                               inputDF: DataFrame,
                                               DateColNameInDF: String,
                                               dateFormatter: String,
                                               daysToSubtract: Long
                                             ): String = {

    val distinctDatesList = getDistinctPartitionDateFromIncremental(inputDF, DateColNameInDF)
    DateTimeUtils.findMinDateFromDateListAndSubtractDays(distinctDatesList, dateFormatter, daysToSubtract)
  }


  /**
   * Reads from hive table using 2 step process
   * @param tempFNDReadLocation temp hdfs location for 2 step read
   * @param inputQuery input query to be executed
   * @param hiveConfig hive config parameters
   * @param jdbcUrl jdbcUrl
   * @return Dataframe
   */
  def readInputTableToDF(tempFNDReadLocation:String,
                         inputQuery:String,
                         hiveConfig:Properties,
                         jdbcUrl:String,
                         retryCount:Int=3
                         )(implicit sparkSession: SparkSession): DataFrame = {

    log.info(s"tempFNDreadLocation used in 2stepRead => $tempFNDReadLocation")
    log.info(s"InputQuery used in 2stepRead=>$inputQuery")
    val hdfsTmpWritePath = tempFNDReadLocation
    val hiveTableReadConfigDetails = new HiveTableReadConfigDetails(inputQuery, hdfsTmpWritePath, jdbcUrl, hiveConfig)
    val readFromHiveTableDF = readFromHiveManagedTableWith2StepProcess(hiveTableReadConfigDetails, retryCount)

    readFromHiveTableDF
  }

  /**
   * Wrapper method to read from hive table which has default values for hive config parameters(hive engine and queue name)
   * @param inputQuery input query to be executed
   * @param queueName queueName
   * @param jdbcUrl jdbcUrl
   * @param retryCount defaulted to 3
   * @param tempFNDReadLocation Defaulted to emtpy.temp hdfs location for 2 step read if tableIsExternal is set to false
   * @param tableIsExternal defaulted to true.If set will read from external table using spark sql else 2 step read
   * @param viewName defaulted to empty.If set will create a view which could be accessed as a table.
   * @return Dataframe
   */
  def readInputTableToDFWrapper(
                                 inputQuery:String,
                                 queueName:String,
                                 jdbcUrl:String,
                                 retryCount:Int=3,
                                 tempFNDReadLocation:String="",
                                 tableIsExternal: Boolean = true,
                                 viewName:String=""
                               )(implicit sparkSession: SparkSession): DataFrame = {

    val readFromHiveTableDF = if (tableIsExternal) {
      readExternalInputTableToDF(inputQuery, retryCount, 3 * 1000)
    } else {
      val hiveConfig = new Properties()
      hiveConfig.put("hive.execution.engine", "tez")
      hiveConfig.put("tez.queue.name", queueName)
      readInputTableToDF(tempFNDReadLocation,
        inputQuery,
        hiveConfig,
        jdbcUrl,
        retryCount)
    }


    if (viewName != "") {
      readFromHiveTableDF.createOrReplaceTempView(viewName)
    }

    readFromHiveTableDF
  }

  /**
   * Reads from external table using spark sql session
   * @param inputQuery input query to be executed
   * @param retries retries
   * @param sleep sleep like 3*1000
   * @return Dataframe
   */

  def readExternalInputTableToDF(inputQuery:String,retries:Int,sleep:Long)(implicit sparkSession: SparkSession): DataFrame = {

    log.info(s"InputQuery used for reading from the external table using spark sql=>$inputQuery")
    val readExternalInputTableDF = withRetryAndThrowException(retries, sleep, "readExternalInputTableToDF")(
      sparkSession.sql(inputQuery)
    )

    readExternalInputTableDF
  }


  /**
   * Gets Min and Max value of the partition column in the sourceDF.Partition column can be string or date
   * @param sourceDF sourceDF
   * @param partitionColName partitionColName
   * @return (min date,max date)
   */
  @SuppressWarnings(Array("UnusedMethodParameter"))
  def minMax(sourceDF: DataFrame,partitionColName:String): (String, String) = {

    log.info("Getting min and max from the incremental dataset")
    val minMax = sourceDF.select(partitionColName)
      .agg(functions.min(partitionColName).as("min"), functions.max(partitionColName).as("max"))

    val minDate:String = try {
      minMax.head().getDate(0).toString
    } catch {

      case _: ClassCastException => minMax.head().getString(0)
    }
    val MaxDate:String = try {
      minMax.head().getDate(1).toString
    } catch {

      case _: ClassCastException => minMax.head().getString(1)
    }
    (minDate,MaxDate)
  }

  /**
   * If topic is empty then it takes previous days data from fnd or target table and adds it to current date partition
   * @param env env
   * @param fndSchema table Schema name or table  db name
   * @param coreHistTable target table name from where previous data is fetched
   * @param queueName queue name
   * @param jdbcUrl jdbcUrl
   * @param partitionColName partition column Name
   * @param tempFNDReadLocation Defaulted to emtpy.temp hdfs location for 2 step read if tableIsExternal is set to true
   * @param tableIsExternal defaulted to true.If set will read from external table using spark sql else 2 step read

   * @return DataFrame
   */

  @SuppressWarnings(Array("asInstanceOf"))
  def handleEmptyTopic(env:String,
                       fndSchema:String,
                       coreHistTable:String,
                       queueName:String,
                       jdbcUrl:String,
                       partitionColName:String,
                       tempFNDReadLocation:String="",
                       tableIsExternal: Boolean = true
                      )(implicit sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._
    val finalFndDF=if(env != "local") {
      log.info("HandleEmptyTopic method : No data from topic, reading yesterday's data from table and write to  EXT and FND tables")
      val managedDB = fndSchema
      val managedTable = coreHistTable
      val inputHiveQuery = s"select * from $managedDB.$managedTable where $partitionColName = (select max($partitionColName) from $managedDB.$managedTable)"
      val appQueueName: String = queueName
      val appJdbcUrl: String = jdbcUrl
      val prevDayDataDF: DataFrame = readInputTableToDFWrapper(
        inputQuery = inputHiveQuery,
        appQueueName,
        appJdbcUrl,
        3,
        tempFNDReadLocation,
        tableIsExternal)(sparkSession)
      val MaxPartitionColNamePrevDayDataValue = prevDayDataDF.agg(org.apache.spark.sql.functions.max(col(partitionColName))).collect()(0)(0)
      log.info(s"Previous day data from partition $MaxPartitionColNamePrevDayDataValue being reloaded to new partition")
      val DropPartitionColPrevDayDataDF = prevDayDataDF.drop(partitionColName)
      //val finalFndDF = DropPartitionColPrevDayDataDF.withColumn(partitionColName,date_add(lit(MaxPartitionColNamePrevDayDataValue), 1))
      DropPartitionColPrevDayDataDF.withColumn(partitionColName, current_date())

    } else {
      val schemaSeq = Seq("value")
      Seq.empty[(String)].toDF(schemaSeq: _*)
    }
    log.info("HandleEmptyTopic output dataframe schema")
    finalFndDF.printSchema()

    finalFndDF

  }

  /**
   * Will execute the inputHiveQuery and gets the count using alpaca and
   * inputHiveQuery select statement can have columns but the method replaces everything between
   * first select and from with count(*)
   * @param inputHiveQuery inputHiveQuery
   * @param env env
   * @return count of the inputHiveQuery
   */

  @SuppressWarnings(Array("All"))
  def alpacaFndCountRead(inputHiveQuery:String,env:String)(implicit sparkSession: SparkSession): Long = {

    val alpacaReadOutputCount =  if (env != "local") {
      val hiveConfig = scala.collection.mutable.Map[String, String]()
      hiveConfig("hive.execution.engine") = "tez"
      val pattern = "(?<=^select)(.*?)(?=from)"
      val r = Pattern.compile(pattern)
      val m = r.matcher(inputHiveQuery)
      val inputHiveQueryCount = m.replaceAll(" count(*) as count ")
      log.info("InputQuery to be read from alpaca to check counts in fnd table for previous day = " + inputHiveQueryCount)
      val alpacaReadOutput = alpacaRead(hiveConfig, inputHiveQueryCount, env)
      val count = try {
        alpacaReadOutput.head().getLong(0)
      }catch {
        case _: Exception => 0
      }
      log.info("count=" + count)
      count
    }

    else {
      0
    }
    log.info("alpacaReadOutput=" + alpacaReadOutputCount)

    alpacaReadOutputCount
  }

  /**
   * Will execute the inputHiveQuery and gets the count using 2 step read and
   * inputHiveQuery select statement can have columns but the method replaces everything between
   * first select and from with count(*)
   * @param inputHiveQuery inputHiveQuery
   * @param env env
   * @return count of the inputHiveQuery
   */
  @SuppressWarnings(Array("All"))
  def twoStepFndCountRead(inputHiveQuery:String,
                          tempFNDReadLocation:String,
                          env:String,
                          queueName:String,
                          jdbcUrl:String,
                          tableIsExternal: Boolean = true
                         )
                         (implicit sparkSession: SparkSession): Long = {

    val fndReadOutputCount = if (env != "local") {
      val pattern = "(?<=^select)(.*?)(?=from)"
      val r = Pattern.compile(pattern)
      val m = r.matcher(inputHiveQuery)
      val inputHiveQueryCount = m.replaceAll(" count(*) as count ")


      val fndReadOutputDF: DataFrame =
        if (tableIsExternal) {
          log.info("InputQuery to be read to check counts in fnd table for previous day using spark sql = " + inputHiveQueryCount)
          readExternalInputTableToDF(inputHiveQueryCount, 3, 3 * 1000)
        } else {
          log.info("InputQuery to be read from 2step read to check counts in fnd table for previous day = " + inputHiveQueryCount)
          readInputTableToDFWrapper(
            inputQuery = inputHiveQueryCount,
            queueName,
            jdbcUrl,
            3,
            tempFNDReadLocation,
            tableIsExternal)(sparkSession)
        }
      val count = try {
        fndReadOutputDF.head().getLong(0)
      } catch {
        case _: Exception => 0
      }
      log.info("count=" + count)
      count
    }

    else {
      0
    }
    log.info("fndReadOutput=" + fndReadOutputCount)

    fndReadOutputCount
  }

  /**
   * This will iterate over list of dates in sourceDF and formats date and gets min data and subtracts using daysToSubtract.
   * Partition column can be string or date
   * @param sourceDF sourceDF
   * @param partitionColName partitionColName
   * @param daysToSubtract days to subtract after getting minimum date from the sourceDF for the partitionColName
   * @return string value
   */
  @SuppressWarnings(Array("UnusedMethodParameter"))
  def iterateOverListOfDatesInDF(sourceDF: DataFrame,partitionColName:String,
                                 dateFormat:String="yyyy-MM-dd",
                                 daysToSubtract:Long=1): String = {
    val deltaDistinctDates = getDistinctPartitionDateFromIncremental(sourceDF,partitionColName)
    val formatter = new SimpleDateFormat(dateFormat)
    val deltaDistinctDatesList = new util.ArrayList[Date]
    deltaDistinctDates.split(",").map(a =>deltaDistinctDatesList.add(formatter.parse(a)))
    val earliestIncrementalDateString = formatter.format(Collections.min(deltaDistinctDatesList))
    val dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
    val earliestIncrementalDate = LocalDate.parse(earliestIncrementalDateString, dateTimeFormatter)
    val earliestFndDate = earliestIncrementalDate.minusDays(daysToSubtract).format(dateTimeFormatter)

    earliestFndDate
  }

  //Redundant.Not needed.Will execute INSERT OVERWRITE local.We could use hive external interface
  /**
   * Will read the DF and execute INSERT OVERWRITE LOCAL and create file in edge node path or could execute any query which could be executed by spark sql
   * @param inputDF                     Dataframe that needs to be moved to edge node path or any spark sql executed on this
   * @param inputQuery                  Defaulted to empty string.Can be any query which could be executed by spark sql
   *                                    If not set ,will do INSERT OVERWRITE LOCAL from inputDF
   * @param selectFlag                  Defaulted to false.If set then inputQuery is of type select
   *                                    which will replace the table name in the from clause with inputDF and
   *                                    will create a view which will executed by spark sql else
   *                                    input query can be any query which could be executed by spark sql
   * @param edgeNodeWritePath           Defaulted to empty string.If inputQuery is not set then DF will be written to this Edge node path
   *                                    If provided ,will add header to the renameFilename
   * @param selectCols                  Defaulted to empty.If provided then select only those columns from inputDF and will be write to edgenode path
   */

  def executeInsertOverWriteFromDF(inputDF:DataFrame,
                                   inputQuery:String="",
                                   selectFlag:Boolean=false,
                                   edgeNodeWritePath:String="",
                                   selectCols:Seq[String]=Seq.empty,
                                   retries:Int=3,
                                   sleep:Long=3 * 1000)(implicit sparkSession: SparkSession): DataFrame = {

    val newInputQuery =  if (!inputQuery.isEmpty) {
      if(selectFlag) {
        val pattern = "(FROM|from)\\s+(?<table>\\S+)"
        val compile = Pattern.compile(pattern)
        val matcher = compile.matcher(inputQuery)
        var tableName = ""
        val value: Unit = while (matcher.find) {
          tableName = matcher.group("table")
        }
        inputDF.createOrReplaceTempView("inputDF")
        inputQuery.replace(tableName, "inputDF")
      } else {
        inputQuery
      }

    }
    else {
      val inputSelectColsDF = if (selectCols.isEmpty) inputDF.select(selectCols.map(col(_)): _*) else inputDF
      inputSelectColsDF.createOrReplaceTempView("inputSelectColsDF")

      s"INSERT OVERWRITE LOCAL DIRECTORY 'file://${edgeNodeWritePath}' " +
        " select * from inputSelectColsDF"
    }

    log.info(s"InputQuery executed against external table using spark sql=>$newInputQuery")
    val readExternalInputTableDF = withRetryAndThrowException(retries, sleep, "readExternalInputTableToDF",
      "Exception thrown in the function executeInsertOverWriteFromDF")(
      sparkSession.sql(newInputQuery)
    )

    readExternalInputTableDF
  }

  /**
   * Will read the DF and create file in the edgeNodeWritePath and will read all files from edgeNodeWritePath and add header to edge node file and move to edgeNodeRenamePath with renamed file name
   * @param inputDF                            Dataframe that needs to be moved to edgenode path
   * @param edgeNodeWritePath                  edge node path where the file is located
   * @param edgeNodeRenamePath                 rename edge node path where the rename file is located
   * @param header                             Defaulted to empty string.comma separated columns.
   *                                           If provided ,will add header to the renameFilename
   * @param renameFilename                     Rename file name in edgeNodeRenamePath folder
   */

  def dataFrameToEdgeNodeAndConsolidateAddHeader(inputDF: DataFrame,
                                                 edgeNodeWritePath:String,
                                                 edgeNodeRenamePath:String="",
                                                 header:String="",
                                                 renameFilename:String=""
                                                )(implicit sparkSession: SparkSession): Int = {

    var shellCmdStatus=0
    executeInsertOverWriteFromDF(inputDF, edgeNodeWritePath = edgeNodeWritePath)
    shellCmdStatus = if(edgeNodeRenamePath != "" && renameFilename != "") {
      consolidateEdgeNodeFilesAndAddFileAddHeader(edgeNodeWritePath, edgeNodeRenamePath, header, renameFilename)
    }
    else
    {
      log.info("Provide value for parameters edgeNodeRenamePath and renameFilename")
      0
    }
    shellCmdStatus
  }

  /**
   * Will read the file in the edge node path(all files in this path) and  add header to edge node file and move to edgeNodeRenamePath with renamed file name
   * @param edgeNodeWritePath                  edge node path where the file is located
   * @param edgeNodeRenamePath                 rename edge node path where the rename file is located
   * @param header                             Defaulted to empty string.comma separated columns.
   *                                           If provided ,will add header to the renameFilename
   * @param renameFilename                     Rename file name in edgeNodeRenamePath folder
   */

  def consolidateEdgeNodeFilesAndAddFileAddHeader(edgeNodeWritePath:String,
                                                  edgeNodeRenamePath:String,
                                                  header:String="",
                                                  renameFilename:String): Int = {

    var shellCmdStatus=0
    val shellCmdMoveFile = s"cat ${edgeNodeWritePath}/* > ${edgeNodeRenamePath}/s$renameFilename}"
    shellCmdStatus =shellCmdRun(shellCmdMoveFile)
    if(header != "" ) {
      log.info(s"Will add header and consolidate all files in $edgeNodeWritePath to $edgeNodeRenamePath/$renameFilename")
      val shellCmdAddHeader = s"sed -i '1i$header' ${edgeNodeRenamePath}/s$renameFilename}"
      shellCmdStatus =shellCmdRun(shellCmdAddHeader)
    }
    else {
      log.info(s"No header provided,will only consolidate all files in $edgeNodeWritePath to $edgeNodeRenamePath/$renameFilename")
    }
    shellCmdStatus
  }

  /**
   * UDF to replace non UTF-8 characters with space in the string
   */


  def nonUtfRemoveUdf: (String) => String = (columnName: String)=> {

    val result = columnName.replaceAll("[^\\x00-\\x7F]", "")

    result
  }

  /**
   * Replaces non UTF-8 characters with space
   * @param inputDF      inputDF
   * @param nonUtfCols   Sequence of columns for which non UTF-8 characters needs to be removed
   * @return DataFrame
   */

  def removeNonUtfCharacter(inputDF: DataFrame, nonUtfCols: Seq[String]
                           ): DataFrame = {

    val nonUtfRemoveUdf1: UserDefinedFunction = udf(nonUtfRemoveUdf)

    nonUtfCols.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(
        colName,
        nonUtfRemoveUdf1(col(colName)).alias(colName)
      )
    }
  }

  /**
   * Extracts date from timestamp.If not then will populate current date
   * @param inputDF      inputDF
   * @param validateColumns   Sequence of timestamp columns
   * @param dateFormat   input dateFormat
   * @return DataFrame
   */

  def extractDateFromTimestamp(inputDF: DataFrame, validateColumns: Seq[String],dateFormat:String
                              ): DataFrame = {

    val outputDF = validateColumns.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(
        colName + "converttodateformat",
        when(substring(col(colName), 1, 10*1).isNull, current_date)
          .otherwise(to_date(substring(col(colName), 1, 10*1),dateFormat))
      )
    }
    val outputOriginalDF = validateColumns.foldLeft(outputDF) { (outputDF, colName) =>
      outputDF.withColumn(
        colName + "original", col(colName))
    }
    val outputDropDF = outputOriginalDF.drop(validateColumns: _*)
    val outputRenamedDF = validateColumns.foldLeft(outputDropDF) { (outputDropDF, colName) =>
      outputDropDF.withColumn(
        colName, col(colName + "converttodateformat"))
    }
    outputRenamedDF.printSchema()
    val validateConvertedColumns = validateColumns.map(s => s + "converttodateformat")
    outputRenamedDF.drop(validateConvertedColumns: _*)
  }

  /**
   * Drops dups based on dupCheckKey and stores in dups in nullDupOutputLoc
   * @param inputDF            inputDF
   * @param nullDupOutputLoc   dups location or path where it will be stored for reference
   * @param dupCheckKey        Sequence of dup keys
   * @param orderByCol         order by column
   * @return DataFrame
   */

  def dropDup(inputDF: DataFrame, nullDupOutputLoc:String,dupCheckKey:Seq[String],orderByCol:String
             ): DataFrame = {


    val pkColumns = dupCheckKey.map(col)
    val windowSpec = Window.partitionBy(pkColumns: _*).orderBy(orderByCol)
    val rowNumberDF = inputDF.withColumn("row_number", row_number.over(windowSpec))
    val rowNumberDupDF = rowNumberDF.filter("row_number > 1")
    val rowNumberDupCount = rowNumberDupDF.count()
    log.info(s"Dup count in source dataset based on key $dupCheckKey=>$rowNumberDupCount")
    if (rowNumberDupCount > 0) {
      log.info(s"Writing dup checked on keys $dupCheckKey in the location for reference=>$nullDupOutputLoc")
      FileWriterService.writeDQCheckData(rowNumberDupDF, nullDupOutputLoc)
    }

    val rowNumberRemoveDupDF = rowNumberDF.filter("row_number = 1")
    rowNumberRemoveDupDF
  }

  /**
   * Will extract date part(YYYY-MM-DD or YYYYMMDD) based on the dateFormat  from input inputColName and stores it in the column newDateColName
   * @param inputDF        The input DataFrame
   * @param dateFormat     date format in the filename(values are YYYY-MM-DD or YYYYMMDD)
   * @param inputColName   Name of the column from which date needs to be extracted
   * @param newDateColName new date column added to input dataframe with date value from filename
   */

  def regexExtractDateToDataframe(inputDF: DataFrame, dateFormat: String,inputColName:String, newDateColName: String): DataFrame = {

    val dateFormatMatch = DateTimeUtils.dateFormatRegexMatcher(dateFormat)
    inputDF.withColumn(newDateColName, regexp_extract(col(inputColName), dateFormatMatch, 0))
  }

  /**
   * Will coalesce value based on count in the input dataframe and inputSplit value which could used while saving the file in hdfs.
   * If count in 10 and split is 5 then coalesce is 2 and adds plus 1
   * @param inputDF        The input DataFrame
   * @param inputSplit     input split count
   * @return coalesce value
   */

  def calculateFileSplit(inputDF:DataFrame,inputSplit:Int)(implicit spark: SparkSession): Int = {

    val inputDFCount = inputDF.count()

    val coalesce =  round(inputDFCount/inputSplit)+1

    coalesce

  }

  /**
   * Will write to hdfs and read.
   * @param inputDF      inputDF
   * @param fileFormat   fileFormat
   * @param writePath    writePath
   * @param coalesce     coalesce.Defaulted to 0
   * @return coalesce value
   */


  def writeToHDFSAndRead(inputDF: DataFrame, fileFormat: String, writePath: String,coalesce:Int=0)(implicit sparkSession: SparkSession): DataFrame
  = {
    inputDF.coalesce(coalesce).write.mode(SaveMode.Overwrite).format(fileFormat).save(writePath)
    log.info(s"Read inputDF dataset from hdfs path=>$writePath")
    val outputDF = sparkSession.read.format(fileFormat)
      .schema(inputDF.schema)
      .load(writePath)

    outputDF
  }


  /**
   * Reads from external table using spark sql session and save DF to hdfs location if hdfsSaveFlag is set to true
   * @param inputQuery                          input query to be executed
   * @param retries                             Defaulted to 3.retries
   * @param sleep                               Defaulted to 3 * 1000.sleep
   * @param hdfsSaveFlag                        Defaulted to false.If set will save result in hdfs
   * @param hdfsSaveSelectColumns               Defaulted to empty.selectColumns to be fetched from inputDF
   * @param hdfsSaveRenameColumns               Defaulted to empty.renameColumns if any
   * @param hdfsSaveWriteMode                   Defaulted to overwrite to be used while saving DF to HDFS
   * @param hdfsSaveFileFormat                  Defaulted to orc to be used while saving DF to HDFS
   * @param hdfsSaveFileSaveLocation            Defaulted to empty string.Hdfs save file location
   * @param hdfsSaveCoalesce                    Defaulted to  1 to be used while saving DF to HDFS
   * @param hdfsSaveOptions                     Defaulted to Empty.Options to be used while saving DF to HDFS.like
   *                                            Map("header"->"true","sep"->",","mapreduce.fileoutputcommitter.marksuccessfuljobs"->"false")
   * @param hdfsDeriveCoalesceValue             Defaulted to false.If set to true then will calculate coalesce value
   *                                            based on count in the input dataframe(inputQuery) and HdfsInputSplit value which could used while saving the file in hdfs.
   * @param hdfsInputSplit                      Defaulted to 500000.Used in calculating coalesce value

   * @return Dataframe
   */
  @SuppressWarnings(Array("MaxParameters"))
  def readExternalInputTableToDFAndSaveToHdfs(inputQuery:String,
                                              retries:Int=3,
                                              sleep:Long=3 * 1000,
                                              viewName:String="",
                                              hdfsSaveFlag:Boolean=false,
                                              hdfsSaveSelectColumns: Seq[String] = Seq.empty[String],
                                              hdfsSaveRenameColumns: Seq[String] = Seq.empty[String],
                                              hdfsSaveWriteMode:String="overwrite",
                                              hdfsSaveFileFormat:String="orc",
                                              hdfsSaveFileSaveLocation:String="",
                                              hdfsSaveCoalesce:Int=1,
                                              hdfsSaveOptions:Map[String,String] = Map.empty,
                                              hdfsDeriveCoalesceValue:Boolean=false,
                                              hdfsInputSplit:Int=500000

                                             )(implicit sparkSession: SparkSession): DataFrame = {


    val readExternalInputTableDF = readExternalInputTableToDF(inputQuery,retries,sleep)
    if (hdfsSaveFlag) {
      hdfsSaveFromDF(readExternalInputTableDF,
        hdfsSaveSelectColumns,
        hdfsSaveRenameColumns,
        hdfsSaveWriteMode,
        hdfsSaveFileFormat,
        hdfsSaveFileSaveLocation,
        if (hdfsDeriveCoalesceValue) calculateFileSplit(readExternalInputTableDF,hdfsInputSplit) else hdfsSaveCoalesce,
        hdfsSaveOptions)
    }
    if (viewName != "") {
      readExternalInputTableDF.createOrReplaceTempView(viewName)
    }
    readExternalInputTableDF
  }

  /**
   * Will validate the input dataframe against provided schema and returns dataframe with validate schema
   * @param inputDF                    inputDF against which schema will be validated
   * @param inputFileFormat            input fileFormat
   * @param options                    Defaulted to empty.Used if inputFileFormat is like json like ("multiline",true)
   * @param inputCsvSchemaString       Defaulted to empty.Used if inputFileFormat is csv.Provide like  val schemaString = \"name string,address string\"
   * @param inputJsonTemplateLocation  Defaulted to empty.Used if inputFileFormat is Json

   * @return validated Dataframe
   */

  def validateSchema(inputDF: DataFrame,
                     inputFileFormat:String,
                     options:Map[String,String] = Map[String,String](),
                     inputCsvSchemaString:String="",
                     inputJsonTemplateLocation:String=""
                    )(implicit sparkSession: SparkSession): DataFrame = {

    val templateSchema = if (inputFileFormat=="csv"){
      if(inputCsvSchemaString =="") {
        log.info("Provide valid input csv schema like val schemaString = 'name string,address string' to create csv schema")
        throw new Exception(
          "Provide valid input csv schema like val schemaString = 'name string,address string' to create csv schema"
        )
      }
      log.info(s"Provided Csv Schema used to create schema=> $inputCsvSchemaString")
      csvStructSchema(inputCsvSchemaString)
    }
    else {
      sparkSession.read.format(inputFileFormat).options(options).load(inputJsonTemplateLocation).schema
    }

    val contractValidated = ContractValidatorService.validateSchema(
      inputDF,
      templateSchema,
      dataTypeCheck = true,
      extraColumnsThrowExceptionUponFailure = false)
    log.info(contractValidated._3.missingColumnsInSource)
    if (contractValidated._3.missingColumnsInSource.isEmpty && contractValidated._3.datatypeMismatchInSource.isEmpty
      && contractValidated._3.nullableMismatchInSource.isEmpty )
    {
      log.info("There are no missing column in source and matches with the template,so moving forward with the pipeline")
    }
    else
    {
      throw ContractValidationException("Contract validation has failed")
    }
    val validatedDF = contractValidated._2
    log.info("validatedDF printSchema : ")
    validatedDF.printSchema()

    validatedDF
  }

  /**
   * Converts the null value in column to default value else will retain input column value in the original column
   *
   * @param inputDF          The initial DataFrame
   * @param validateColumns  sequence of columns for which we need to convert the format of the date
   * @param defaultValue     Default value to be populated in case column has null value
   * @return returns dataframe
   */

  def convertNullToDefault(inputDF: DataFrame, validateColumns: Seq[String], defaultValue: Long
                          ): DataFrame = {

    val outputDF = validateColumns.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(
        colName + "convertnulltodefault",
        when(col(colName).isNull, defaultValue)
          .otherwise(col(colName))
      )
    }

    val outputOriginalDF = validateColumns.foldLeft(outputDF) { (outputDF, colName) =>
      outputDF.withColumn(
        colName + "original", col(colName))
    }
    val outputDropDF = outputOriginalDF.drop(validateColumns: _*)
    val outputRenamedDF = validateColumns.foldLeft(outputDropDF) { (outputDropDF, colName) =>
      outputDropDF.withColumn(
        colName, col(colName + "convertnulltodefault"))
    }

    val validateConvertedColumns = validateColumns.map(s => s + "convertnulltodefault")
    outputRenamedDF.drop(validateConvertedColumns: _*)
  }

  /**
   * Gets difference between the  date and the days attributes from the inputDF.
   * If date is in epoch format then will convert to date before subtracting.
   *
   * @param inputDF                       inputDF
   * @param dateAttribute                 Attribute name of the date attribute in the inputDF
   * @param daysAttribute                 Attribute name of the days attribute in the inputDF
   * @param attributeNameAfterSubtraction New attribute name after subtracting dateAttribute-daysAttribute
   * @return Dataframe with new column attributeNameAfterSubtraction
   */
  def subtractDateFromDaysAttribute(inputDF: DataFrame,
                                    dateAttribute: String,
                                    daysAttribute: String,
                                    attributeNameAfterSubtraction: String
                                   ): DataFrame = {

    val derivedInputDF = {
      val setDateColumn: Column = DateTimeUtils.dropMillisecondsFromUnixEpochForTimestampConversion(dateAttribute).as(dateAttribute)
      inputDF.withColumn(dateAttribute + "_withoutmilli_" + attributeNameAfterSubtraction, setDateColumn)
    }

    val date_add = udf((x: String, y: Long) => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val result = new Date(sdf.parse(x).getTime() + TimeUnit.DAYS.toMillis(y))
      sdf.format(result)
    })


    val outputDF: DataFrame = derivedInputDF.withColumn(attributeNameAfterSubtraction,
      when(col(dateAttribute + "_withoutmilli_" + attributeNameAfterSubtraction).isNull, col(dateAttribute + "_withoutmilli_" + attributeNameAfterSubtraction))
        .otherwise(when(col(dateAttribute + "_withoutmilli_" + attributeNameAfterSubtraction).isNotNull,
          date_add(col(dateAttribute + "_withoutmilli_" + attributeNameAfterSubtraction),
            col(daysAttribute) * lit(-1))
        )
        ))

    //    val outputDF: DataFrame = derivedInputDF
    //      .withColumn(attributeNameAfterSubtraction, date_add(col(dateAttribute + "_withoutmilli_" + attributeNameAfterSubtraction),
    //        col(daysAttribute) * lit(-1)))

    outputDF
  }

  /**
   * Remove dollar characters in the original column
   * @param inputDF      inputDF
   * @param dollarCols   Sequence of columns for which non UTF-8 characters needs to be removed
   * @return DataFrame
   */

  def removeDollarCharacter(inputDF: DataFrame, dollarCols: Seq[String]
                           ): DataFrame = {

    val outputDF = dollarCols.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(
        colName + "transform",
        regexp_replace(col(colName), "[$,]", "").alias(colName)
      )
    }


    val outputOriginalDF = dollarCols.foldLeft(outputDF) { (outputDF, colName) =>
      outputDF.withColumn(
        colName + "original", col(colName))
    }
    val outputDropDF = outputOriginalDF.drop(dollarCols: _*)
    val outputRenamedDF = dollarCols.foldLeft(outputDropDF) { (outputDropDF, colName) =>
      outputDropDF.withColumn(
        colName, col(colName + "transform"))
    }

    val validateConvertedColumns = dollarCols.map(s => s + "transform")
    outputRenamedDF.drop(validateConvertedColumns: _*)
  }

  /**
   * Gets distinct dates from the inputColumn in the input dataframe
   * @param inputDF      inputDF
   * @param inputColumn  Date column for which distinct dates will be polled from inputDF
   * @return DataFrame
   */

  def getDistinctInputDates(inputDF: DataFrame,inputColumn:String): String = {
    val distinctDatesList = getDistinctPartitionDateFromIncremental(inputDF, inputColumn)
    val listOfDatesInInput = distinctDatesList.split(",").map(a => s"'$a'").mkString(",")
    listOfDatesInInput
  }


  /**
   * Replaces null with default value and returns transformed value in the original column name
   * @param inputDF        inputDF
   * @param inputCols      Sequence of columns for which non UTF-8 characters needs to be removed
   * @param defaultValue   Default value to be replaced for the Null Value
   * @param nullAndEmpty   Defaulted to false.If set will replace null or empty with default value else will replace only for null value

   * @return DataFrame
   */

  def replaceNullWithDefault(inputDF: DataFrame, inputCols: Seq[String],defaultValue:String,
                             nullAndEmpty:Boolean=false
                            ): DataFrame = {

    val outputDF = if (nullAndEmpty) {

      inputCols.foldLeft(inputDF) { (inputDF, colName) =>
        inputDF.withColumn(
          colName + "transform",
          when(col(colName).isNull, lit(defaultValue))
            .when(trim(col(colName)) === "", lit(defaultValue))
            .otherwise(col(colName))
        )
      }
    }
    else
    {
      inputCols.foldLeft(inputDF) { (inputDF, colName) =>
        inputDF.withColumn(
          colName + "transform",
          coalesce(col(colName), lit(defaultValue)).alias(colName)
        )
      }
    }

    val outputOriginalDF = inputCols.foldLeft(outputDF) { (outputDF, colName) =>
      outputDF.withColumn(
        colName + "original", col(colName))
    }
    val outputDropDF = outputOriginalDF.drop(inputCols: _*)
    val outputRenamedDF = inputCols.foldLeft(outputDropDF) { (outputDropDF, colName) =>
      outputDropDF.withColumn(
        colName, col(colName + "transform"))
    }

    val validateConvertedColumns = inputCols.map(s => s + "transform")
    outputRenamedDF.drop(validateConvertedColumns: _*)
  }

  /**
   * Will read from external table using spark sql or managed table using 2 step read.
   * if its managed table 2 step will write to "/user/${nuid.toUpperCase()}/hive/tableread/$schemaName/$tableName"
   * where nuid is queueName before the underscore and jdbcUrl read from config file(app.jdbcUrl)

   * @param pipeLineConfig          PipeLineConfig
   * @param inputQuery              inputQuery
   * @param schemaName              schemaName.Used to identify if table to be read is external or managed
   * @param tableName               tableName.Used to identify if table to be read is external or managed
   * @return : Dataframe
   */
  @SuppressWarnings(Array("TryGet"))
  def readMaxPartitionAndCountFromExternalOrManaged (
                                                      pipeLineConfig: PipeLineConfig,
                                                      inputQuery:String,
                                                      schemaName:String,
                                                      tableName:String
                                                    )(implicit sparkSession: SparkSession): DataFrame = {
    val env: String = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    val queueName: String = pipeLineConfig.config.getString(MarketingConstants.APP_QUEUENAME)
    val jdbcUrl: String = pipeLineConfig.config.getString(MarketingConstants.APP_JDBCURL)
    val tableType=hiveTableType(env,schemaName,tableName)
    val splitQueueName  = queueName.split("_")
    val nuid = splitQueueName(0)
    val tempFNDreadLocation = s"/user/${nuid.toUpperCase()}/hive/tableread/$schemaName/$tableName"
    readInputTableToDFWrapper(
      inputQuery,
      queueName,
      jdbcUrl,
      3,
      tempFNDreadLocation,
      if (tableType.toUpperCase.trim =="EXTERNAL") true else false)

  }

  /**
   * Read max Partition and its count from the table
   * @param pipeLineConfig:               pipeLineConfig
   * @param partitionDateColName:         partitionDateColName
   * @param schemaName:                   schemaName
   * @param tableName:                    tableName
   * @param readAlpaca                    If set will read from alpaca
   *                                      else if table is external, will read using spark sql else if managed ,will read using 2 step
   * @return   tuple of maxPartition and countOfMaxPartition
   */
  def readMaxPartitionAndCountFromTable (pipeLineConfig: PipeLineConfig,
                                         partitionDateColName: String,
                                         schemaName: String,
                                         tableName: String,
                                         readAlpaca:Boolean
                                        )
                                        (implicit sparkSession: SparkSession): (String, Long) ={

    val env = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    var maxPartition = "0001-01-01"
    var countOfMaxPartition:Long = 0

    val hiveConfig = scala.collection.mutable.Map[String, String]()
    hiveConfig("hive.execution.engine") = "tez"
    val inputQuery = s"SELECT $partitionDateColName,count FROM " +
      s"(SELECT $partitionDateColName,count(*) as count,row_number() over " +
      s"( order by $partitionDateColName desc) as rn from $schemaName.$tableName group by $partitionDateColName) sq WHERE sq.rn = 1"
    log.info("InputQuery to be read  = " + inputQuery)
    val readMaxPartitionAndCountDF = if (readAlpaca) {
      alpacaRead(hiveConfig, inputQuery, env)
    } else {
      readMaxPartitionAndCountFromExternalOrManaged(pipeLineConfig,inputQuery,schemaName,tableName)
    }
    val readMaxPartitionAndCountConvertDF = readMaxPartitionAndCountDF.withColumn(partitionDateColName + "new", col(partitionDateColName).cast(StringType))
    val readMaxPartitionAndCountDropDF = readMaxPartitionAndCountConvertDF.drop(partitionDateColName)
    val readMaxPartitionAndCountRenameDF = renameColumns(readMaxPartitionAndCountDropDF, Seq((partitionDateColName + "new", partitionDateColName)))
    if (readMaxPartitionAndCountRenameDF.rdd.isEmpty()) {
      maxPartition = "0001-01-01"
      countOfMaxPartition = 0
    } else {
      val firstRow = readMaxPartitionAndCountRenameDF.head()
      maxPartition = firstRow.getString(1)
      countOfMaxPartition = firstRow.getLong(0)
    }

    val maxPartitionTable = maxPartition
    val countOfMaxPartitionTable = countOfMaxPartition

    log.info(s"MaxPartition in the table $schemaName.$tableName =" + maxPartitionTable)
    log.info(s"CountOfMaxPartition $maxPartitionTable in the table $schemaName.$tableName =" + countOfMaxPartitionTable)

    (maxPartitionTable, countOfMaxPartitionTable)
  }

  /**
   * Gets latest record partitioned by keyColName and ordered by OrderByColName(can be one or two columns.if two should be comma separated)
   * based on row_number/dense_rank/rank
   *
   * @param sourceDF          temp hdfs location for 2 step read
   * @param keyColName        Sequence of keyColName
   * @param orderByColName    comma separated orderByColName(Can be one or two columns.If two should be comma separated)
   * @param windowingStrategy defaulted to dense_rank.possible values row_number/dense_rank/rank
   * @return Dataframe
   */

   def getLatestByPartitionWithOneOrTwoOrderByCols(sourceDF: DataFrame,
                                                   keyColName: Seq[String],
                                                   orderByColName: String,
                                                   windowingStrategy: String,
                                                   exceptionMsg:String) = {

    val outputDF = if (keyColName.nonEmpty && orderByColName != "") {
      val orderByColNameSize = orderByColName.split(",").size
      if (orderByColNameSize > 2) {
        //throw ReadFromTableAsPrevUnionIncrementalException(s"ReadFromTableAsPrevUnionIncremental error...Provide one or two orderByColName")
        throw GeneralException(exceptionMsg)

      }
      if (orderByColNameSize == 1) {
        log.info(s"Removing dups from Dataframe based on keyColName $keyColName and OrderByColName $orderByColName")
        DataFrameUtils.getLatestByPartitionDateArray(sourceDF, keyColName, orderByColName, windowingStrategy)
      }
      else {
        val orderByColName1 = orderByColName.split(",")(0)
        val orderByColName2 = orderByColName.split(",")(1)
        log.info(s"Removing dups from Dataframe based on keyColName $keyColName and OrderByColName $orderByColName1,$orderByColName2")

        DataFrameUtils.getLatestByPartitionWithTwoOrderByCols(sourceDF, keyColName, orderByColName1, orderByColName2, windowingStrategy)
      }
    }
    else {

      sourceDF
    }

    outputDF
  }

}
