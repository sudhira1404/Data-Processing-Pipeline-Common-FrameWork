package com.tgt.dse.mdf.common.pipeline.validation

import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dsc.kelsa.datapipeline.core.validations.ValidationFunctions
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.DateFormatException
import org.apache.spark.sql.functions.{col, lit, to_date, when}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameValidatorService extends LoggingTrait {
  /**
   * Validates data types for the input columns in the given dataframe
   */

  def validateDataTypes(inputDF: DataFrame, dataTypeFailureLocation: String,
                        datatypeValidationColumns: Seq[(String, String)])(implicit spark: SparkSession): DataFrame = {

    logger.info(s"$spark")
    PipeLineMetadataService.setStringMetricValue(MarketingConstants.APP_ENV, MarketingConstants.VALIDATEDATATYPES)
    //perform typecasting of the properties
    val rawDataTypeValidatedDF = ValidationFunctions.dataTypeCastValidator(inputDF,
      datatypeValidationColumns)
    //Filter valid and invalid records
    val validDataTypeValidatedDF = rawDataTypeValidatedDF
      .filter(col(MarketingConstants.IS_ROW_VALID) === 1)
      .select(inputDF.columns.map(col): _*)
    val invalidDataTypeValidatedDF = rawDataTypeValidatedDF
      .filter(col(MarketingConstants.IS_ROW_VALID) === 0)
    //data typing failure counts
    val invalidDataTypeCount = invalidDataTypeValidatedDF.count()
    PipeLineMetadataService.setLongMetricValue(MarketingConstants.INVALIDDATATYPEROWS,
      invalidDataTypeCount)
    //Write the error records if present
    if (invalidDataTypeCount > 0) {
      log.info(s"Writing invalid Datatype validator records in the path $dataTypeFailureLocation")
      invalidDataTypeValidatedDF
        .write
        .mode(MarketingConstants.APPEND)
        .format(MarketingConstants.FILE_FORMAT)
        .save(dataTypeFailureLocation)
    } else {
      log.info("There are no invalid Datatype validator records")
    }
    validDataTypeValidatedDF
  }

  /**
   * Validates Not null columns for the input columns in the given dataframe
   */
  def validateNOTNULLColumns(inputDF: DataFrame, NullOrEmptyRecordsLocation: String,
                             notnullColumns: Seq[String]): DataFrame = {

    PipeLineMetadataService.setStringMetricValue(MarketingConstants.STAGE, MarketingConstants.VALIDATENOTNULLCOLUMNS)
    val RawNotNullDF = ValidationFunctions.validateNullForColumnsOr(inputDF,
      notnullColumns, "validateNULLstatus", "validateNULLdescription")
    val notnullValiddDF = RawNotNullDF.filter(col("validateNULLstatus") === 1)
      .select(inputDF.columns.map(col): _*)
    val notnullInvaliddDF = RawNotNullDF.filter(col("validateNULLstatus") === 0)
    val notnullInvaliddDFcount = notnullInvaliddDF.count()
    PipeLineMetadataService.setLongMetricValue(MarketingConstants.INVALIDNOTNULLROWS, notnullInvaliddDFcount)
    if (notnullInvaliddDFcount > 0) {
      notnullInvaliddDF.write.mode(MarketingConstants.APPEND).format(MarketingConstants.FILE_FORMAT).save(NullOrEmptyRecordsLocation)
    } else {
      log.info("There are no invalid NotNull records")
    }

    notnullValiddDF
  }

  /**
   * Validates the date format in the given dataframe
   *
   * @param validateColumns sequence of columns for which we need to validate the format of the date
   * @param dateFormat      Date format against which validate columns list will be validated
   * @return returns validated dataframe
   */
  def validateDateFormat(inputDF: DataFrame, validateColumns: Seq[String], dateFormat: String): DataFrame = {

    val outputDF = validateColumns.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(
        colName + "afterdateformat",
        to_date(col(colName), dateFormat)
      )
    }
    val validateNewColumns = validateColumns.map(v => s"$v" + "afterdateformat")
    val filterExpr = validateNewColumns.map(v => s"$v is Null").mkString(" or ")
    val invalidDF: DataFrame = outputDF.filter(filterExpr)
    val invalidInputDateFormatCount: Long = invalidDF.count()
    if (invalidInputDateFormatCount > 0) {
      log.error("Corrupted records detected!")
      invalidDF.show(2, false)
      throw DateFormatException("Date format issues detected in the source column")
    }
    else {
      inputDF
    }
  }

  /**
   * Validates the date format in the given dataframe which has nulls in the validateColumns
   *
   * @param validateColumns sequence of columns for which we need to validate the format of the date
   * @param dateFormat      Date format against which validate columns list will be validated
   * @param defaultDate     default date to be used like 9999-12-31 to convert nulls in validateColumns to default date
   * @return returns validated dataframe
   */

  def validateDateFormatWithNull(inputDF: DataFrame, validateColumns: Seq[String], defaultDate: String, dateFormat: String): DataFrame = {

    val outputDF = validateColumns.foldLeft(inputDF) { (inputDF, colName) =>
      inputDF.withColumn(colName, when(col(colName).isNull, to_date(lit(defaultDate), dateFormat)).otherwise(col(colName)))
    }

    val dateFormatDF = DataFrameValidatorService.validateDateFormat(outputDF, validateColumns, dateFormat)

    val dateFormatOutputDF = validateColumns.foldLeft(dateFormatDF) { (dateFormatDF, colName) =>
      dateFormatDF.withColumn(colName, when(col(colName) === lit(defaultDate), null).otherwise(col(colName)))
    }

    dateFormatOutputDF
  }

  /**
   * Compares schemas from two Dataframes based on Data types
   *
   * @param sourceDF sourceDF
   * @param targetDF targetDF
   * @return tuple(in source but not in target and in target but not in source)
   */

  def schemaComparison(sourceDF: DataFrame, targetDF: DataFrame): (List[StructField], List[StructField]) = {

    val sourceFields = sourceDF.schema.fields
    val targetFields = targetDF.schema.fields

    //dataType comparison.replace 'dataType' with 'nullable' for nullcomparison
    val sourceFieldNameTypeMap = sourceFields.map(f => f.name -> f.dataType).toMap
    val targetFieldNameTypeMap = targetFields.map(f => f.name -> f.dataType).toMap

    val diffFieldsInSource = sourceFields.filter(f => sourceFieldNameTypeMap.get(f.name) != targetFieldNameTypeMap.get(f.name)).toList
    val diffFieldsInTarget = targetFields.filter(f => sourceFieldNameTypeMap.get(f.name) != targetFieldNameTypeMap.get(f.name)).toList
    (diffFieldsInSource, diffFieldsInTarget)
  }
}
