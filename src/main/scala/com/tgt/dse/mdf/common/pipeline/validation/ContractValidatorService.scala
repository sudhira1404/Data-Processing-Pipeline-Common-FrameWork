package com.tgt.dse.mdf.common.pipeline.validation

import com.tgt.dsc.kelsa.datapipeline.core.constants.DPPMetricsConstants
import com.tgt.dsc.kelsa.datapipeline.core.service.PipeLineMetadataService
import com.tgt.dsc.kelsa.datapipeline.core.types.KelsaTypes.ValidateStructureContext
import com.tgt.dsc.kelsa.datapipeline.core.validations.ValidationFunctions
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.ContractValidationException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/**
 * Used when a contract validation fails. Stores the results of the validation.
 * @param schemaValidationInfo A list of com.tgt.dsc.kelsa.datapipeline.core.types.KelsaTypes.ValidateStructureContext
 */
final case class ContractValidatorService(schemaValidationInfo: List[ValidateStructureContext]) {
  val missingColumnsInSource : String = schemaValidationInfo.filter(record =>  record.errorCode == 1).mkString(",")
  val extraColumnsInSource :String = schemaValidationInfo.filter(record =>  record.errorCode == 2).mkString(",")
  val datatypeMismatchInSource :String = schemaValidationInfo.filter(record =>  record.errorCode == 3).mkString(",")
  val nullableMismatchInSource :String = schemaValidationInfo.filter(record =>  record.errorCode == 4).mkString(",")
}

/**
 * Used to validate the contract between expected schema and DataFrame
 */
@SuppressWarnings(Array("all"))
object ContractValidatorService extends LoggingTrait {

  /**
   * @param inputDF        The DataFrame to validate the schema against
   * @param templateSchema The StructType containing the fields, datatypes, and nullable options to validate the
   *                       input Dataframe against
   * @param dataTypeCheck  Whether to perform Data Type checking against the schema. Defaults to false.
   * @param nullableCheck  Whether to perform Nullable checking against the schema. Defaults to false.
   * @return A tuple containing the validated schema, and the final DataFrame that adheres to that schema
   */
  def validateSchema(inputDF: DataFrame, templateSchema: StructType, dataTypeCheck: Boolean = false,
                     nullableCheck: Boolean = false,extraColumnsThrowExceptionUponFailure: Boolean = true
                    ): (StructType, DataFrame,ContractValidatorService) = {
    //update metric stage to contract validation
    PipeLineMetadataService.setStringMetricValue(
      MarketingConstants.STAGE,
      MarketingConstants.VALIDATE_CONTRACT
    )
    val schemaValidationInfo = getSchemaValidationInfo(inputDF, templateSchema, dataTypeCheck, nullableCheck)
    val validationFailures: ContractValidatorService = ContractValidatorService(schemaValidationInfo._2)
    var failFlag = false
    var failExtraColFlag = false
    if (!schemaValidationInfo._1) {
      log.error("Error context below:")
      schemaValidationInfo._2.foreach(
        errorContext => {
          log.error(errorContext.toString)
        }
      )
      //It is upto the pipeline developer to decide whether to throw the error and stop the flow
      // or log error and push missing columns , extra columns, data type mismatches, nullable mismatches to grafana or any other
      //alerting system to take further action of either fixing the pipeline code or inform source about the change
      if (validationFailures.missingColumnsInSource.nonEmpty) {
        generateMetrics(validationFailures.missingColumnsInSource,
          MarketingConstants.MISSINGCOLLSOURCE,
          DPPMetricsConstants.MISSINGCOLUMNROWSCOUNT)
        failFlag=true
      }
      if (validationFailures.datatypeMismatchInSource.nonEmpty) {
        generateMetrics(validationFailures.datatypeMismatchInSource,
          MarketingConstants.DATATYPE_MISMATCH_SOURCE,
          DPPMetricsConstants.DATATYPEFAILROWSCOUNT)
        failFlag=true
      }
      if (validationFailures.nullableMismatchInSource.nonEmpty) {
        generateMetrics(validationFailures.nullableMismatchInSource,
          MarketingConstants.NULLABLE_MISMATCH_SOURCE,
          DPPMetricsConstants.CORRUPTROWSCOUNT)
        failFlag=true
      }
      if (validationFailures.extraColumnsInSource.nonEmpty) {
        generateMetrics(validationFailures.extraColumnsInSource,
          MarketingConstants.EXTRACOLLSOURCE,
          DPPMetricsConstants.NEWCOLUMNROWSCOUNT)
        failExtraColFlag=true
      }

    }

    if (failFlag) {
      throw ContractValidationException("Contract validation has failed")
    }
    else {
      if (failExtraColFlag) {
        if (extraColumnsThrowExceptionUponFailure) {
          throw ContractValidationException("Contract validation has failed")
        } else {
          log.error("There are extra columns in source but will not throw exception because " +
            "of extraColumnsThrowExceptionUponFailure flag being set")
        }
      }
      else {
        log.info("Contract validation successfully completed")
      }
    }

    val columnList = templateSchema.fields.map(field => field.name).toSeq
    val columns = columnList.map(name => col(name))
    val schemaEnrichedDF: DataFrame = inputDF.select(columns: _*)
    //schemaEnrichedTemplateDF is not needed
    //val schemaEnrichedTemplateDF: DataFrame = sparkSession.createDataFrame(schemaEnrichedDF.rdd, templateSchema)
    //schemaEnrichedTemplateDF.count()

    (templateSchema,schemaEnrichedDF,validationFailures)
  }
  private def getSchemaValidationInfo(inputDF: DataFrame, templateSchema: StructType, dataTypeCheck: Boolean,
                                      nullableCheck: Boolean): (Boolean, List[ValidateStructureContext]) = {

    //method to check if we have got any new columns or any columns is missing in input compared to template message
    val schemaValidationInfo : (Boolean, List[ValidateStructureContext]) =
      ValidationFunctions.validateStructureWithSchema(
        templateSchema,
        inputDF.schema,
        dataTypeCheck,
        nullableCheck
      )
    schemaValidationInfo
  }

  private def generateMetrics(mismatchValues: String, metricSourceValuesName: String,
                              metricSourceCountName: String ): Unit = {
    var error: String = ""
    metricSourceValuesName match {
      case MarketingConstants.MISSINGCOLLSOURCE => {
        error = s"Missing Columns In Source are : $mismatchValues"
      }
      case MarketingConstants.EXTRACOLLSOURCE => {
        error = s"Extra Columns In Source are : $mismatchValues"
      }
      case MarketingConstants.DATATYPE_MISMATCH_SOURCE => {
        error = s"DataType Mismatches in Source are: $mismatchValues"
      }
      case MarketingConstants.NULLABLE_MISMATCH_SOURCE => {
        error = s"Nullable Mismatch Columns in Source are : $mismatchValues"
      }
      case _ => {throw ContractValidationException("Invalid value passed for metricSourceValuesName")}
    }
    val count: Int = mismatchValues.split(",").length
    PipeLineMetadataService.setStringMetricValue(metricSourceValuesName, mismatchValues)
    PipeLineMetadataService.setLongMetricValue(metricSourceCountName, count.toLong)
    log.error(error)
  }
}
