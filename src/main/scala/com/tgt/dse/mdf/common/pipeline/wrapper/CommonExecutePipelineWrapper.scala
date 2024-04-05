package com.tgt.dse.mdf.common.pipeline.wrapper

import com.jcraft.jsch.ChannelSftp
import com.springml.sftp.client.SFTPClient
import com.tgt.dsc.kelsa.datapipeline.core.service.CompactorService.updateCheckPointPath
import com.tgt.dsc.kelsa.datapipeline.core.transformations.TransFunctions.renameColumns
import com.tgt.dsc.kelsa.datapipeline.core.transformations.{FlattenFunctions, TransFunctions}
import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants
import com.tgt.dse.mdf.common.pipeline.exceptions.{ContractValidationException, FileCopyException, FileMoveException, HiveWriteException, TransferFromHdfsToSftpException, TransferFromSftpToHdfsException}
import com.tgt.dse.mdf.common.pipeline.service.FileReader.{readFileToDataFrame, readJsonKafkaFile}
import com.tgt.dse.mdf.common.pipeline.service.{DataQualityPublisherGeneric, FileWriterService, HiveExternalTableWriter, ReadKafkaFiles, SftpTransferService}
import com.tgt.dse.mdf.common.pipeline.transform.RevenueTransformer
import com.tgt.dse.mdf.common.pipeline.types.HiveExternalAtomicTable
import com.tgt.dse.mdf.common.pipeline.util.CommonExecutePipelineUtils.{dropDup, sendDFToEmail}
import com.tgt.dse.mdf.common.pipeline.util.{CommonExecutePipelineUtils, DataFrameUtils}
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.{convertToSparkDateTypeFormat, countByFilePartitionDateAndCollect, getLatestByPartitionDateArray, regexExtractDateToDataframe, replaceNullWithDefault}
import com.tgt.dse.mdf.common.pipeline.util.DateTimeUtils.appendPathWithTimeStamp
import com.tgt.dse.mdf.common.pipeline.util.MarketingCommonUtils.{appendSlashInEnd, csvStructSchema, setJobGroup}
import com.tgt.dse.mdf.common.pipeline.util.SftpUtils.disConnectSftpConnection
import com.tgt.dse.mdf.common.pipeline.validation.DataFrameValidatorService.{validateDataTypes, validateNOTNULLColumns}
import com.tgt.dse.mdf.common.pipeline.validation.{ContractValidatorService, DataFrameValidatorService}
import com.tgt.dse.mdf.common.pipeline.vo.CommonExecutePipelineParameterObjects.{AddDatePartitionColumn, AddFileDate, ArchiveArchivePartition, ArchiveEphemeral, AtomicHistLoad, CommonPipeLineHelper, ConvertDateFormat, ConvertDatePartOfTsToDefault, ConvertMicroToCurrency, ConvertTimestampFromLocalToUtc, ConvertTimestampFromUtcToLocal, ConvertTimestampToDate, CountByFileDateAndPartitionDate, DeleteTriggerFile, DqCheck, DropDups, ExecuteExternalSql, ExecuteSqlAgainstDataframe, ExecuteSqlAgainstTable, ExecutionOrder, FndLoad, GetLatestByKeyCol, HandleNullsInPKColumns, JoinCal, JoinTwoDataFrames, ReadInput, ReadInputKafka, RemoveDollarCharacter, RemoveNonUtfCharacter, Rename, ReplaceNullWithDefaultForAmtColumn, ReplaceNullWithDefaultForQtyColumn, ReplaceNullWithDefaultForStringColumn, SaveDFToHdfs, SendDFToEmail, TransferFromHdfsToSftp, TransferFromSftpToHdfs, ValidateDataType, ValidateDateFormat, ValidateNotNull, ValidateSchema}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CommonExecutePipelineWrapper extends LoggingTrait {


  /**
   * Wrapper to moves the files from the temporary landing directory to the processing directory
   *
   * @param commonPipeLineHelper CommonPipeLineHelper Type
   * @param sparkSession         Implicit Spark session
   * @return Unit
   */

  def moveEphemeralFilesToProcessWrapper(commonPipeLineHelper: CommonPipeLineHelper)(implicit sparkSession: SparkSession): Unit = {
    val ephemeralMovedFilesize: BigInt = FileWriterService.hdfsMove(
      FileWriterService(commonPipeLineHelper.fileLandingPath,
        commonPipeLineHelper.inputEphemeralPath,
        zerobyteabort = true))(sparkSession)
    if (!ephemeralMovedFilesize.>(0))
      {
        throw FileMoveException("Failed to move files to ephemeral path. Check source and landing paths and rerun")
      }
    log.info(s"Filesize moved to EphemeralPath ${commonPipeLineHelper.inputEphemeralPath} is $ephemeralMovedFilesize")
  }


  /**
   * Wrapper to read input files
   *
   * @param commonPipeLineHelper CommonPipeLineHelper case class
   * @param readInput            readInput type
   * @param sparkSession         Implicit Spark session
   * @return DataFrame
   */
  def readInputFilesWrapper(commonPipeLineHelper: CommonPipeLineHelper,
                            readInput: ReadInput
                           )(implicit sparkSession: SparkSession): DataFrame = {
    log.info("Read from input location : " + commonPipeLineHelper.inputEphemeralPath)
    val inputDF: DataFrame =
      readFileToDataFrame(
        fileLocation = commonPipeLineHelper.inputEphemeralPath,
        fileFormat = readInput.inputEphemeralFilesFormat,
        corruptRecordLocation = commonPipeLineHelper.corruptRecordLocation,
        templateLocation = readInput.templateLocation,
        hasHeader = readInput.hasHeader,
        renameColumns = readInput.renameColumns,
        selectColumns = readInput.selectColumns,
        filterCondition = readInput.filter,
        delimiter = readInput.delimiter,
        options = readInput.options,
        getFileName = readInput.getFileName
      )

    inputDF
  }

  /**
   * Wrapper to read input kafka files
   *
   * @param commonPipeLineHelper CommonPipeLineHelper case class
   * @param readInputKafka       ReadInputKafka type
   * @param sparkSession         Implicit Spark session
   * @return DataFrame
   */

  def readInputKafkaFilesWrapper(commonPipeLineHelper: CommonPipeLineHelper,
                                 readInputKafka: ReadInputKafka
                                )(implicit sparkSession: SparkSession): (DataFrame, List[String], Long) = {

    readJsonKafkaFile(ReadKafkaFiles(
      commonPipeLineHelper.inputEphemeralPath,
      readInputKafka.templateLocation,
      readInputKafka.checkpointPath,
      commonPipeLineHelper.corruptRecordLocation,
      readInputKafka.inputEphemeralFilesFormat,
      readInputKafka.numberOfPartitions,
      commonPipeLineHelper.env,
      readInputKafka.dropColumns,
      readInputKafka.ephemeralFilePathPrefix,
      addKafkaTimestamp =readInputKafka.addKafkaTimestamp
    ))
  }
  /**
   * Wrapper to validate the schema
   *
   * @param inputDF        inputDF
   * @param validateSchema ValidateSchema type
   * @param sparkSession   Implicit Spark session
   * @return DataFrame
   */

  def validateSchemaWrapper(inputDF: DataFrame,
                            validateSchema: ValidateSchema
                           )(implicit sparkSession: SparkSession): DataFrame = {

    val schema = if (validateSchema.inputEphemeralFilesFormat == "csv") {
      csvStructSchema(validateSchema.csvSchemaString)
    }
    else {
      val templateDF: DataFrame = sparkSession.read.format(validateSchema.inputEphemeralFilesFormat)
        .option("multiLine", true)
        .load(validateSchema.templateLocation)

      templateDF.schema
    }
    val contractValidated = ContractValidatorService.validateSchema(
      inputDF,
      schema,
      dataTypeCheck = true,
      extraColumnsThrowExceptionUponFailure = false)
    log.info(contractValidated._3.missingColumnsInSource)
    if (contractValidated._3.missingColumnsInSource.isEmpty && contractValidated._3.datatypeMismatchInSource.isEmpty
      && contractValidated._3.nullableMismatchInSource.isEmpty) {
      log.info("There are no missing column in source and matches with the template,so moving forward with the pipeline")
    }
    else {
      throw ContractValidationException("Contract validation has failed")
    }
    val validatedDF = contractValidated._2
    logInfo("validatedDF printSchema : ")
    validatedDF.printSchema()

    validatedDF
  }



  /**
   * Wrapper to add file date from filename to a column
   *
   * @param inputDF               inputDF
   * @param addFileDate           AddFileDate type
   * @param sparkSession         Implicit Spark session
   * @return DataFrame
   */

  def addFileDateWrapper(inputDF: DataFrame, addFileDate: AddFileDate)(implicit sparkSession: SparkSession): DataFrame = {

    regexExtractDateToDataframe(inputDF, addFileDate.sourceDateFormat, addFileDate.sourceColumn, addFileDate.targetColumn)

  }


  /**
   * Wrapper to addDatePartitionColumn
   *
   * @param inputDF                inputDF
   * @param addDatePartitionColumn AddDatePartitionColumn type
   * @param sparkSession           Implicit Spark session
   * @return DataFrame
   */

  def addDatePartitionColumnWrapper(inputDF: DataFrame, addDatePartitionColumn: AddDatePartitionColumn)(implicit sparkSession: SparkSession): DataFrame = {

    CommonExecutePipelineUtils.addDatePartitionColumn(inputDF, addDatePartitionColumn.isPartitionColumnDatatypeDate, addDatePartitionColumn.sourceColumn, addDatePartitionColumn.targetColumn)

  }

  /**
   * Wrapper to flatten
   *
   * @param inputDF               inputDF
   * @param sparkSession         Implicit Spark session
   * @return DataFrame
   */

  def flattenWrapper(inputDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {

    FlattenFunctions.flattenDataframe(inputDF)
  }

  /**
   * Wrapper to get latest record by partition column
   *
   * @param inputDF               inputDF
   * @param getLatestByKeyCol     GetLatestByKeyCol type
   * @param sparkSession         Implicit Spark session
   * @return DataFrame
   */

  def getLatestByKeyColWrapper(inputDF: DataFrame, getLatestByKeyCol: GetLatestByKeyCol)(implicit sparkSession: SparkSession): DataFrame = {

    getLatestByPartitionDateArray(inputDF, getLatestByKeyCol.keyColName, getLatestByKeyCol.orderByColName, getLatestByKeyCol.windowingStrategy)

  }

  /**
   * Wrapper to count by partition date and file date and gets the count and collapse and coverts this to key/value pair
   *
   * @param inputDF               inputDF
   * @param getLatestByKeyCol     GetLatestByKeyCol type
   * @param sparkSession         Implicit Spark session
   * @return A collapsed key value pair of partition date,file date and counts associated to the
   *         partition date and post to grafana metric tgtDfPartitionFileDateCountJson and
   *         different file_date processed in current run and posted to grafana metric tgtDfFileDate

   */

  def countByFileDateAndPartitionDateWrapper(inputDF: DataFrame, countByFileDateAndPartitionDate: CountByFileDateAndPartitionDate)(implicit sparkSession: SparkSession): String = {

    countByFilePartitionDateAndCollect(inputDF, countByFileDateAndPartitionDate.partitionDateColumn, countByFileDateAndPartitionDate.fileDateColumn)

  }

  /**
   * Wrapper to trim columns
   *
   * @param inputDF               inputDF
   * @param sparkSession         Implicit Spark session
   * @return DataFrame
   */

  def trimWrapper(inputDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {

    TransFunctions.trimColumns(inputDF)
  }

  /**
   * Wrapper to validateNotNull columns
   *
   * @param inputDF             inputDF
   * @param validateNotNull     ValidateNotNull type
   * @param sparkSession        Implicit Spark session
   * @return DataFrame
   */

  def validateNotNullWrapper(inputDF: DataFrame, validateNotNull: ValidateNotNull,commonPipeLineHelper:CommonPipeLineHelper,jobStartTime:String)(implicit sparkSession: SparkSession): DataFrame = {

    val nullOrEmptyRecordsLocation: String = appendPathWithTimeStamp(
      appendSlashInEnd(commonPipeLineHelper.pipelineFailures) + MarketingConstants.NULLOREMPTYRECORDS, jobStartTime)
    validateNOTNULLColumns(inputDF, nullOrEmptyRecordsLocation, validateNotNull.inputColumns)

  }

  /**
   * Wrapper to validateDataType columns
   *
   * @param inputDF             inputDF
   * @param validateDataType    ValidateDataType type
   * @param sparkSession        Implicit Spark session
   * @return DataFrame
   */

  def validateDataTypeWrapper(inputDF: DataFrame, validateDataType: ValidateDataType,commonPipeLineHelper:CommonPipeLineHelper,jobStartTime:String)(implicit sparkSession: SparkSession): DataFrame = {

    val dataTypeFailureLocation: String = appendPathWithTimeStamp(
      appendSlashInEnd(commonPipeLineHelper.pipelineFailures) + MarketingConstants.DATATYPEFAILURES, jobStartTime)
    validateDataTypes(inputDF, dataTypeFailureLocation, validateDataType.inputColumns)

  }

  /**
   * Wrapper to validateDateFormat columns
   *
   * @param inputDF             inputDF
   * @param validateDateFormat  ValidateDateFormat type
   * @param sparkSession        Implicit Spark session
   * @return DataFrame
   */

  def validateDateFormatWrapper(inputDF: DataFrame, validateDateFormat: ValidateDateFormat)(implicit sparkSession: SparkSession): DataFrame = {

    DataFrameValidatorService.validateDateFormat(inputDF, validateDateFormat.inputColumns, validateDateFormat.sourceDateFormat)


  }


  /**
   * Wrapper to replaceNullWithDefaultForQtyColumn
   *
   * @param inputDF                               inputDF
   * @param replaceNullWithDefaultForQtyColumn    ReplaceNullWithDefaultForQtyColumn type
   * @param sparkSession                          Implicit Spark session
   * @return DataFrame
   */


  def replaceNullWithDefaultForQtyColumnWrapper(inputDF: DataFrame, replaceNullWithDefaultForQtyColumn: ReplaceNullWithDefaultForQtyColumn)(implicit sparkSession: SparkSession): DataFrame = {

    replaceNullWithDefault(inputDF, replaceNullWithDefaultForQtyColumn.inputColumns, replaceNullWithDefaultForQtyColumn.default, true)

  }

  /**
   * Wrapper to replaceNullWithDefaultForAmtColumn
   *
   * @param inputDF                               inputDF
   * @param replaceNullWithDefaultForAmtColumn    ReplaceNullWithDefaultForAmtColumn type
   * @param sparkSession                          Implicit Spark session
   * @return DataFrame
   */

  def replaceNullWithDefaultForAmtColumnWrapper(inputDF: DataFrame, replaceNullWithDefaultForAmtColumn: ReplaceNullWithDefaultForAmtColumn)(implicit sparkSession: SparkSession): DataFrame = {

    replaceNullWithDefault(inputDF, replaceNullWithDefaultForAmtColumn.inputColumns, replaceNullWithDefaultForAmtColumn.default, true)

  }


  /**
   * Wrapper to replaceNullWithDefaultForStringColumn
   *
   * @param inputDF                                   inputDF
   * @param replaceNullWithDefaultForStringColumn    ReplaceNullWithDefaultForStringColumn type
   * @param sparkSession                             Implicit Spark session
   * @return DataFrame
   */

  def replaceNullWithDefaultForStringColumnWrapper(inputDF: DataFrame, replaceNullWithDefaultForStringColumn: ReplaceNullWithDefaultForStringColumn)(implicit sparkSession: SparkSession): DataFrame = {

    replaceNullWithDefault(inputDF, replaceNullWithDefaultForStringColumn.inputColumns, replaceNullWithDefaultForStringColumn.default, true)

  }

  /**
   * Wrapper to dropDups
   *
   * @param inputDF      inputDF
   * @param dropDups     DropDups type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def dropDupWrapper(inputDF: DataFrame, dropDups: DropDups)(implicit sparkSession: SparkSession): DataFrame = {

    dropDup(inputDF, dropDups.location, dropDups.partitionColumns, dropDups.orderByColumn)
  }

  /**
   * Wrapper to executeExternalSql
   *
   * @param executionOrder         ExecutionOrder type
   * @param executeExternalSql     ExecuteExternalSql type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def executeExternalSqlWrapper(executionOrder:ExecutionOrder, executeExternalSql: ExecuteExternalSql,listOfDF:ArrayBuffer[DataFrame])(implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {

    CommonExecutePipelineUtils.executeExternalSql(executionOrder.executionOrder, executeExternalSql.dataFrameNameUsedInSql, executeExternalSql.inputSql, listOfDF)

  }

  /**
   * Wrapper to executeSqlAgainstTable
   * @param executeSqlAgainstTable     ExecuteSqlAgainstTable type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def executeSqlAgainstTableWrapper(executeSqlAgainstTable: ExecuteSqlAgainstTable)(implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {

    CommonExecutePipelineUtils.executeSqlAgainstTable(executeSqlAgainstTable.inputSql,executeSqlAgainstTable.env,executeSqlAgainstTable.selectColumns)

  }

  /**
   * Wrapper to executeSqlAgainstDataframe
   *
   * @param executeSqlAgainstDataframe         ExecuteSqlAgainstDataframe type
   * @param listOfDF                           list of Dataframes that are in inputSql.Order should match the order specified in executeSqlAgainstDataframe.dataFrameOrderOfExecution
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def executeSqlAgainstDataframeWrapper(executeSqlAgainstDataframe: ExecuteSqlAgainstDataframe,listOfDF:ArrayBuffer[DataFrame])(implicit sparkSession: SparkSession): mutable.Seq[DataFrame] = {

    CommonExecutePipelineUtils.executeSqlAgainstDataframe(executeSqlAgainstDataframe.dataFrameOrderOfExecution,listOfDF, executeSqlAgainstDataframe.dataFrameNameUsedInSql, executeSqlAgainstDataframe.inputSql)

  }

  /**
   * Wrapper to removeDollarCharacter
   *
   * @param inputDF                inputDF
   * @param removeDollarCharacter  RemoveDollarCharacter type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def removeDollarCharacterWrapper(inputDF: DataFrame,removeDollarCharacter:RemoveDollarCharacter)(implicit sparkSession: SparkSession): DataFrame = {

    DataFrameUtils.removeDollarCharacter(inputDF, removeDollarCharacter.inputColumns)

  }

  /**
   * Wrapper to removeNonUtfCharacter
   *
   * @param inputDF                inputDF
   * @param removeNonUtfCharacter  RemoveNonUtfCharacter type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def removeNonUtfCharacterWrapper(inputDF: DataFrame,removeNonUtfCharacter:RemoveNonUtfCharacter)(implicit sparkSession: SparkSession): DataFrame = {

    DataFrameUtils.removeNonUtfCharacter(inputDF, removeNonUtfCharacter.inputColumns)

  }

  /**
   * Wrapper to convertMicroToCurrency
   *
   * @param inputDF                inputDF
   * @param convertMicroToCurrency ConvertMicroToCurrency type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def convertMicroToCurrencyWrapper(inputDF: DataFrame,convertMicroToCurrency:ConvertMicroToCurrency)(implicit sparkSession: SparkSession): DataFrame = {

    RevenueTransformer.convertMicroToCurrency(inputDF, convertMicroToCurrency.inputColumns)

  }

  /**
   * Wrapper to handleNullsInPKColumns
   *
   * @param inputDF                inputDF
   * @param handleNullsInPKColumns HandleNullsInPKColumns type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def handleNullsInPKColumnsWrapper(inputDF: DataFrame,handleNullsInPKColumns:HandleNullsInPKColumns)(implicit sparkSession: SparkSession): DataFrame = {

    CommonExecutePipelineUtils.handleNullsInPKColumns(inputDF, handleNullsInPKColumns.inputColumns)

  }

  /**
   * Wrapper to convertDatePartOfTsToDefault
   *
   * @param inputDF                                   inputDF
   * @param convertDatePartOfTsToDefault              ConvertDatePartOfTsToDefault type
   * @param sparkSession                             Implicit Spark session
   * @return DataFrame
   */

  def convertDatePartOfTsToDefaultWrapper(inputDF: DataFrame, convertDatePartOfTsToDefault: ConvertDatePartOfTsToDefault)(implicit sparkSession: SparkSession): DataFrame = {

    convertToSparkDateTypeFormat(inputDF, convertDatePartOfTsToDefault.inputColumns, convertDatePartOfTsToDefault.inputTsFormat)

  }

  /**
   * Wrapper to convertDateFormat
   *
   * @param inputDF                inputDF
   * @param convertDateFormat      ConvertDateFormat type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def convertDateFormatWrapper(inputDF: DataFrame,convertDateFormat:ConvertDateFormat)(implicit sparkSession: SparkSession): DataFrame = {

    DataFrameUtils.convertDateFormat(inputDF, convertDateFormat.inputColumns, convertDateFormat.inputDateFormat, convertDateFormat.outputDateFormat)

  }

  /**
   * Wrapper to convertTimestampToDate
   *
   * @param inputDF                inputDF
   * @param convertTimestampToDate ConvertTimestampToDate type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def convertTimestampToDateWrapper(inputDF: DataFrame,convertTimestampToDate:ConvertTimestampToDate)(implicit sparkSession: SparkSession): DataFrame = {

    CommonExecutePipelineUtils.convertTimestampToDate(inputDF, convertTimestampToDate.inputColumns, convertTimestampToDate.outputFormat)

  }


  /**
   * Wrapper to convertTimestampFromLocalToUtc
   *
   * @param inputDF                inputDF
   * @param convertTimestampFromLocalToUtc ConvertTimestampFromLocalToUtc type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def convertTimestampFromLocalToUtcWrapper(inputDF: DataFrame,convertTimestampFromLocalToUtc:ConvertTimestampFromLocalToUtc)(implicit sparkSession: SparkSession): DataFrame = {

    CommonExecutePipelineUtils.convertTimestampFromLocalToUtc(inputDF, convertTimestampFromLocalToUtc.inputColumns, convertTimestampFromLocalToUtc.inputFormat, convertTimestampFromLocalToUtc.outputFormat, convertTimestampFromLocalToUtc.timeZone)

  }

  /**
   * Wrapper to ConvertTimestampFromUtcToLocal
   *
   * @param inputDF                inputDF
   * @param convertTimestampFromUtcToLocal ConvertTimestampFromUtcToLocal type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def convertTimestampFromUtcToLocalWrapper(inputDF: DataFrame,convertTimestampFromUtcToLocal:ConvertTimestampFromUtcToLocal)(implicit sparkSession: SparkSession): DataFrame = {

    CommonExecutePipelineUtils.convertTimestampFromUtcToLocal(inputDF, convertTimestampFromUtcToLocal.inputColumns, convertTimestampFromUtcToLocal.inputFormat, convertTimestampFromUtcToLocal.outputFormat, convertTimestampFromUtcToLocal.timeZone)

  }


  /**
   * Wrapper to joinCal
   * @param inputDF                inputDF
   * @param joinCal                JoinCal type
   * @param commonPipeLineHelper   CommonPipeLineHelper type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def joinCalWrapper(inputDF: DataFrame,joinCal:JoinCal,commonPipeLineHelper:CommonPipeLineHelper)(implicit sparkSession: SparkSession): DataFrame = {

    CommonExecutePipelineUtils.joinCal(inputDF, joinCal.inputJoinConditionColumn,joinCal.calendarColumns,joinCal.calendarColumnsWithAnyTransformation,joinCal.calendarTable,joinCal.calendarJoinConditionColumn,joinCal.tempCalendarExtLocation,commonPipeLineHelper.jdbcURL,commonPipeLineHelper.queueName,commonPipeLineHelper.env)

  }

  /**
   * Wrapper to joinTwoDataFramesWrapper
   * @param leftDF                leftDF
   * @param rightDF               rightDF
   * @param joinTwoDataFrames     JoinTwoDataFrames type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def joinTwoDataFramesWrapper(leftDF: DataFrame,rightDF: DataFrame,joinTwoDataFrames:JoinTwoDataFrames)(implicit sparkSession: SparkSession): DataFrame = {

    DataFrameUtils.joinTwoDataFrames(leftDF,rightDF,joinTwoDataFrames.joinType,joinTwoDataFrames.joinCondition,joinTwoDataFrames.prefixRightDFCommonCol)._1

  }


  /**
   * Wrapper to rename columns
   *
   * @param inputDF      inputDF
   * @param rename       Rename type
   * @param sparkSession         Implicit Spark session
   * @return DataFrame
   */

  def renameWrapper(inputDF: DataFrame, rename: Rename)(implicit sparkSession: SparkSession): DataFrame = {

    renameColumns(inputDF, rename.renameColumns)
  }

  /**
   * Checks the key fields for data quality issues in both the initial DataFrame loaded from files (if there is any ,
   * transformations on the source dataframe ,do it intially in the pipeline and provide that as source),
   * and the final DataFrame which has been prepared to write to core history.
   *
   * @param sourceDF     The initial DataFrame
   * @param targetDF     The final DataFrame that has been cleansed and transformed
   * @param dqCheck      DqCheck  type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */
  def dqCheckWrapper(sourceDF: DataFrame, targetDF: DataFrame, dqCheck: DqCheck)(implicit sparkSession: SparkSession): DataFrame = {

    DataQualityPublisherGeneric.dqCheck(sourceDF, targetDF,
      DataQualityPublisherGeneric(dqCheck.pipeLineConfig,
        dqCheck.dqColumns,
        dqCheck.dupsCheckKey,
        dqCheck.nullsCheckKey,
        dqCheck.loggerName,
        reportingDateColumnFlag = dqCheck.reportingDateColumnFlag,
        srcColumnrenameList = dqCheck.srcColumnRenameList,
        tgtColumnrenameList = dqCheck.tgtColumnRenameList,
        scale = dqCheck.scale,
        throwExceptionUponFailure = dqCheck.throwExceptionUponFailure,
        nullCheckFlag = dqCheck.nullCheckFlag,
        dupCheckFlag = dqCheck.dupCheckFlag,
        partitionDateColName = dqCheck.partitionDateColName,
        threshold = dqCheck.threshold,
        thresholdPercent = dqCheck.thresholdPercent,
        thresholdPercentJson = dqCheck.thresholdPercentJson,
        ignoreCountCheck = dqCheck.ignoreCountCheck,
        targetTablename = dqCheck.targetTablename,
        dqCheckResultShowToLog = dqCheck.dqCheckResultShowToLog
      )
    )

  }

  /**
   * Wrapper to load atomic hist
   *
   * @param atomicHistLoad       AtomicHistLoad type
   * @param commonPipeLineHelper CommonPipeLineHelper type
   * @param sparkSession         Implicit Spark session
   * @return Unit
   */

  def atomicHistLoadWrapper(inputDF: DataFrame,
                            atomicHistLoad: AtomicHistLoad,
                            commonPipeLineHelper: CommonPipeLineHelper
                           )
                           (implicit sparkSession: SparkSession): DataFrame = {
    if (commonPipeLineHelper.env != "local") {
      val writer: HiveExternalTableWriter = HiveExternalAtomicTable(
        env = commonPipeLineHelper.env,
        schemaName = commonPipeLineHelper.hiveLandingSchema,
        tableName = commonPipeLineHelper.lndCoreHistTable,
        partitionColumns = atomicHistLoad.partitionColumns,
        insertColumnNames = atomicHistLoad.insertColumns,
        repartition = atomicHistLoad.repartition
      ).writer

      val successfulWrite = HiveExternalTableWriter.writeToExternalFndTable(inputDF, writer)

      if (successfulWrite) {
        log.info(s"Successfully wrote to external atomic history table ${commonPipeLineHelper.hiveLandingSchema}.${commonPipeLineHelper.lndCoreHistTable}")
      } else {
        throw HiveWriteException(s"Failed while writing to external atomic history table ${commonPipeLineHelper.hiveLandingSchema}.${commonPipeLineHelper.lndCoreHistTable}")
      }
    }
    inputDF
  }

  /**
   * Wrapper to insert into foundation or CoreHist
   *
   * @param fndDF                Foundation or coreHist dataframe to be inserted to external table
   * @param fndLoad              FndLoad type
   * @param commonPipeLineHelper CommonPipeLineHelper type
   * @param sparkSession         Implicit Spark session
   * @return Unit
   */

  def fndLoadWrapper(fndDF: DataFrame,
                     fndLoad: FndLoad,
                     commonPipeLineHelper: CommonPipeLineHelper
                    )(implicit sparkSession: SparkSession): DataFrame = {


    log.info("Writing data to external Hive table")
    setJobGroup("Writing data to external Hive table", "Writing data to external Hive table")
    val successfulWrite = HiveExternalTableWriter.writeToExternalFndTable(fndDF, HiveExternalTableWriter(
      commonPipeLineHelper.env,
      insertColumnNames = fndLoad.insertColumns,
      partitionColumnNames = fndLoad.partitionColumns,
      dirWriteLocation = if (commonPipeLineHelper.env != "local") "" else commonPipeLineHelper.fndExtLocation,
      schema = commonPipeLineHelper.fndDb,
      tableName = commonPipeLineHelper.fndTable,
      repartition = fndLoad.repartition))(sparkSession)
    if (successfulWrite) {
      log.info(s"Successfully wrote to external Hive table ${commonPipeLineHelper.fndDb}.${commonPipeLineHelper.fndTable}")
    } else {
      throw HiveWriteException(s"Failed while writing to Hive table ${commonPipeLineHelper.fndDb}.${commonPipeLineHelper.fndTable}")
    }

    fndDF
  }

  /**
   * Wrapper to move all of the processed files from the processing directory to archive partition
   *
   * @param archiveArchivePartition ArchiveArchivePartition type
   * @param commonPipeLineHelper    CommonPipeLineHelper type
   * @param sparkSession            Implicit Spark session
   * @return Unit
   */
  def archiveArchivePartitionWrapper(archiveArchivePartition: ArchiveArchivePartition,commonPipeLineHelper: CommonPipeLineHelper)
                                    (implicit sparkSession: SparkSession): Unit = {

    //move files to archive partition and archive and remove file from inputEphemeralPath
    if (commonPipeLineHelper.env != "local") {
      val archiveMovedFileSize: BigInt = FileWriterService.hdfsMove(FileWriterService(
        commonPipeLineHelper.inputEphemeralPath,
        commonPipeLineHelper.archivePathPartition,
        zerobyteabort = false,
        dbName = commonPipeLineHelper.hiveLandingSchema,
        partition = true,
        tablename = commonPipeLineHelper.lndCoreHistTable,
        droppartition = true,
        jdbcUrl = commonPipeLineHelper.jdbcURL,
        queueName = commonPipeLineHelper.queueName,
        deletesource = archiveArchivePartition.deleteSource,
        fileDateRegexPattern = "(\\d{4})-(\\d{2})-(\\d{2})",
        fileDateFormatter = "yyyy-MM-dd",
        partitionColName = archiveArchivePartition.partitionColName
      ))
      if (!archiveMovedFileSize.>(0)) throw FileMoveException("Failed to remove ingested source files from ephemeral folder")

      //      val archiveCopyFileSize: BigInt = FileWriterService.hdfsMove(FileWriterService(
      //        commonPipeLineHelper.inputEphemeralPath,
      //        commonPipeLineHelper.archivePath,
      //        zerobyteabort = false,
      //        deletesource = true
      //      ))
      //      if (!archiveCopyFileSize.>(0)) throw FileCopyException("Failed to copy ingested source files to archival directory")
      //      log.info(s"Filesize moved to archivePath ${commonPipeLineHelper.archivePath} is $archiveCopyFileSize")
      log.info(s"Filesize moved to archivePartitionPath ${commonPipeLineHelper.archivePathPartition} is $archiveMovedFileSize")
    }
  }

  /**
   * Wrapper to move all of the processed files from the processing directory into the archive directory
   *
   * @param archiveEphemeral     ArchiveEphemeral type
   * @param commonPipeLineHelper CommonPipeLineHelper type
   * @param sparkSession         Implicit Spark session
   * @return Unit
   */
  def archiveEphemeralWrapper(archiveEphemeral: ArchiveEphemeral,commonPipeLineHelper: CommonPipeLineHelper)(implicit sparkSession: SparkSession): Unit = {

    //moved files to archive and remove file from inputEphemeralPath
    // if (commonPipeLineHelper.env != "local") {
    val archiveCopyFileSize: BigInt = FileWriterService.hdfsMove(FileWriterService(
      commonPipeLineHelper.inputEphemeralPath,
      commonPipeLineHelper.archivePath,
      zerobyteabort = false,
      deletesource = archiveEphemeral.deleteSource)
    )
    if (!archiveCopyFileSize.>(0)) throw FileCopyException("Failed to copy ingested source files to archival directory")
    log.info(s"Filesize moved to archivePath ${commonPipeLineHelper.archivePath} is $archiveCopyFileSize")
    //}

  }

  /**
   * Wrapper to DeleteTriggerFile
   * @param deleteTriggerFile      DeleteTriggerFile type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def deleteTriggerFileWrapper(deleteTriggerFile:DeleteTriggerFile)(implicit sparkSession: SparkSession): Any = {

    CommonExecutePipelineUtils.deleteTriggerFile(deleteTriggerFile.doneFile)

  }


  /**
   * Wrapper to updateCheckPointPath
   * @param readInputKafka         ReadInputKafka type
   * @param sparkSession Implicit Spark session
   * @return DataFrame
   */

  def updateCheckPointPathWrapper(readInputKafka:ReadInputKafka,maxProcessedPartition:String)(implicit sparkSession: SparkSession): Unit = {

    updateCheckPointPath(readInputKafka.checkpointPath, maxProcessedPartition)

  }

  /**
   * Wrapper to sendDFToEmail
   * @param inputDF         inputDF
   * @param sendDFToEmailType         SendDFToEmail Type
   * @param commonPipeLineHelper      CommonPipeLineHelper Type

   * @param sparkSession Implicit Spark session
   * @return Unit
   */

  def sendDFToEmailWrapper(inputDF:DataFrame,sendDFToEmailType:SendDFToEmail,commonPipeLineHelper: CommonPipeLineHelper)(implicit sparkSession: SparkSession): Unit = {

    sendDFToEmail(inputDF,commonPipeLineHelper.env,sendDFToEmailType.customSqlFlag,sendDFToEmailType.customSql,sendDFToEmailType.fromAddress,sendDFToEmailType.toAddress,sendDFToEmailType.reportFilename,sendDFToEmailType.emailSubject,sendDFToEmailType.emailBodySubject,sendDFToEmailType.attachDFResult,sendDFToEmailType.attachDFResultToEmailBody,sendDFToEmailType.debug)

  }


  /**
   * Wrapper which creates sftp connection and calls transferFromSftpToHdfs to transfer file from SFTP to HDFS
   * Recursively transfer file from sftp server to hdfs.If sftp location contains sub directory then it copies files from sub directory as well
   *  transferFromSftpToHdfs:       TransferFromSftpToHdfs type

   * @return Unit
   */
  def transferFromSftpToHdfsWrapper(transferFromSftpToHdfs:TransferFromSftpToHdfs)(implicit sparkSession: SparkSession): Unit = {

    log.info("Inside transferFromSftpToHdfsWrapper")

    val sftpTransferService = new SftpTransferService
    //val sftpTransferConfig= SftpTransferConfig(pipeLineConfig)
    val sftpConnection:(ChannelSftp,SFTPClient) = sftpTransferService.createSftpConnection(transferFromSftpToHdfs.sftpPasswordJceKeyStorePath,
      transferFromSftpToHdfs.sftpPasswordJceKeyName,
      transferFromSftpToHdfs.sftpPrivateKeyPath,
      transferFromSftpToHdfs.sftpUser,
      transferFromSftpToHdfs.sftpHost,
      transferFromSftpToHdfs.sftpPort)
    try {
      val listOfRemoteAndHdfsFilesWithFileSize= sftpTransferService.transferFromSftpToHdfs(sftpConnection,
        transferFromSftpToHdfs.sftpFileLocationPath,
        transferFromSftpToHdfs.hdfsFileLocationPath,
        transferFromSftpToHdfs.sftpFileRenameFileName,
        transferFromSftpToHdfs.sftpSearchFilePattern,
        transferFromSftpToHdfs.copyMechanismType,
        transferFromSftpToHdfs.fileCompletenessCheck,
        transferFromSftpToHdfs.retriesForFileCompletenessCheck,
        transferFromSftpToHdfs.sleepForFileCompletenessCheck,
        transferFromSftpToHdfs.isFileAlreadySftpCheck,
        transferFromSftpToHdfs.hdfsFileLocationToCheckIsFileAlreadySftpCheck
      )
      log.info(s"List of Sftp and Hdfs files and file size moved from Sftp to Hdfs ${listOfRemoteAndHdfsFilesWithFileSize.mkString(",")}")
      disConnectSftpConnection(sftpConnection._1)
      //can add if there no files throw exception
    } catch {
      case e: Exception =>
        disConnectSftpConnection(sftpConnection._1)
        throw TransferFromSftpToHdfsException(s"Failed to push from remote ${transferFromSftpToHdfs.sftpFileLocationPath}" +
          s" to HDFS ${transferFromSftpToHdfs.hdfsFileLocationPath}=>" + e)
    }

  }

  /**
   *  Wrapper which creates sftp connection and calls transferFromHdfsToSftp to transfer file from HDFS to SFTP
   *  Recursively transfer file from HDFS location to SFTP location.If hdfs location contains sub directory then it copies files from sub directory as well
   *  transferFromHdfsToSftp:       TransferFromHdfsToSftp type
   * @return Unit
   */

  def transferFromHdfsToSftpWrapper(transferFromHdfsToSftp:TransferFromHdfsToSftp
                                   )(implicit sparkSession: SparkSession): Unit = {

    log.info("Inside transferFromHdfsToSftpWrapper")
    val sftpTransferService = new SftpTransferService
    //val sftpTransferConfig= SftpTransferConfig(pipeLineConfig)
    val sftpChannel: ChannelSftp = sftpTransferService.createSftpConnection(transferFromHdfsToSftp.sftpPasswordJceKeyStorePath,
      transferFromHdfsToSftp.sftpPasswordJceKeyName,
      transferFromHdfsToSftp.sftpPrivateKeyPath,
      transferFromHdfsToSftp.sftpUser,
      transferFromHdfsToSftp.sftpHost,
      transferFromHdfsToSftp.sftpPort)._1
    try {
      val listOfHdfsAndRemoteFilesWithFileSize =sftpTransferService.transferFromHdfsToSftp(sftpChannel,
        transferFromHdfsToSftp.sftpFileLocationPath,
        transferFromHdfsToSftp.hdfsFileLocationPath,
        transferFromHdfsToSftp.hdfsFileRenameFileName,
        transferFromHdfsToSftp.hdfsSearchFilePattern,
        transferFromHdfsToSftp.copyMechanismType,
        transferFromHdfsToSftp.sftpSleep,
        transferFromHdfsToSftp.hdfsFileRenameFlag)
      log.info(s"List of Hdfs and Sftp files and file size moved from hdfs to sftp ${listOfHdfsAndRemoteFilesWithFileSize.mkString(",")}")
      disConnectSftpConnection(sftpChannel)
      //can add if there no files throw exception
    } catch {
      case e: Exception =>
        disConnectSftpConnection(sftpChannel)
        throw  TransferFromHdfsToSftpException(s"Failed to push from HDFS path $transferFromHdfsToSftp.{hdfsFileLocationPath}" +
          s" to remote path ${transferFromHdfsToSftp.sftpFileLocationPath}==>" + e)
    }
  }

  /**
   * Wrapper to saveDFToHdfs
   * @param inputDF         inputDF
   * @param saveDFToHdfs    saveDFToHdfs Type

   * @param sparkSession Implicit Spark session
   * @return Unit
   */

  def saveDFToHdfsWrapper(inputDF:DataFrame,saveDFToHdfs:SaveDFToHdfs)(implicit sparkSession: SparkSession): Unit = {

    CommonExecutePipelineUtils.saveDFToHdfs(inputDF,saveDFToHdfs.selectColumns,saveDFToHdfs.filterCondition,saveDFToHdfs.hdfsTempFileLocation,saveDFToHdfs.hdfsFileLocation,saveDFToHdfs.hdfsSaveOptions,saveDFToHdfs.hdfsSaveMode,saveDFToHdfs.hdfsSaveFileFormat,saveDFToHdfs.hdfsSaveCoalesce,saveDFToHdfs.coalesceInputSplit,saveDFToHdfs.hdfsRenameFilename,saveDFToHdfs.hdfsRenameFileSearchPattern,saveDFToHdfs.deleteHdfsFileSearchPattern,saveDFToHdfs.deleteHdfsFileRecursive)

  }

}