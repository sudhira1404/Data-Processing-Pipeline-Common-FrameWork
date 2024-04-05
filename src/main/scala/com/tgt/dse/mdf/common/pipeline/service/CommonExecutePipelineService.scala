package com.tgt.dse.mdf.common.pipeline.service


import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants.DATEFORMATTER
import com.tgt.dse.mdf.common.pipeline.exceptions.ZeroRecordInDataframeException
import com.tgt.dse.mdf.common.pipeline.util.CommonExecutePipelineUtils.{addDataframeToListAndHandleEmptyTopicAndShowResult, addDataframeToListAndHandleZeroRecordAndShowResult, addDataframeToListAndShowResult, convertTimestampFromLocalToUtc, convertTimestampFromUtcToLocal, convertTimestampToDate, deleteLockAndHiddenFileFromLocal, deleteTriggerFile, dropDup, executeExternalSql, handleNullsInPKColumns, joinCal, pickDFFromListOfDFBasedOnOrderOfExecution, showCustomResultsToLog, showResultsToLog, stringToDataFrameColumns}
import com.tgt.dse.mdf.common.pipeline.util.DataFrameUtils.convertDateFormat
import com.tgt.dse.mdf.common.pipeline.vo.CommonExecutePipelineParameterObjects.{AbortOnZeroRecords, AddDatePartitionColumn, AddFileDate, ArchiveArchivePartition, ArchiveEphemeral, AtomicHistLoad, CommonPipeLineHelper, ConvertDateFormat, ConvertDatePartOfTsToDefault, ConvertMicroToCurrency, ConvertTimestampFromLocalToUtc, ConvertTimestampFromUtcToLocal, ConvertTimestampToDate, CountByFileDateAndPartitionDate, DeleteTriggerFile, DqCheck, DropDups, ExecuteExternalSql, ExecutionOrder, FndLoad, GetLatestByKeyCol, HandleNullsInPKColumns, JoinCal, JoinTwoDataFrames, ReadInput, ReadInputKafka, RemoveDollarCharacter, RemoveNonUtfCharacter, Rename, ReplaceNullWithDefaultForAmtColumn, ReplaceNullWithDefaultForQtyColumn, ReplaceNullWithDefaultForStringColumn, SaveDFToHdfs, SendDFToEmail, ShowCustomResultsToLog, ShowResultsToLog, TransferFromHdfsToSftp, TransferFromSftpToHdfs, ValidateDataType, ValidateDateFormat, ValidateNotNull, ValidateSchema}
import com.tgt.dse.mdf.common.pipeline.wrapper.CommonExecutePipelineWrapper.{addDatePartitionColumnWrapper, addFileDateWrapper, archiveArchivePartitionWrapper, archiveEphemeralWrapper, atomicHistLoadWrapper, convertDatePartOfTsToDefaultWrapper, convertMicroToCurrencyWrapper, convertTimestampFromLocalToUtcWrapper, convertTimestampFromUtcToLocalWrapper, convertTimestampToDateWrapper, countByFileDateAndPartitionDateWrapper, deleteTriggerFileWrapper, dqCheckWrapper, dropDupWrapper, executeExternalSqlWrapper, flattenWrapper, fndLoadWrapper, getLatestByKeyColWrapper, handleNullsInPKColumnsWrapper, joinCalWrapper, joinTwoDataFramesWrapper, moveEphemeralFilesToProcessWrapper, readInputFilesWrapper, readInputKafkaFilesWrapper, removeDollarCharacterWrapper, removeNonUtfCharacterWrapper, renameWrapper, replaceNullWithDefaultForAmtColumnWrapper, replaceNullWithDefaultForQtyColumnWrapper, replaceNullWithDefaultForStringColumnWrapper, saveDFToHdfsWrapper, sendDFToEmailWrapper, transferFromHdfsToSftpWrapper, transferFromSftpToHdfsWrapper, trimWrapper, updateCheckPointPathWrapper, validateDataTypeWrapper, validateDateFormatWrapper, validateNotNullWrapper, validateSchemaWrapper}
import com.tgt.dse.mdf.common.pipeline.{LoggingTrait, MarketingPipelineConfigs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer

trait CommonExecutePipelineService extends LoggingTrait {

  object CommonExecutePipeLineService {

    /**
     * Will execute all the transformation/Action names mentioned in app.executionOrder.executionOrder and returns
     * list of output of dataframe after executing each transformation/Action from executionOrder
     * If any transformation does not output a Dataframe then previous transformation Dataframe in stored in list of output of dataframe
     * like archiveArchivePartition will move files but will add the previous transformation output dataframe to list,so
     * if there 9 entries in executionOrder then there will 9 dataframes in the list of final output with 0-8 as index
     * and pick any dataframe and apply subsequent transformation specific to project
     * @param config    MarketingPipelineConfigs Type
     * @return ArrayBuffer[DataFrame]
     */

    @SuppressWarnings(Array("maxLength"))
    def executePipeLine(config: MarketingPipelineConfigs):ArrayBuffer[DataFrame] = {

      val jobStartTime: String = LocalDateTime.now().format(DATEFORMATTER)
      val initializeExecutionOrder = ExecutionOrder(config.pipeLineConfig)
      val initializeShowResultsToLog = ShowResultsToLog(config.pipeLineConfig)
      val initializeShowCustomResultsToLog = ShowCustomResultsToLog(config.pipeLineConfig)
      val initializeAbortOnZeroRecords = AbortOnZeroRecords(config.pipeLineConfig)
      val initializeCommonPipeLineHelper = CommonPipeLineHelper(config.pipeLineConfig)
      val initializeReadInput = ReadInput(config.pipeLineConfig)
      val initializeReadInputKafka = ReadInputKafka(config.pipeLineConfig)
      val initializeValidateSchema = ValidateSchema(config.pipeLineConfig)
      val initializeAddFileDate = AddFileDate(config.pipeLineConfig)
      val initializeAddDatePartitionColumn = AddDatePartitionColumn(config.pipeLineConfig)
      val initializeExecuteExternalSql = ExecuteExternalSql(config.pipeLineConfig)
      val initializeGetLatestByKeyCol = GetLatestByKeyCol(config.pipeLineConfig)
      val initializeCountByFileDateAndPartitionDate = CountByFileDateAndPartitionDate(config.pipeLineConfig)
      val initializeDropDups = DropDups(config.pipeLineConfig)
      val initializeValidateNotNull = ValidateNotNull(config.pipeLineConfig)
      val initializeValidateDataType = ValidateDataType(config.pipeLineConfig)
      val initializeValidateDateFormat = ValidateDateFormat(config.pipeLineConfig)
      val initializeRemoveDollarCharacter = RemoveDollarCharacter(config.pipeLineConfig)
      val initializeRemoveNonUtfCharacter = RemoveNonUtfCharacter(config.pipeLineConfig)
      val initializeReplaceNullWithDefaultForQtyColumn = ReplaceNullWithDefaultForQtyColumn(config.pipeLineConfig)
      val initializeReplaceNullWithDefaultForAmtColumn = ReplaceNullWithDefaultForAmtColumn(config.pipeLineConfig)
      val initializeReplaceNullWithDefaultForStringColumn = ReplaceNullWithDefaultForStringColumn(config.pipeLineConfig)
      val initializeConvertMicroToCurrency = ConvertMicroToCurrency(config.pipeLineConfig)
      val initializeHandleNullsInPKColumns = HandleNullsInPKColumns(config.pipeLineConfig)
      val initializeConvertDatePartOfTsToDefault = ConvertDatePartOfTsToDefault(config.pipeLineConfig)
      val initializeConvertDateFormat = ConvertDateFormat(config.pipeLineConfig)
      val initializeConvertTimestampToDate = ConvertTimestampToDate(config.pipeLineConfig)
      val initializeConvertTimestampFromLocalToUtc = ConvertTimestampFromLocalToUtc(config.pipeLineConfig)
      val initializeConvertTimestampFromUtcToLocal = ConvertTimestampFromUtcToLocal(config.pipeLineConfig)
      val initializeJoinCal = JoinCal(config.pipeLineConfig)
      val initializeJoinTwoDataFrames = JoinTwoDataFrames(config.pipeLineConfig)
      val initializeRename = Rename(config.pipeLineConfig)
      val initializeDqCheck = DqCheck(config.pipeLineConfig)
      val initializeArchiveArchivePartition = ArchiveArchivePartition(config.pipeLineConfig)
      val initializeArchiveEphemeral = ArchiveEphemeral(config.pipeLineConfig)
      val initializeAtomicHistLoad = AtomicHistLoad(config.pipeLineConfig)
      val initializeFndLoad = FndLoad(config.pipeLineConfig)
      val initializeDeleteTriggerFile = DeleteTriggerFile(config.pipeLineConfig)
      val initializeSendDFToEmail = SendDFToEmail(config.pipeLineConfig)
      val initializeSaveDFToHdfs = SaveDFToHdfs(config.pipeLineConfig)
      val initializeTransferFromSftpToHdfs = TransferFromSftpToHdfs(config.pipeLineConfig)
      val initializeTransferFromHdfsToSftp = TransferFromHdfsToSftp(config.pipeLineConfig)

      implicit val sparkSession: SparkSession = config.sparkSession

      deleteLockAndHiddenFileFromLocal(config, initializeCommonPipeLineHelper)

      log.info("In executePipeLine Method")

      //declare any variable used in the fold
      val listedValidPathsForKafkaRead = List.empty[String]
      val listOfDF = ArrayBuffer[DataFrame]()

      val executionOrderDF = stringToDataFrameColumns(initializeExecutionOrder.executionOrder)
      showResultsToLog(executionOrderDF, true, "Order of Execution", 1)

      //val finalDF:DataFrame =
      try {
        val foldOutputDF = executionOrderDF.columns
          .foldLeft(executionOrderDF) { (outputDF, colName) =>
            colName match {
              case "moveEphemeralFilesToProcess" => log.info("In moveEphemeralFilesToProcess and check for zero byte file");
                moveEphemeralFilesToProcessWrapper(initializeCommonPipeLineHelper)
                addDataframeToListAndShowResult(listOfDF, outputDF, "moveEphemeralFilesToProcess", initializeCommonPipeLineHelper, initializeShowResultsToLog)

                outputDF

              case "readInput" => log.info("In readInput")
                val readInputFilesDF = readInputFilesWrapper(initializeCommonPipeLineHelper, initializeReadInput)(sparkSession)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, readInputFilesDF, "readInput", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)
                readInputFilesDF.persist(StorageLevel.MEMORY_AND_DISK)

                readInputFilesDF

              case "readInputKafka" => log.info("In readInput")
                val readInputKafkaFilesTuple = readInputKafkaFilesWrapper(initializeCommonPipeLineHelper, initializeReadInputKafka)(sparkSession)
                val readInputKafkaFilesDF: DataFrame = readInputKafkaFilesTuple._1
                val listedValidPaths = readInputKafkaFilesTuple._2 :+ listedValidPathsForKafkaRead
                val inputCount: Long = readInputKafkaFilesTuple._3
                addDataframeToListAndHandleEmptyTopicAndShowResult(listOfDF, readInputKafkaFilesDF, "readInputKafka", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeReadInputKafka.handleEmptyTopic, initializeReadInputKafka.fndTablePartitionColumn, initializeCommonPipeLineHelper, initializeShowResultsToLog)
                readInputKafkaFilesDF.persist(StorageLevel.MEMORY_AND_DISK)
                log.info(s"Input count after reading from kafka=>$inputCount")
                readInputKafkaFilesDF

              case "validateSchema" => log.info("In validateSchema")
                val validateSchemaDF = validateSchemaWrapper(outputDF, initializeValidateSchema)(sparkSession)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, validateSchemaDF, "validateSchema", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                validateSchemaDF

              case "flatten" => log.info("In flatten")
                val FlattenDF = flattenWrapper(outputDF)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, FlattenDF, "flatten", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                FlattenDF

              case "addFileDate" => log.info("In addFileDate")
                val addFileDateDF = addFileDateWrapper(outputDF, initializeAddFileDate)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, addFileDateDF, "addFileDate", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                addFileDateDF

              case "addDatePartitionColumn" => log.info("In addDatePartitionColumn")
                val addPartitionColumnDF = addDatePartitionColumnWrapper(outputDF, initializeAddDatePartitionColumn)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, addPartitionColumnDF, "addDatePartitionColumn", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                addPartitionColumnDF

              case "getLatestByKeyCol" => log.info("In getLatestByKeyCol")
                val getLatestByKeyColDF = getLatestByKeyColWrapper(outputDF, initializeGetLatestByKeyCol)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, getLatestByKeyColDF, "getLatestByKeyCol", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                getLatestByKeyColDF

              case "countByFileDateAndPartitionDate" => log.info("In countByFileDateAndPartitionDate")
                countByFileDateAndPartitionDateWrapper(outputDF, initializeCountByFileDateAndPartitionDate)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, outputDF, "countByFileDateAndPartitionDate", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                outputDF

              case "executeExternalSql" => log.info("In executeExternalSql")
                executeExternalSqlWrapper(initializeExecutionOrder, initializeExecuteExternalSql, listOfDF)
                listOfDF += outputDF
                outputDF

              case "dropDups" => log.info("In dropDups");
                val dropDupsDF = dropDupWrapper(outputDF, initializeDropDups)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, dropDupsDF, "dropDups", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                dropDupsDF

              case "trim" => log.info("In trim");
                val trimDF = trimWrapper(outputDF)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, trimDF, "trim", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                trimDF

              case "validateNotNull" => log.info("In validateNotNull")
                val validateNotNullDF = validateNotNullWrapper(outputDF, initializeValidateNotNull,initializeCommonPipeLineHelper,jobStartTime)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, validateNotNullDF, "validateNotNull", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                validateNotNullDF

              case "validateDataType" => log.info("In validateDataType")

                val validateDataTypeDF = validateDataTypeWrapper(outputDF, initializeValidateDataType,initializeCommonPipeLineHelper,jobStartTime)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, validateDataTypeDF, "validateDataType", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                validateDataTypeDF

              case "validateDateFormat" => log.info("In validateDateFormat")
                val validateDateFormatDF = validateDateFormatWrapper(outputDF, initializeValidateDateFormat)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, validateDateFormatDF, "validateDateFormat", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                validateDateFormatDF

              case "removeDollarCharacter" => log.info("In removeDollarCharacter")
                val removeDollarCharacterDF: DataFrame = removeDollarCharacterWrapper(outputDF, initializeRemoveDollarCharacter)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, removeDollarCharacterDF, "removeDollarCharacter", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                removeDollarCharacterDF

              case "removeNonUtfCharacter" => log.info("In removeNonUtfCharacter")
                val removeNonUtfCharacterDF: DataFrame = removeNonUtfCharacterWrapper(outputDF, initializeRemoveNonUtfCharacter)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, removeNonUtfCharacterDF, "removeNonUtfCharacter", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                removeNonUtfCharacterDF

              case "replaceNullWithDefaultForQtyColumn" => log.info("In replaceNullWithDefaultForQtyColumn")
                val replaceNullWithDefaultForQtyColumnDF = replaceNullWithDefaultForQtyColumnWrapper(outputDF, initializeReplaceNullWithDefaultForQtyColumn)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, replaceNullWithDefaultForQtyColumnDF, "replaceNullWithDefaultForQtyColumn", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                replaceNullWithDefaultForQtyColumnDF

              case "replaceNullWithDefaultForAmtColumn" => log.info("In replaceNullWithDefaultForAmtColumn")
                val replaceNullWithDefaultForAmtColumnDF = replaceNullWithDefaultForAmtColumnWrapper(outputDF, initializeReplaceNullWithDefaultForAmtColumn)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, replaceNullWithDefaultForAmtColumnDF, "replaceNullWithDefaultForAmtColumn", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                replaceNullWithDefaultForAmtColumnDF

              case "replaceNullWithDefaultForStringColumn" => log.info("In replaceNullWithDefaultForStringColumn")
                val replaceNullWithDefaultForStringColumnDF = replaceNullWithDefaultForStringColumnWrapper(outputDF, initializeReplaceNullWithDefaultForStringColumn)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, replaceNullWithDefaultForStringColumnDF, "replaceNullWithDefaultForStringColumn", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                replaceNullWithDefaultForStringColumnDF

              case "convertMicroToCurrency" => log.info("In convertMicroToCurrency")
                val convertMicroToCurrencyDF = convertMicroToCurrencyWrapper(outputDF, initializeConvertMicroToCurrency)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, convertMicroToCurrencyDF, "convertMicroToCurrency", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                convertMicroToCurrencyDF

              case "handleNullsInPKColumns" => log.info("In handleNullsInPKColumns")
                val handleNullsInPKColumnsDF = handleNullsInPKColumnsWrapper(outputDF, initializeHandleNullsInPKColumns)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, handleNullsInPKColumnsDF, "handleNullsInPKColumns", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                handleNullsInPKColumnsDF

              case "convertDatePartOfTsToDefault" => log.info("In convertDatePartOfTsToDefault");
                val convertDatePartOfTsToDefaultDF = convertDatePartOfTsToDefaultWrapper(outputDF, initializeConvertDatePartOfTsToDefault)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, convertDatePartOfTsToDefaultDF, "convertDatePartOfTsToDefault", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                convertDatePartOfTsToDefaultDF

              case "convertDateFormat" => log.info("In convertDateFormat")
                val convertDateFormatDF = convertDateFormat(outputDF, initializeConvertDateFormat.inputColumns, initializeConvertDateFormat.inputDateFormat, initializeConvertDateFormat.outputDateFormat)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, convertDateFormatDF, "convertDateFormat", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                convertDateFormatDF

              case "convertTimestampToDate" => log.info("In convertTimestampToDate")
                val convertTimestampToDateDF = convertTimestampToDateWrapper(outputDF, initializeConvertTimestampToDate)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, convertTimestampToDateDF, "convertTimestampToDate", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                convertTimestampToDateDF

              case "convertTimestampFromLocalToUtc" => log.info("In convertTimestampFromLocalToUtc")
                val convertTimestampFromLocalToUtcDF = convertTimestampFromLocalToUtcWrapper(outputDF, initializeConvertTimestampFromLocalToUtc)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, convertTimestampFromLocalToUtcDF, "convertTimestampFromLocalToUtc", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                convertTimestampFromLocalToUtcDF

              case "convertTimestampFromUtcToLocal" => log.info("In convertTimestampFromUtcToLocal")
                val convertTimestampFromUtcToLocalDF = convertTimestampFromUtcToLocalWrapper(outputDF, initializeConvertTimestampFromUtcToLocal)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, convertTimestampFromUtcToLocalDF, "convertTimestampFromUtcToLocal", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                convertTimestampFromUtcToLocalDF

              case "joinCal" => log.info("In joinCal")
                val joinCalDF = joinCalWrapper(outputDF, initializeJoinCal,initializeCommonPipeLineHelper)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, joinCalDF, "joinCal", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                joinCalDF

              case "joinTwoDataFrames" => log.info("In joinTwoDataFrames")
                val joinTwoDataFramesLeftAndRightDF = pickDFFromListOfDFBasedOnOrderOfExecution(initializeExecutionOrder.executionOrder, initializeJoinTwoDataFrames.joinTwoDataframesLeftAndRightDFName(0), initializeJoinTwoDataFrames.joinTwoDataframesLeftAndRightDFName(1), listOfDF)
                val joinTwoDataFramesLeftDF = joinTwoDataFramesLeftAndRightDF._1
                val joinTwoDataFramesRightDF = joinTwoDataFramesLeftAndRightDF._2
                val joinTwoDataFramesDF = joinTwoDataFramesWrapper(joinTwoDataFramesLeftDF,joinTwoDataFramesRightDF, initializeJoinTwoDataFrames)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, joinTwoDataFramesDF, "joinTwoDataFrames", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                joinTwoDataFramesDF

              case "rename" => log.info("In rename")
                val renameDF = renameWrapper(outputDF, initializeRename)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, renameDF, "rename", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                renameDF

              case "dqCheck" => log.info("In dqCheck")
                val dqCheckSrcAndTgtDF = pickDFFromListOfDFBasedOnOrderOfExecution(initializeExecutionOrder.executionOrder, initializeDqCheck.dqCheckSourceAndTargetDFName(0), initializeDqCheck.dqCheckSourceAndTargetDFName(1), listOfDF)
                val dqCheckSourceDF = dqCheckSrcAndTgtDF._1
                val dqCheckTargetDF = dqCheckSrcAndTgtDF._2
                log.info(s"dqCheckSourceDF = $dqCheckSourceDF and dqCheckTargetDF = $dqCheckTargetDF")
                val dqCheckDF = dqCheckWrapper(dqCheckSourceDF, dqCheckTargetDF, initializeDqCheck)
                listOfDF += dqCheckDF
                outputDF

              case "archiveArchivePartition" => log.info("In archiveArchivePartition")
                val archiveArchivePartitionDF = archiveArchivePartitionWrapper(initializeArchiveArchivePartition,initializeCommonPipeLineHelper)
                listOfDF += outputDF
                outputDF

              case "archiveEphemeral" => log.info("In archiveEphemeral")
                archiveEphemeralWrapper(initializeArchiveEphemeral,initializeCommonPipeLineHelper)
                listOfDF += outputDF
                //showResultsToLog(archiveEphemeralDF, initializeShowResultsToLog.showResultsToLog)
                outputDF

              case "atomicHistLoad" => log.info("In atomicHistLoad")
                val atomicHistLoadSrcDF = pickDFFromListOfDFBasedOnOrderOfExecution(initializeExecutionOrder.executionOrder, initializeAtomicHistLoad.inputDFNameFromExecutionOrder, initializeAtomicHistLoad.inputDFNameFromExecutionOrder, listOfDF)._1
                log.info(s"atomicHistLoadSrcDF = $atomicHistLoadSrcDF")
                val atomicHistLoadDF = atomicHistLoadWrapper(atomicHistLoadSrcDF, initializeAtomicHistLoad, initializeCommonPipeLineHelper)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, atomicHistLoadDF, "atomicHistLoad", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                outputDF

              case "fndLoad" => log.info("In fndLoad")
                val fndLoadSrcDF = pickDFFromListOfDFBasedOnOrderOfExecution(initializeExecutionOrder.executionOrder, initializeFndLoad.inputDFNameFromExecutionOrder, initializeFndLoad.inputDFNameFromExecutionOrder, listOfDF)._1
                log.info(s"fndLoadSrcDF = $fndLoadSrcDF")
                val fndLoadDF = fndLoadWrapper(fndLoadSrcDF, initializeFndLoad, initializeCommonPipeLineHelper)
                addDataframeToListAndHandleZeroRecordAndShowResult(listOfDF, fndLoadDF, "fndLoad", initializeAbortOnZeroRecords.abortOnZeroRecords, initializeCommonPipeLineHelper, initializeShowResultsToLog)

                fndLoadDF

              case "deleteTriggerFile" => log.info("In deleteTriggerFile")
                deleteTriggerFileWrapper(initializeDeleteTriggerFile)
                addDataframeToListAndShowResult(listOfDF, outputDF, "deleteTriggerFile", initializeCommonPipeLineHelper, initializeShowResultsToLog)
                outputDF

              case "updateCheckPointPath" => log.info("In updateCheckPointPath")
                val maxProcessedPartition: String = listedValidPathsForKafkaRead.max.split("=").last
                updateCheckPointPathWrapper(initializeReadInputKafka, maxProcessedPartition)
                addDataframeToListAndShowResult(listOfDF, outputDF, "updateCheckPointPath", initializeCommonPipeLineHelper, initializeShowResultsToLog)
                outputDF

              case "sendDFToEmail" => log.info("In sendDFToEmail")
                sendDFToEmailWrapper(outputDF,initializeSendDFToEmail,initializeCommonPipeLineHelper)
                listOfDF += outputDF
                outputDF

              case "saveDFToHdfs" => log.info("In saveDFToHdfs")
                saveDFToHdfsWrapper(outputDF,initializeSaveDFToHdfs)
                listOfDF += outputDF
                outputDF

              case "transferFromSftpToHdfs" => log.info("In transferFromSftpToHdfs")
                transferFromSftpToHdfsWrapper(initializeTransferFromSftpToHdfs)
                listOfDF += outputDF
                outputDF

              case "transferFromHdfsToSftp" => log.info("In transferFromHdfsToSftp")
                transferFromHdfsToSftpWrapper(initializeTransferFromHdfsToSftp)
                listOfDF += outputDF
                outputDF
              case _ => addDataframeToListAndShowResult(listOfDF, outputDF, "other", initializeCommonPipeLineHelper, initializeShowResultsToLog)
                outputDF
            }
          }
        showResultsToLog(foldOutputDF, initializeShowResultsToLog.showResultsToLog, "Showing output of last job in the executionOrder", initializeShowResultsToLog.numOfRows)
        if (initializeShowCustomResultsToLog.showCustomResultsToLog) {
          showCustomResultsToLog(initializeExecutionOrder.executionOrder, initializeShowCustomResultsToLog.dataFrameNameAndExecuteSql, listOfDF, initializeShowCustomResultsToLog.numOfRows)

        }
      }

      catch {
        case e: ZeroRecordInDataframeException => log.info("There are zero records in the Dataframe after reading,Skipping rest of flow")

      }

      listOfDF
    }

  }

}
