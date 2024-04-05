package com.tgt.dse.mdf.common.pipeline.constants

/**
 * Constants used by the application
 */
object CommonExecutePipelineApplicationConstants {

  //executionOrder
  val APP_EXECUTIONORDER="app.executionOrder.executionOrder"
  //showResultsToLog
  val APP_SHOWRESULTSTOLOG="app.showResultsToLog.showResultsToLog"
  val APP_SHOWRESULTSTOLOG_NUMOFROWS="app.showResultsToLog.numOfRows"

  //showCustomResultsToLog
  val APP_SHOWCUSTOMRESULTSTOLOG="app.showCustomResultsToLog.showCustomResultsToLog"
  val APP_SHOWCUSTOMRESULTSTOLOG_DATAFRAMENAMEANDEXECUTESQL="app.showCustomResultsToLog.dataFrameNameAndExecuteSql"
  val APP_SHOWCUSTOMRESULTSTOLOG_NUMOFROWS="app.showCustomResultsToLog.numOfRows"

  //abortOnZeroRecords
  val APP_ABORTONZERORECORDS="app.abortOnZeroRecords.abortOnZeroRecords"

  //readInput
  val APP_READINPUT_INPUTEPHEMERALFILESFORMAT = "app.readInput.inputEphemeralFilesFormat"
  val APP_READINPUT_TEMPLATELOCATION= "app.readInput.templateLocation"
  val APP_READINPUT_HASHEADER="app.readInput.hasHeader"
  val APP_READINPUT_RENAMECOLUMNSNAMES="app.readInput.renameColumns"
  val APP_READINPUT_SELECTCOLUMNS="app.readInput.selectColumns"
  val APP_READINPUT_FILTER="app.readInput.filter"
  val APP_READINPUT_DELIMITER="app.readInput.delimiter"
  val APP_READINPUT_OPTIONS="app.readInput.options"
  val APP_READINPUT_GETFILENAMES="app.readInput.getFileName"
  val APP_READINPUT_ABORTONZERORECORDS="app.readInput.abortOnZeroRecords"

  //readKafkaInput
  val APP_READINPUTKAFKA_INPUTEPHEMERALFILESFORMAT = "app.readInputKafka.inputEphemeralFilesFormat"
  val APP_READINPUTKAFKA_TEMPLATELOCATION= "app.readInputKafka.templateLocation"
  val APP_READINPUTKAFKA_CHECKPOINTPATH="app.readInputKafka.checkpointPath"
  val APP_READINPUTKAFKA_DROPCOLUMNS="app.readInputKafka.dropColumns"
  val APP_READINPUTKAFKA_EPHEMERALFILEPATHPREFIX="app.readInputKafka.ephemeralFilePathPrefix"
  val APP_READINPUTKAFKA_ADDKAFKATIMESTAMP="app.readInputKafka.addKafkaTimestamp"
  val APP_READINPUTKAFKA_NUMBEROFPARTITIONS="app.readInputKafka.getFileName"
  val APP_READINPUTKAFKA_ABORTONZERORECORDS="app.readInput.abortOnZeroRecords"
  val APP_READINPUTKAFKA_HANDLEEMPTYTOPIC="app.readInputKafka.handleEmptyTopic"
  val APP_READINPUTKAFKA_FNDTABLEPARTITIONCOLUMN="app.readInputKafka.fndTablePartitionColumn"

  //validateSchema
  val APP_VALIDATESCHEMA_CSVSCHEMASTRING="app.validateSchema.csvSchemaString"
  val APP_VALIDATESCHEMA_TEMPLATELOCATION="app.validateSchema.templateLocation"

  //flatten
  //val APP_FLATTEN = "app.flatten.flatten"

  //addFileDate
  val APP_ADDFILEDATE_SOURCEDATEFORMAT="app.addFileDate.sourceDateFormat"
  val APP_ADDFILEDATE_SOURCECOLUMN="app.addFileDate.sourceColumn"
  val APP_ADDFILEDATE_TARGETCOLUMN="app.addFileDate.targetColumn"

  //addPartitionColumn
  val APP_ADDDATEPARTITIONCOLUMN_ISPARTITIONCOLUMNDATATYPEDATE = "app.addDatePartitionColumn.isPartitionColumnDatatypeDate"
  val APP_ADDDATEPARTITIONCOLUMN_OUTPUTFORMAT = "app.addDatePartitionColumn.outputFormat"
  val APP_ADDDATEPARTITIONCOLUMN_SOURCECOLUMN = "app.addDatePartitionColumn.sourceColumn"
  val APP_ADDDATEPARTITIONCOLUMN_TARGETCOLUMN = "app.addDatePartitionColumn.targetColumn"

  //getLatestByKeyCol
  val APP_GETLATESTBYKEYCOL_KEYCOLNAME="app.getLatestByKeyCol.keyColName"
  val APP_GETLATESTBYKEYCOL_ORDERBYCOLNAME="app.getLatestByKeyCol.orderByColName"
  val APP_GETLATESTBYKEYCOL_WINDOWINGSTRATEGY="app.getLatestByKeyCol.windowingStrategy"

  //countByFileDateAndPartitionDate
  val APP_COUNTBYFILEDATEANDPARTITIONDATE_PARTITIONDATECOLUMN="app.countByFileDateAndPartitionDate.partitionDateColumn"
  val APP_COUNTBYFILEDATEANDPARTITIONDATE_FILEDATECOLUMN="app.countByFileDateAndPartitionDate.fileDateColumn"

  //executeExternalSql
  val APP_EXECUTEEXTERNALSQL_DATAFRAMENAMEUSEDINSQL = "app.executeExternalSql.dataFrameNameUsedInSql"
  val APP_EXECUTEEXTERNALSQL_INPUTSQL = "app.executeExternalSql.inputSql"

  //executeSqlAgainstTable
  val APP_EXECUTESQLAGAINSTTABLE_INPUTSQL = "app.executeSqlAgainstTable.inputSql"
  val APP_EXECUTESQLAGAINSTTABLE_ENV="app.executeSqlAgainstTable.env"
  val APP_EXECUTESQLAGAINSTTABLE_SELECTCOLUMNS="app.executeSqlAgainstTable.selectColumns"

  //executeSqlAgainstDataframe
  val APP_EXECUTESQLAGAINSTDATAFRAME_INPUTSQL = "app.executeSqlAgainstDataframe.inputSql"
  val APP_EXECUTESQLAGAINSTDATAFRAME_DATAFRAMENAMEUSEDINSQL = "app.executeSqlAgainstDataframe.dataFrameNameUsedInSql"
  val APP_EXECUTESQLAGAINSTDATAFRAME_DATAFRAMEORDEROFEXECUTION="app.executeSqlAgainstDataframe.dataFrameOrderOfExecution"

  //dropDups
  val APP_DROPDUPS_LOCATION = "app.dropDups.location"
  val APP_DROPDUPS_PARTITIONCOLUMNS = "app.dropDups.partitionColumns"
  val APP_DROPDUPS_ORDERBYCOLUMN = "app.dropDups.orderByColumn"

  //validateNotNull
  val APP_VALIDATENOTNULL_INPUTCOLUMNS="app.validateNotNull.inputColumns"

  //validateDataType
  val APP_VALIDATEDATATYPE_INPUTCOLUMNS="app.validateDataType.inputColumns"

  //validateDateFormat
  val APP_VALIDATEDATEFORMAT_INPUTCOLUMNS="app.validateDateFormat.inputColumns"
  val APP_VALIDATEDATEFORMAT_SOURCEDATEFORMAT="app.validateDateFormat.sourceDateFormat"

  //removeDollarCharacter
  val APP_REMOVEDOLLARCHARACTER_INPUTCOLUMNS="app.removeDollarCharacter.inputColumns"

  //removeNonUtfCharacter
  val APP_REMOVENONUTFCHARACTER_INPUTCOLUMNS="app.removeNonUtfCharacter.inputColumns"

  //replaceNullWithDefaultForQtyColumn
  val APP_REPLACENULLWITHDEFAULTFORQTYCOLUMN_INPUTCOLUMNS="app.replaceNullWithDefaultForQtyColumn.inputColumns"
  val APP_REPLACENULLWITHDEFAULTFORQTYCOLUMN_DEFAULT="app.replaceNullWithDefaultForQtyColumn.default"

  //replaceNullWithDefaultForAmtColumn
  val APP_REPLACENULLWITHDEFAULTFORAMTCOLUMN_INPUTCOLUMNS="app.replaceNullWithDefaultForAmtColumn.inputColumns"
  val APP_REPLACENULLWITHDEFAULTFORAMTCOLUMN_DEFAULT="app.replaceNullWithDefaultForAmtColumn.default"

  //replaceNullWithDefaultForStringColumn
  val APP_REPLACENULLWITHDEFAULTFORSTRINGCOLUMN_INPUTCOLUMNS="app.replaceNullWithDefaultForStringColumn.inputColumns"
  val APP_REPLACENULLWITHDEFAULTFORSTRINGCOLUMN_DEFAULT="app.replaceNullWithDefaultForStringColumn.default"

  //convert micro to currency
  val APP_CONVERTMICROTOCURRENCY_INPUTCOLUMNS="app.convertMicroToCurrency.inputColumns"

  //handleNullsInPKColumns
  val APP_HANDLENULLSINPKCOLUMNS_INPUTCOLUMNS="app.handleNullsInPKColumns.inputColumns"


  //convertDatePartOfTsToDefault
  val APP_CONVERTDATEPARTOFTSTODEFAULT_INPUTCOLUMNS="app.convertDatePartOfTsToDefault.inputColumns"
  val APP_CONVERTDATEPARTOFTSTODEFAULT_INPUTDATEFORMAT="app.convertDatePartOfTsToDefault.inputTsFormat"

  //convertDateFormat
  val APP_CONVERTDATEFORMAT_INPUTCOLUMNS="app.convertDateFormat.inputColumns"
  val APP_CONVERTDATEFORMAT_INPUTDATEFORMAT="app.convertDateFormat.inputDateFormat"
  val APP_CONVERTDATEFORMAT_OUTPUTDATEFORMAT="app.convertDateFormat.outputDateFormat"

  //convertTimestampToDate
  val APP_CONVERTTIMESTAMPTODATE_INPUTCOLUMNS="app.convertTimestampToDate.inputColumns"
  val APP_CONVERTTIMESTAMPTODATE_OUTPUTFORMAT="app.convertTimestampToDate.outputFormat"

  //convertTimestampFromLocalToUtc
  val APP_CONVERTTIMESTAMPFROMLOCALTOUTC_INPUTCOLUMNS="app.convertTimestampFromLocalToUtc.inputColumns"
  val APP_CONVERTTIMESTAMPFROMLOCALTOUTC_INPUTFORMAT="app.convertTimestampFromLocalToUtc.inputFormat"
  val APP_CONVERTTIMESTAMPFROMLOCALTOUTC_OUTPUTFORMAT="app.convertTimestampFromLocalToUtc.outputFormat"
  val APP_CONVERTTIMESTAMPFROMLOCALTOUTC_TIMEZONE="app.convertTimestampFromLocalToUtc.timezone"

  //convertTimestampFromUtcToLocal
  val APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_INPUTCOLUMNS="app.convertTimestampFromUtcToLocal.inputColumns"
  val APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_INPUTFORMAT="app.convertTimestampFromUtcToLocal.inputFormat"
  val APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_OUTPUTFORMAT="app.convertTimestampFromUtcToLocal.outputFormat"
  val APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_TIMEZONE="app.convertTimestampFromUtcToLocal.timezone"

  //joinCal
  val APP_JOINCAL_CALENDARCOLUMNS="app.joinCal.calendarColumns"
  val APP_JOINCAL_CALENDARCOLUMNSWITHANYTRANSFORMATION="app.joinCal.calendarColumnsWithAnyTransformation"
  val APP_JOINCAL_CALENDARTABLE="app.joinCal.calendarTable"
  val APP_JOINCAL_TEMPCALENDAREXTLOCATION="app.joinCal.tempCalendarExtLocation"
  val APP_JOINCAL_INPUTJOINCONDITIONCOLUMN="app.joinCal.inputJoinConditionColumn"
  val APP_JOINCAL_CALENDARJOINCONDITIONCOLUMN="app.joinCal.calendarJoinConditionColumn"

  //joinTwoDataFrames
  val APP_JOINTWODATAFRAMES_LEFTANDRIGHTDFNAME="app.joinTwoDataframes.joinTwoDataframesLeftAndRightDFName"
  val APP_JOINTWODATAFRAMES_JOINTYPE="app.joinTwoDataframes.joinType"
  val APP_JOINTWODATAFRAMES_JOINCONDITION="app.joinTwoDataframes.joinCondition"
  val APP_JOINTWODATAFRAMES_PREFIXRIGHTDFCOMMONCOL="app.joinTwoDataframes.prefixRightDFCommonCol"


  //rename
  val APP_RENAME_COLUMNS="app.rename.renameColumns"

  //dqCheck
  //val APP_DQCHECK="app.dqCheck.dqCheck"
  val APP_DQCHECK_SOURCEANDTARGETDFNAME="app.dqCheck.dqCheckSourceAndTargetDFName"
  val APP_DQCHECK_DQCOLUMNS="app.dqCheck.dqColumns"
  val APP_DQCHECK_DUPSCHECKKEY="app.dqCheck.dupsCheckKey"
  val APP_DQCHECK_NULLSCHECKKEY="app.dqCheck.nullsCheckKey"
  val APP_DQCHECK_LOGGERNAME="app.dqCheck.loggerName"
  val APP_DQCHECK_REPORTINGDATECOLUMNFLAG="app.dqCheck.reportingDateColumnFlag"
  val APP_DQCHECK_SRCCOLUMNRENAMELIST="app.dqCheck.srcColumnRenameList"
  val APP_DQCHECK_TGTCOLUMNRENAMELIST="app.dqCheck.tgtColumnRenameList"
  val APP_DQCHECK_SCALE="app.dqCheck.scale"
  val APP_DQCHECK_THROWEXCEPTIONUPONFAILURE="app.dqCheck.throwExceptionUponFailure"
  val APP_DQCHECK_NULLCHECKFLAG="app.dqCheck.nullCheckFlag"
  val APP_DQCHECK_DUPCHECKFLAG="app.dqCheck.dupCheckFlag"
  val APP_DQCHECK_PARTITIONDATECOLNAME="app.dqCheck.partitionDateColName"
  val APP_DQCHECK_TARGETTABLENAME="app.dqCheck.targetTablename"
  val APP_DQCHECK_THRESHOLD="app.dqCheck.threshold"
  val APP_DQCHECK_THRESHOLDPERCENT="app.dqCheck.thresholdPercent"
  val APP_DQCHECK_THRESHOLDPERCENTJSON="app.dqCheck.thresholdPercentJson"
  val APP_DQCHECK_IGNORECOUNTCHECK="app.dqCheck.ignoreCountCheck"
  val APP_DQCHECK_DQCHECKRESULTSHOWTOLOG="app.dqCheck.dqCheckResultShowToLog"

  //archiveArchivePartition
  val APP_ARCHIVEARCHIVEPARTITION_DELETESOURCE="app.archiveArchivePartition.deleteSource"

  //archiveEphemeral
  val APP_ARCHIVEEPHEMERAL_DELETESOURCE="app.archiveEphemeral.deleteSource"

  //atomicHistLoad
  val APP_ATOMICHISTLOAD_INPUTDFNAMEFROMEXECUTIONORDER ="app.atomicHistLoad.inputDFNameFromExecutionOrder"
  val APP_ATOMICHISTLOAD_INSERTCOLUMNS="app.atomicHistLoad.insertColumns"
  val APP_ATOMICHISTLOAD_PARTITIONCOLUMNS="app.atomicHistLoad.partitionColumns"
  val APP_ATOMICHISTLOAD_REPARTITION="app.atomicHistLoad.repartition"

  //fnd_load
  val APP_FNDLOAD_INPUTDFNAMEFROMEXECUTIONORDER ="app.fndLoad.inputDFNameFromExecutionOrder"
  val APP_FNDLOAD_INSERTCOLUMNS="app.fndLoad.insertColumns"
  val APP_FNDLOAD_PARTITIONCOLUMNS="app.fndLoad.partitionColumns"
  val APP_FNDLOAD_REPARTITION="app.fndLoad.repartition"

  //deleteTriggerFileDoneFile
  val APP_DELETETRIGGERFILE_DONEFILE ="app.deleteTriggerFile.doneFile"

  //sendDFToEmail
  val APP_SENDDFTOEMAIL_CUSTOMSQLFLAG ="app.sendDFToEmail.customSqlFlag"
  val APP_SENDDFTOEMAIL_CUSTOMSQL ="app.sendDFToEmail.customSql"
  val APP_SENDDFTOEMAIL_FROMADDRESS ="app.sendDFToEmail.fromAddress"
  val APP_SENDDFTOEMAIL_TOADDRESS ="app.sendDFToEmail.toAddress"
  val APP_SENDDFTOEMAIL_REPORTFILENAME ="app.sendDFToEmail.reportFilename"
  val APP_SENDDFTOEMAIL_EMAILSUBJECT ="app.sendDFToEmail.emailSubject"
  val APP_SENDDFTOEMAIL_EMAILBODYSUBJECT ="app.sendDFToEmail.emailBodySubject"
  val APP_SENDDFTOEMAIL_ATTACHDFRESULT ="app.sendDFToEmail.attachDFResult"
  val APP_SENDDFTOEMAIL_ATTACHDFRESULTTOEMAILBODY ="app.sendDFToEmail.attachDFResultToEmailBody"
  val APP_SENDDFTOEMAIL_DEBUG ="app.sendDFToEmail.debug"

  //transferFromSftpToHdfs
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPPASSWORDJCEKEYSTOREPATH ="app.transferFromSftpToHdfs.sftpPasswordJceKeyStorePath"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPPASSWORDJCEKEYNAME ="app.transferFromSftpToHdfs.sftpPasswordJceKeyName"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH ="app.transferFromSftpToHdfs.sftpPrivateKeyPath"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPUSER ="app.transferFromSftpToHdfs.sftpUser"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPHOST ="app.transferFromSftpToHdfs.sftpHost"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPPORT ="app.transferFromSftpToHdfs.sftpPort"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPFILELOCATIONPATH ="app.transferFromSftpToHdfs.sftpFileLocationPath"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPFILERENAMEFILENAME ="app.transferFromSftpToHdfs.sftpFileRenameFileName"
  val APP_TRANSFERFROMSFTPTOHDFS_SFTPSEARCHFILEPATTERN ="app.transferFromSftpToHdfs.sftpSearchFilePattern"
  val APP_TRANSFERFROMSFTPTOHDFS_COPYMECHANISMTYPE ="app.transferFromSftpToHdfs.copyMechanismType"
  val APP_TRANSFERFROMSFTPTOHDFS_HDFSFILELOCATIONPATH ="app.transferFromSftpToHdfs.hdfsFileLocationPath"
  val APP_TRANSFERFROMSFTPTOHDFS_FILECOMPLETENESSCHECK="app.transferFromSftpToHdfs.fileCompletenessCheck"
  val APP_TRANSFERFROMSFTPTOHDFS_RETRIESFORFILECOMPLETENESSCHECK="app.transferFromSftpToHdfs.retriesForFileCompletenessCheck"
  val APP_TRANSFERFROMSFTPTOHDFS_SLEEPFORFILECOMPLETENESSCHECK="app.transferFromSftpToHdfs.sleepForFileCompletenessCheck"
  val APP_TRANSFERFROMSFTPTOHDFS_ISFILEALREADYSFTPCHECK="app.transferFromSftpToHdfs.isFileAlreadySftpCheck"
  val APP_TRANSFERFROMSFTPTOHDFS_HDFSFILELOCATIONTOCHECKISFILEALREADYSFTPCHECK="app.transferFromSftpToHdfs.hdfsFileLocationToCheckIsFileAlreadySftpCheck"


  //transferFromHdfsToSftp
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPPASSWORDJCEKEYSTOREPATH ="app.transferFromHdfsToSftp.sftpPasswordJceKeyStorePath"
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPPASSWORDJCEKEYNAME ="app.transferFromHdfsToSftp.sftpPasswordJceKeyName"
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH ="app.transferFromHdfsToSftp.sftpPrivateKeyPath"
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPUSER ="app.transferFromHdfsToSftp.sftpUser"
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPHOST ="app.transferFromHdfsToSftp.sftpHost"
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPPORT ="app.transferFromHdfsToSftp.sftpPort"
  val APP_TRANSFERFROMHDFSTOSFTP_HDFSFILELOCATIONPATH ="app.transferFromHdfsToSftp.hdfsFileLocationPath"
  val APP_TRANSFERFROMHDFSTOSFTP_HDFSFILERENAMEFLAG ="app.transferFromHdfsToSftp.hdfsFileRenameFlag"
  val APP_TRANSFERFROMHDFSTOSFTP_HDFSFILERENAMEFILENAME ="app.transferFromHdfsToSftp.hdfsFileRenameFileName"
  val APP_TRANSFERFROMHDFSTOSFTP_HDFSSEARCHFILEPATTERN ="app.transferFromHdfsToSftp.hdfsSearchFilePattern"
  val APP_TRANSFERFROMHDFSTOSFTP_COPYMECHANISMTYPE ="app.transferFromHdfsToSftp.copyMechanismType"
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPSLEEP ="app.transferFromHdfsToSftp.sftpSleep"
  val APP_TRANSFERFROMHDFSTOSFTP_SFTPFILELOCATIONPATH ="app.transferFromHdfsToSftp.sftpFileLocationPath"


  //saveDFToHdfs
  val APP_SAVEDFTOHDFS_SELECTCOLUMNS ="app.saveDFToHdfs.selectColumns"
  val APP_SAVEDFTOHDFS_FILTERCONDITION ="app.saveDFToHdfs.filterCondition"
  val APP_SAVEDFTOHDFS_HDFSTEMPFILELOCATION ="app.saveDFToHdfs.hdfsTempFileLocation"
  val APP_SAVEDFTOHDFS_HDFSFILELOCATION ="app.saveDFToHdfs.hdfsFileLocation"
  val APP_SAVEDFTOHDFS_HDFSSAVEOPTIONS ="app.saveDFToHdfs.hdfsSaveOptions"
  val APP_SAVEDFTOHDFS_HDFSSAVEMODE ="app.saveDFToHdfs.hdfsSaveMode"
  val APP_SAVEDFTOHDFS_HDFSSAVEFILEFORMAT ="app.saveDFToHdfs.hdfsSaveFileFormat"
  val APP_SAVEDFTOHDFS_HDFSSAVECOALESCE ="app.saveDFToHdfs.hdfsSaveCoalesce"
  val APP_SAVEDFTOHDFS_COALESCEINPUTSPLIT ="app.saveDFToHdfs.coalesceInputSplit"
  val APP_SAVEDFTOHDFS_HDFSRENAMEFILENAME ="app.saveDFToHdfs.hdfsRenameFilename"
  val APP_SAVEDFTOHDFS_HDFSRENAMEFILESEARCHPATTERN ="app.saveDFToHdfs.hdfsRenameFileSearchPattern"
  val APP_SAVEDFTOHDFS_DELETEHDFSFILESEARCHPATTERN ="app.saveDFToHdfs.deleteHdfsFileSearchPattern"
  val APP_SAVEDFTOHDFS_DELETEHDFSFILERECURSIVE ="app.saveDFToHdfs.deleteHdfsFileRecursive"

}


