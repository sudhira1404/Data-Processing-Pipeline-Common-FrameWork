package com.tgt.dse.mdf.common.pipeline.vo

import com.tgt.dsc.kelsa.datapipeline.core.config.PipeLineConfig
import com.tgt.dse.mdf.common.pipeline.constants.{CommonExecutePipelineApplicationConstants, MarketingConstants}
import com.tgt.dse.mdf.common.pipeline.constants.MarketingConstants.DATEFORMATTER
import com.tgt.dse.mdf.common.pipeline.util.CommonExecutePipelineUtils.{convertStringToArray, getBooleanValueFromPipeLineConfig,
  getDoubleValueFromPipeLineConfig, getIntValueFromPipeLineConfig, getLongValueFromPipeLineConfig, getStringValueFromPipeLineConfig, getValueFromPipeLineConfigAndConvertToListOfStringTuple, getValueFromPipeLineConfigAndConvertToMap, getValueFromPipeLineConfigAndConvertToSeq}
import com.tgt.dse.mdf.common.pipeline.util.{DateTimeUtils, MarketingCommonUtils}

import java.time.LocalDateTime

object CommonExecutePipelineParameterObjects {

  /**
   * CommonPipeLineHelper Case Class.All are mandatory members in config file
   * Members
   * env                   From MarketingConstants
   * jdbcURL               From MarketingConstants
   * queueName             From MarketingConstants
   * fileLandingPath       From MarketingConstants
   * inputEphemeralPath    From MarketingConstants
   * archivePath           From MarketingConstants
   * archivePathPartition  From MarketingConstants
   * hiveLandingSchema     From MarketingConstants
   * lndCoreHistTable      From MarketingConstants
   * pipelineFailures      From MarketingConstants
   * nullOrEmptyRecords    From MarketingConstants
   * dataTypeFailures      From MarketingConstants
   * lockLocation          From MarketingConstants
   * corruptRecordLocation From MarketingConstants
   * fndDb                 From MarketingConstants
   * fndTable              From MarketingConstants
   * fndExtLocation        From MarketingConstants

   * @return CommonPipeLineHelper type
   */
  case class CommonPipeLineHelper(pipeLineConfig: PipeLineConfig) {
    val JOB_START_TIME: String = LocalDateTime.now().format(DATEFORMATTER)
    val config = pipeLineConfig.config
    val env: String = config.getString(MarketingConstants.APP_ENV)
    val jdbcURL: String = config.getString(MarketingConstants.APP_JDBCURL)
    val queueName: String = config.getString(MarketingConstants.APP_QUEUENAME)
    val fileLandingPath: String = MarketingCommonUtils.appendSlashInEnd(config.getString(MarketingConstants.APP_FILEAPILANDINGLOCATION))
    val inputEphemeralPath: String = MarketingCommonUtils.appendSlashInEnd(config.getString(MarketingConstants.APP_INPUTLOCATION))
    val archivePath: String = MarketingCommonUtils.appendSlashInEnd(config.getString(MarketingConstants.APP_EXISTINGRECORDSLOCATION))
    val archivePathPartition: String = config.getString(MarketingConstants.APP_ARCHIVEPARTITIONRECORDSLOCATION)
    val hiveLandingSchema: String = config.getString(MarketingConstants.APP_LNDSCHEMA)
    val lndCoreHistTable: String = config.getString(MarketingConstants.APP_lndCoreHistTable)
    val pipelineFailures: String = config.getString(MarketingConstants.APP_PIPELINEFAILURES)
    val nullOrEmptyRecords: String = MarketingConstants.NULLOREMPTYRECORDS
    val dataTypeFailures: String = MarketingConstants.DATATYPEFAILURES
    val lockLocation: String = MarketingCommonUtils.appendSlashInEnd(config.getString(MarketingConstants.APP_LOCKLOCATION))
    val corruptRecordLocation: String =
      DateTimeUtils.appendPathWithTimeStamp(MarketingCommonUtils.appendSlashInEnd(
        pipeLineConfig.config.getString(MarketingConstants.APP_PIPELINEFAILURES)) + MarketingConstants.CORRUPTRECORDS,
        JOB_START_TIME)
    val fndDb: String = pipeLineConfig.config.getString(MarketingConstants.APP_FNDSCHEMA)
    val fndTable: String = pipeLineConfig.config.getString(MarketingConstants.APP_coreHistTable)
    val fndExtLocation: String = MarketingCommonUtils.appendSlashInEnd(config.getString(MarketingConstants.APP_FNDEXTLOCATION))

  }

  /**
   * ExecutionOrder Case Class
   * Members
   * executionOrder                   String Type.Mandatory to trigger common template.Maps to app.executionOrder.executionOrder in config file

   * @return ExecutionOrder type
   */
  case class ExecutionOrder(pipeLineConfig: PipeLineConfig) {

    val executionOrder: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTIONORDER)

  }

  /**
   * ShowResultsToLog Case Class
   * Members
   * showResultsToLog                   Boolean Type.Defaulted to false.Maps to app.showResultsToLog.showResultsToLog in config file
   * numOfRows                          Int Type.Defaulted to 0.Maps to app.showResultsToLog.numOfRows in config file

   * @return ShowResultsToLog type
   */

  case class ShowResultsToLog(pipeLineConfig: PipeLineConfig) {

    val showResultsToLog: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SHOWRESULTSTOLOG)
    val numOfRows: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SHOWRESULTSTOLOG_NUMOFROWS)

  }

  /**
   * ShowCustomResultsToLog Case Class
   * Members
   * showCustomResultsToLog                   Boolean Type.Defaulted to false.Maps to app.showCustomResultsToLog.showCustomResultsToLog in config file
   * dataFrameNameAndExecuteSql               Seq[(String, String)] Type.Maps to app.showCustomResultsToLog.dataFrameNameAndExecuteSql in config file.provide like """{ \"readInput\":\"select * from readInput\"   ,
                                              \"rename\":\"select * from rename\"
                                        }"""
   * numOfRows                                Int Type.Maps to app.showCustomResultsToLog.numOfRows in config file

   * @return ShowResultsToLog type
   */
  case class ShowCustomResultsToLog(pipeLineConfig: PipeLineConfig) {

    val showCustomResultsToLog: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SHOWCUSTOMRESULTSTOLOG)
    val dataFrameNameAndExecuteSql: Seq[(String, String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SHOWCUSTOMRESULTSTOLOG_DATAFRAMENAMEANDEXECUTESQL)
    val numOfRows: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SHOWCUSTOMRESULTSTOLOG_NUMOFROWS)

  }

  /**
   * AbortOnZeroRecords Case Class
   * Members
   * abortOnZeroRecords                   Boolean Type.Defaulted to false.Maps to app.abortOnZeroRecords.abortOnZeroRecords in config file.If set,will  throw generalException if input dataframe has count =0
   * else not set by default and if input dataframe has count =0 flow will skip remaining execution

   * @return ShowResultsToLog type
   */
  case class AbortOnZeroRecords(pipeLineConfig: PipeLineConfig) {

    val abortOnZeroRecords: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ABORTONZERORECORDS)

  }

  /**
   * ReadInput Case Class.Refer FileReader.readFileToDataFrame for parameter definition
   * Members
   * inputEphemeralFilesFormat      String Type.Need to provide.Maps to app.readInput.inputEphemeralFilesFormat in config file.Provide single string like "csv" or "json"
   * templateLocation               String Type.Defaulted to empty string.Maps to app.readInput.templateLocation in config file.Provide if inputEphemeralFilesFormat="json" in config  to infer schema
   * hasHeader                      Boolean Type.Defaulted to false.Maps to app.readInput.hasHeader in config file
   * renameColumns                  Seq[String] Type.Defaulted to empty.Maps to app.readInput.renameColumns in config file.Provide as comma separated string like "abc,qwer,asd" in config "
   * selectColumns                  Seq[String] Type.Defaulted to empty.Maps to app.readInput.selectColumns in config file.Provide as comma separated string like "abc,qwer,asd" in config  "
   * filter                         String Type.Defaulted to empty.Defaulted to empty string.Maps to app.readInput.filter in config file
   * delimiter                      String Type.Defaulted to empty.Maps to app.readInput.delimiter in config file
   * options                        Map[String, String] Type.Defaulted to empty.Provide like "header->true,quote->\",escape->\",MultiLine->false,ignoreTrailingWhiteSpace-> true" in config file
   * getFileName                    Boolean Type.Defaulted to false.
   * abortOnZeroRecords             Boolean Type.Defaulted to false.Maps to app.readInput.abortOnZeroRecords in config file.Will abort if input dataframe count is zero

   * @return ReadInput type
   */

  case class ReadInput(pipeLineConfig: PipeLineConfig) {

    val inputEphemeralFilesFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_INPUTEPHEMERALFILESFORMAT)
    val templateLocation: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_TEMPLATELOCATION)
    val hasHeader: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_HASHEADER)
    val renameColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_RENAMECOLUMNSNAMES)
    val selectColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_SELECTCOLUMNS)
    val filter: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_FILTER)
    val delimiter: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_DELIMITER)
    val options: Map[String, String] = getValueFromPipeLineConfigAndConvertToMap(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_OPTIONS)
    val getFileName: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_GETFILENAMES)
    val abortOnZeroRecords: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_ABORTONZERORECORDS)

  }

  /**
   * ReadInputKafka Case Class.Refer FileReader.ReadKafkaFiles for parameter definition
   * Members
   * inputEphemeralFilesFormat      String Type.Defaulted to Json.Maps to app.readInputKafka.inputEphemeralFilesFormat in config file
   * templateLocation               String Type.Defaulted to empty.Maps to app.readInputKafka.templateLocation in config file
   * checkpointPath                 String Type.Need to provide.Maps to app.readInputKafka.checkpointPath in config file
   * dropColumns                    Seq[String] Type.Defaulted to empty.Maps to app.readInputKafka.dropColumns in config file
   * ephemeralFilePathPrefix        String Type.Defaulted to "load_date=" load_date=.Maps to app.readInputKafka.ephemeralFilePathPrefix in config file
   * addKafkaTimestamp              Boolean Type.Defaulted to false.Maps to app.readInputKafka.addKafkaTimestamp in config file.Will add kafkaDateFieldName=report_d and kafkaTimestampFieldName=hive_load_timestamp as columns
   * numberOfPartition              Int Type.Need to provide.Maps to app.readInputKafka.numberOfPartition in config file
   * abortOnZeroRecords             Boolean Type.Defaulted to false.Maps to app.readInputKafka.abortOnZeroRecords in config file.If set will abort the job if input dataframe count is zero
   * handleEmptyTopic               Boolean Type.Defaulted to false.Maps to app.readInputKafka.handleEmptyTopic in config file.If set will get previous day's records from the table based on partition column if input dataframe count is zero
   * fndTablePartitionColumn        String Type.Defaulted to empty.Maps to app.readInputKafka.fndTablePartitionColumn in config file.Used when handleEmptyTopic is true to get previous day's records from the table based on partition column

   * @return ReadInputKafka type
   */

  case class ReadInputKafka(pipeLineConfig: PipeLineConfig) {

    val inputEphemeralFilesFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_INPUTEPHEMERALFILESFORMAT,"json")
    val templateLocation: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_TEMPLATELOCATION)
    val checkpointPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_CHECKPOINTPATH)
    val dropColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_DROPCOLUMNS)
    val ephemeralFilePathPrefix: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_EPHEMERALFILEPATHPREFIX,"load_date=")
    val addKafkaTimestamp: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_ADDKAFKATIMESTAMP)
    val numberOfPartitions: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_NUMBEROFPARTITIONS)
    val abortOnZeroRecords: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_ABORTONZERORECORDS)
    val handleEmptyTopic: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_HANDLEEMPTYTOPIC)
    val fndTablePartitionColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUTKAFKA_FNDTABLEPARTITIONCOLUMN)
  }


  /**
   * ValidateSchema Case Class
   * Members
   * inputEphemeralFilesFormat        String Type.Need to provide.Maps to app.readInput.inputEphemeralFilesFormat in config file.
   * csvSchemaString                  String Type.Defaulted to empty string.Maps to app.validateSchema.csvSchemaString in config file.If csv provide schema for csv like "Source string, CompanyName string"
   * templateLocation                 String Type.Defaulted to empty string.Maps to app.validateSchema.templateLocation in config file.If json provide the template path
   *
   * @return ValidateSchema type
   */

  case class ValidateSchema(pipeLineConfig: PipeLineConfig) {

    val inputEphemeralFilesFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_READINPUT_INPUTEPHEMERALFILESFORMAT)
    val csvSchemaString: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_VALIDATESCHEMA_CSVSCHEMASTRING)
    val templateLocation: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_VALIDATESCHEMA_TEMPLATELOCATION)

  }

  /**
   * AddFileDate Case Class
   * Members
   * sourceDateFormat              String Type.Need to provide.Maps to app.addFileDate.sourceFormat in config file.Source column date format
   * sourceColumn                  String Type.Need to provide.Maps to app.addFileDate.sourceColumn in config file.Source date format column like filename which is available when calling ReadInput with getFilename set and this maps to targetColumn
   * targetColumn                  String Type.Need to provide.Maps to app.addFileDate.targetColumn in config file.Target columns like report_d
   *
   * @return AddFileDate type
   */

  case class AddFileDate(pipeLineConfig: PipeLineConfig) {

    val sourceDateFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ADDFILEDATE_SOURCEDATEFORMAT)
    val sourceColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ADDFILEDATE_SOURCECOLUMN)
    val targetColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ADDFILEDATE_TARGETCOLUMN)

  }

  /**
   * AddDatePartitionColumn Case Class
   * Members
   * isPartitionColumnDatatypeDate Boolean Type.Need to provide.Maps to app.AddDatePartitionColumn.isPartitionColumnDatatypeDate in config file.If partition column is date or string type
   * sourceColumn                  String Type.Need to provide.Defaulted to empty string.Maps to app.AddDatePartitionColumn.sourceColumn in config file.Source column which needs to map to target column
   * targetColumn                  String Type.Need to provide.Defaulted to empty string.Maps to app.AddDatePartitionColumn.targetColumn in config file.Target partition column name
   *
   * @return AddDatePartitionColumn type
   */
  case class AddDatePartitionColumn(pipeLineConfig: PipeLineConfig) {

    val isPartitionColumnDatatypeDate: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ADDDATEPARTITIONCOLUMN_ISPARTITIONCOLUMNDATATYPEDATE)
    val sourceColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ADDDATEPARTITIONCOLUMN_SOURCECOLUMN)
    val targetColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ADDDATEPARTITIONCOLUMN_TARGETCOLUMN)

  }

  /**
   * GetLatestByKeyCol Case Class.(Refer DataFrameUtils.getLatestByPartitionDateArray for parameter definition)
   * Members
   * keyColName                           Seq[String] Type.Need to provide.Maps to app.getLatestByKeyCol.keyColName in config file.Provide like "report_d,anyothercolumnname"
   * orderByColName                       String Type.Need to provide.Maps to app.getLatestByKeyCol.orderByColName in config file.Provide single string like "file_date"
   * windowingStrategy                    String Type.Need to provide.Maps to app.getLatestByKeyCol.windowingStrategy in config file.Provide single string like "row_number" or dense_rank or rank

   * @return GetLatestByKeyCol type
   */
  case class GetLatestByKeyCol(pipeLineConfig: PipeLineConfig) {
    val keyColName: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_GETLATESTBYKEYCOL_KEYCOLNAME)
    val orderByColName: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_GETLATESTBYKEYCOL_ORDERBYCOLNAME)
    val windowingStrategy: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_GETLATESTBYKEYCOL_WINDOWINGSTRATEGY)

  }

  /**
   * CountByFileDateAndPartitionDate Case Class(Refer DataFrameUtils.CountByFileDateAndPartitionDate for parameter definition)
   * Members
   * partitionDateColumn                           String Type.Need to provide.Maps to app.countByFileDateAndPartitionDate.partitionDateColumn in config file
   * fileDateColumn                                String Type.Need to provide.Maps to app.countByFileDateAndPartitionDate.fileDateColumn in config file
   *
   * @return CountByFileDateAndPartitionDate type
   */

  case class CountByFileDateAndPartitionDate(pipeLineConfig: PipeLineConfig) {
    val partitionDateColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_COUNTBYFILEDATEANDPARTITIONDATE_PARTITIONDATECOLUMN)
    val fileDateColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_COUNTBYFILEDATEANDPARTITIONDATE_FILEDATECOLUMN)

  }

  /**
   * ExecuteExternalSql Case Class
   * Members
   * dataFrameNameUsedInSql                  Seq[String] Type.Need to provide.Maps to app.executeExternalSql.dataFrameNameUsedInSql in config file.Provide as comma separated strings in config file
   * inputSql                                Seq[(String,String)] Type.Need to provide.Maps to app.executeExternalSql.inputSql in config file."""{ \"1\":\"select * from readInput\"   ,
                                         \"1\":\"select * from rename\"
                                        }"""
   *
   * @return ExecuteExternalSql type
   */

  case class ExecuteExternalSql(pipeLineConfig: PipeLineConfig) {

    val dataFrameNameUsedInSql: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTEEXTERNALSQL_DATAFRAMENAMEUSEDINSQL)
    val inputSql: Seq[(String,String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTEEXTERNALSQL_INPUTSQL)
  }

  /**
   * ExecuteSqlAgainstTable Case Class
   * Members
   * inputSql                            Seq[(String,String)] Type.Need to provide.Maps to app.executeSqlAgainstTable.inputSql in config file."""{ \"1\":\"select * from prd_mdf_fnd.readInput\"   ,
                                         \"2\":\"select * from prd_mdf_fnd.readInput\"
                                        }"""
   * env                                String Type.Need to provide.Maps to app.executeSqlAgainstTable.env in config file like local/stage/prod
   * selectColumns                      String Type.Need to provide.Maps to app.executeSqlAgainstTable.selectColumns in config file

   *
   * @return ExecuteSqlAgainstTable type
   */

  case class ExecuteSqlAgainstTable(pipeLineConfig: PipeLineConfig) {

    val inputSql: Seq[(String,String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTESQLAGAINSTTABLE_INPUTSQL)
    val env: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTESQLAGAINSTTABLE_ENV)
    val selectColumns: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTESQLAGAINSTTABLE_SELECTCOLUMNS)

  }


  /**
   * ExecuteSqlAgainstDataframe Case Class
   * Members
   * inputSql                            Seq[(String,String)] Type.Need to provide.Maps to app.executeSqlAgainstDataframe.inputSql in config file."""{ \"1\":\"select * from readInput\"   ,
                                         \"2\":\"select * from transform\"
                                        }"""
   * dataFrameNameUsedInSql             Seq[String] Type.Need to provide.Maps to app.executeSqlAgainstDataframe.dataFrameNameUsedInSql in config file.Provide like "readInput,transform"  as mentioned in inputSql as comma separated strings in config file
   * dataFrameOrderOfExecution          String Type.Maps to app.executeSqlAgainstDataframe.dataFrameOrderOfExecution in config file.Provide like "readInput,transform" as mentioned in inputSql

   * @return ExecuteSqlAgainstDataframe type
   */

  case class ExecuteSqlAgainstDataframe(pipeLineConfig: PipeLineConfig) {

    val inputSql: Seq[(String,String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTESQLAGAINSTDATAFRAME_INPUTSQL)
    val dataFrameNameUsedInSql: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTESQLAGAINSTDATAFRAME_DATAFRAMENAMEUSEDINSQL)
    val dataFrameOrderOfExecution: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_EXECUTESQLAGAINSTDATAFRAME_DATAFRAMEORDEROFEXECUTION)

  }



  /**
   * DropDups Case Class(refer CommonExecutePipelineUtils.dropDup for parameter definition or readme file)
   * Members
   * location                  String Type.Need to provide.Maps to app.dropDups.location in config file
   * partitionColumns          Seq[String] Type.Need to provide.Maps to app.dropDups.partitionColumns in config file.Provide as comma separated strings in config file
   * orderByColumn             String Type.Need to provide.Maps to app.dropDups.orderByColumn in config file
   *
   * @return DropDups type
   */

  case class DropDups(pipeLineConfig: PipeLineConfig) {

    val location: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DROPDUPS_LOCATION)
    val partitionColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DROPDUPS_PARTITIONCOLUMNS)
    val orderByColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DROPDUPS_ORDERBYCOLUMN)
  }

  /**
   * ValidateNotNull Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.validateNotNull.inputColumns in config file.Provide as comma separated strings in config file
   *
   * @return ValidateNotNull type
   */

  case class ValidateNotNull(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_VALIDATENOTNULL_INPUTCOLUMNS)
  }

  /**
   * ValidateDataType Case Class
   * Members
   * inputColumns          Seq[(String, String)] Type.Need to provide.Maps to app.validateDataType.inputColumns in config file.Provide like "{\"CampaignBudget\":\"decimal(38,2)\", \"Revenue\":\"decimal(38,8)\"}" in config file
   *
   * @return ValidateDataType type
   */

  case class ValidateDataType(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[(String, String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_VALIDATEDATATYPE_INPUTCOLUMNS)
  }

  /**
   * ValidateDateFormat Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.validateDateFormat.inputColumns in config file.Provide as comma separated strings in config file
   * sourceDateFormat      String Type.Need to provide.Maps to app.validateDateFormat.sourceDateFormat in config file
   *
   * @return ValidateDateFormat type
   */

  case class ValidateDateFormat(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_VALIDATEDATEFORMAT_INPUTCOLUMNS)
    val sourceDateFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_VALIDATEDATEFORMAT_SOURCEDATEFORMAT)
  }


  /**
   * RemoveDollarCharacter Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.removeDollarCharacter.inputColumns in config file.Provide as comma separated strings in config file
   *
   * @return RemoveDollarCharacter type
   */

  case class RemoveDollarCharacter(pipeLineConfig: PipeLineConfig) {
    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REMOVEDOLLARCHARACTER_INPUTCOLUMNS)
  }

  /**
   * RemoveNonUtfCharacter Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.removeNonUtfCharacter.inputColumns in config file.Provide as comma separated strings in config file
   *
   * @return RemoveNonUtfCharacter type
   */

  case class RemoveNonUtfCharacter(pipeLineConfig: PipeLineConfig) {
    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REMOVENONUTFCHARACTER_INPUTCOLUMNS)
  }

  /**
   * ReplaceNullWithDefaultForQtyColumn Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.replaceNullWithDefaultForQtyColumn.inputColumns in config file.Provide as comma separated strings in config file
   * default               String Type.Need to provide.Maps to app.replaceNullWithDefaultForQtyColumn.default in config file
   *
   * @return ReplaceNullWithDefaultForQtyColumn type
   */

  case class ReplaceNullWithDefaultForQtyColumn(pipeLineConfig: PipeLineConfig) {
    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REPLACENULLWITHDEFAULTFORQTYCOLUMN_INPUTCOLUMNS)
    val default: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REPLACENULLWITHDEFAULTFORQTYCOLUMN_DEFAULT)

  }

  /**
   * ReplaceNullWithDefaultForAmtColumn Case Class
   * Members
   * inputColumns          Seq[String] Type Type.Maps to app.replaceNullWithDefaultForAmtColumn.inputColumns in config file.Provide as comma separated strings in config file
   * default               String Type.Need to provide.Maps to app.replaceNullWithDefaultForAmtColumn.default in config file
   *
   * @return ReplaceNullWithDefaultForAmtColumn type
   */

  case class ReplaceNullWithDefaultForAmtColumn(pipeLineConfig: PipeLineConfig) {
    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REPLACENULLWITHDEFAULTFORAMTCOLUMN_INPUTCOLUMNS)
    val default: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REPLACENULLWITHDEFAULTFORAMTCOLUMN_DEFAULT)

  }

  /**
   * ReplaceNullWithDefaultForStringColumn Case Class
   * Members
   * inputColumns          Seq[String] Type.Maps to app.replaceNullWithDefaultForStringColumn.inputColumns in config file.Provide as comma separated strings in config file
   * default               String Type.Need to provide.Maps to app.replaceNullWithDefaultForStringColumn.default in config file
   *
   * @return ReplaceNullWithDefaultForStringColumn type
   */

  case class ReplaceNullWithDefaultForStringColumn(pipeLineConfig: PipeLineConfig) {
    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REPLACENULLWITHDEFAULTFORSTRINGCOLUMN_INPUTCOLUMNS)
    val default: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_REPLACENULLWITHDEFAULTFORSTRINGCOLUMN_DEFAULT)

  }

  /**
   * ConvertMicroToCurrency Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.convertMicroToCurrency.inputColumns in config file.Provide as comma separated strings in config file
   *
   * @return ConvertMicroToCurrency type
   */

  case class ConvertMicroToCurrency(pipeLineConfig: PipeLineConfig) {
    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTMICROTOCURRENCY_INPUTCOLUMNS)

  }


  /**
   * HandleNullsInPKColumns Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide..Maps to app.handleNullsInPKColumns.inputColumns in config file.Provide Json like """{ \"CampaignId\":\"123\"   ,
                                            \"CampaignName\":\"NA\"   ,
                                            \"CampaignStartDate\":\"9999-12-31\"
                                            }"""
   *
   * @return HandleNullsInPKColumns type
   */
  //handleNullsInPKColumns
  case class HandleNullsInPKColumns(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[(String, String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_HANDLENULLSINPKCOLUMNS_INPUTCOLUMNS)
  }

  /**
   * ConvertDatePartOfTsToDefault Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.convertDatePartOfTsToDefault.inputColumns in config file.Provide as comma separated strings in config file
   * inputTsFormat         String Type.Need to provide.Maps to app.convertDatePartOfTsToDefault.inputTsFormat in config file
   *
   * @return HandleNullsInPKColumns type
   */
  //convertDatePartOfTsToDefault
  case class ConvertDatePartOfTsToDefault(pipeLineConfig: PipeLineConfig) {
    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTDATEPARTOFTSTODEFAULT_INPUTCOLUMNS)
    val inputTsFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTDATEPARTOFTSTODEFAULT_INPUTDATEFORMAT)

  }

  /**
   * ConvertDateFormat Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.convertDateFormat.inputColumns in config file.Provide as comma separated strings in config file
   * inputDateFormat       String Type.Need to provide.Maps to app.convertDateFormat.inputDateFormat in config file
   * outputDateFormat      String Type.Need to provide.Maps to app.convertDateFormat.outputDateFormat in config file
   *
   * @return ConvertDateFormat type
   */
  case class ConvertDateFormat(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTDATEFORMAT_INPUTCOLUMNS)
    val inputDateFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTDATEFORMAT_INPUTDATEFORMAT)
    val outputDateFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTDATEFORMAT_OUTPUTDATEFORMAT)

  }

  /**
   * ConvertTimestampToDate Case Class
   * Members
   * inputColumns                        Seq[String] Type.Need to provide.Maps to app.convertTimestampToDate.inputColumns in config file.Provide as comma separated strings in config file
   * convertTimestampToDateOutputFormat  String Type.Need to provide.Maps to app.convertTimestampToDate.outputFormat in config file
   *
   * @return ConvertTimestampToDate type
   */
  case class ConvertTimestampToDate(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPTODATE_INPUTCOLUMNS)
    val outputFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPTODATE_OUTPUTFORMAT)

  }

  /**
   * ConvertTimestampFromLocalToUtc Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.convertTimestampFromLocalToUtc.inputColumns in config file.Provide as comma separated strings in config file
   * inputFormat           String Type.Need to provide.Maps to app.convertTimestampFromLocalToUtc.inputFormat in config file
   * outputFormat          String Type.Need to provide.Maps to app.convertTimestampFromLocalToUtc.outputFormat in config file
   * timeZone              String Type.Need to provide.Maps to app.convertTimestampFromLocalToUtc.timeZone in config file
   *
   * @return ConvertTimestampFromLocalToUtc type
   */
  case class ConvertTimestampFromLocalToUtc(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMLOCALTOUTC_INPUTCOLUMNS)
    val inputFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMLOCALTOUTC_INPUTFORMAT)
    val outputFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMLOCALTOUTC_OUTPUTFORMAT)
    val timeZone: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMLOCALTOUTC_TIMEZONE)

  }

  /**
   * ConvertTimestampFromUtcToLocal Case Class
   * Members
   * inputColumns          Seq[String] Type.Need to provide.Maps to app.convertTimestampFromUtcToLocal.inputColumns in config file.Provide as comma separated strings in config file
   * inputFormat           String Type.Need to provide.Maps to app.convertTimestampFromUtcToLocal.inputFormat in config file
   * outputFormat          String Type.Need to provide.Maps to app.convertTimestampFromUtcToLocal.outputFormat in config file
   * timeZone              String Type.Need to provide.Maps to app.convertTimestampFromUtcToLocal.timeZone in config file
   *
   * @return ConvertTimestampFromUtcToLocal type
   */

  case class ConvertTimestampFromUtcToLocal(pipeLineConfig: PipeLineConfig) {

    val inputColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_INPUTCOLUMNS)
    val inputFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_INPUTFORMAT)
    val outputFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_OUTPUTFORMAT)
    val timeZone: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_CONVERTTIMESTAMPFROMUTCTOLOCAL_TIMEZONE)

  }

  /**
   * JoinCal Case Class
   * Members
   * calendarColumns                       String Type.Need to provide.Maps to app.joinCal.calendarColumns in config file
   * calendarColumnsWithAnyTransformation  String Type.Need to provide.Maps to app.joinCal.calendarColumnsWithAnyTransformation in config file
   * calendarTable                         String Type.Need to provide.Maps to app.joinCal.calendarTable in config file
   * tempCalendarExtLocation               String Type.Need to provide.Maps to app.joinCal.tempCalendarExtLocation in config file
   * inputJoinConditionColumn              String Type.Need to provide.Maps to app.joinCal.inputJoinConditionColumn in config file
   * calendarJoinConditionColumn           String Type.Need to provide.Maps to app.joinCal.calendarJoinConditionColumn in config file
   *
   * @return JoinCal type
   */
  case class JoinCal(pipeLineConfig: PipeLineConfig) {

    val calendarColumns: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINCAL_CALENDARCOLUMNS)
    val calendarColumnsWithAnyTransformation: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINCAL_CALENDARCOLUMNSWITHANYTRANSFORMATION)
    val calendarTable: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINCAL_CALENDARTABLE)
    val tempCalendarExtLocation: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINCAL_TEMPCALENDAREXTLOCATION)
    val inputJoinConditionColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINCAL_INPUTJOINCONDITIONCOLUMN)
    val calendarJoinConditionColumn: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINCAL_CALENDARJOINCONDITIONCOLUMN)

  }

  /**
   * JoinTwoDataFrames Case Class
   * Members
   * joinTwoDataframesLeftAndRightDFName           Array[String] Type.Need to provide.Maps to app.joinTwoDataFrames.joinTwoDataframesLeftAndRightDFName in config file
   * joinType                                      String Type.Need to provide.Maps to app.joinTwoDataFrames.joinType in config file
   * joinCondition                                 String Type.Need to provide.Maps to app.joinTwoDataFrames.joinCondition in config file
   * prefixRightDFCommonCol                        String Type.Need to provide.Maps to app.joinCondition.prefixRightDFCommonCol in config file
   * @return JoinTwoDataFrames type
   */
  case class JoinTwoDataFrames(pipeLineConfig: PipeLineConfig) {

    val joinTwoDataframesLeftAndRightDFName: Array[String] = convertStringToArray(getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINTWODATAFRAMES_LEFTANDRIGHTDFNAME))
    val joinType: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINTWODATAFRAMES_JOINTYPE)
    val joinCondition: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINTWODATAFRAMES_JOINCONDITION)
    val prefixRightDFCommonCol: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_JOINTWODATAFRAMES_PREFIXRIGHTDFCOMMONCOL)

  }

  /**
   * Rename Case Class
   * Members
   * renameColumns          Seq[String] Type.Need to provide.Maps to app.rename.renameColumns in config file.Provide Json like  """{ \"Source\":\"source_name\"   ,
                                       \"CompanyName\":\"company_name\"   ,
                                       \"AccountId\":\"account_id\"   ,
                                       \"CampaignName\":\"campaign_name\"   ,
                                       \"CampaignId\":\"campaign_id\"   ,
                                       \"CampaignBudget\":\"campaign_budget_a\"   ,
                                       \"CampaignStartDate\":\"campaign_start_date\"   ,
                                       \"CampaignEndDate\":\"campaign_end_date\"   ,
                                       \"ClassId\":\"class_id\"   ,
                                       \"DepartmentId\" :\"dept_id\"   ,
                                       \"VendorNumber\":\"vendor_number\"   ,
                                       \"TCIN\" :\"tcin\"   ,
                                       \"CalendarYear\":\"calendar_year\"   ,
                                       \"CalendarMonth\":\"calendar_month\"   ,
                                       \"Revenue\":\"revenue_a\"
                                       }""" in config file
   *
   * @return Rename type
   */

  case class Rename(pipeLineConfig: PipeLineConfig) {

    val renameColumns: Seq[(String, String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_RENAME_COLUMNS)
  }

  /**
   * DqCheck Case Class
   * Members
   * dqCheckSourceAndTargetDFName           Array[String] Type.Need to provide.Maps to app.dqCheck.dqCheckSourceAndTargetDFName in config file
   * dqColumns                              Seq[String] Type.Need to provide.Maps to app.dqCheck.dqColumns in config file
   * dupsCheckKey                           Seq[String] Type.Defaulted to empty.Maps to app.dqCheck.dupsCheckKey in config file
   * nullsCheckKey                          Seq[String] Type .Defaulted to empty.Maps to app.dqCheck.nullsCheckKey in config file
   * loggerName                             String Type.Need to provide..Maps to app.dqCheck.loggerName in config file
   * reportingDateColumnFlag                boolean Type.Defaulted to false.Maps to app.dqCheck.reportingDateColumnFlag in config file
   * srcColumnRenameList                    Seq[(String, String)] Type.Defaulted to empty.Maps to app.dqCheck.srcColumnRenameList in config file
   * tgtColumnRenameList                    Seq[(String, String)] Type.Defaulted to empty.Maps to app.dqCheck.tgtColumnRenameList in config file
   * scale                                  Int Type.Defaulted to 0.Maps to app.dqCheck.scale in config file
   * throwExceptionUponFailure              boolean Type.Defaulted to true.Maps to app.dqCheck.throwExceptionUponFailure in config file
   * nullCheckFlag                          boolean Type .Defaulted to true.Maps to app.dqCheck.nullCheckFlag in config file
   * dupCheckFlag                           boolean Type.Defaulted to true.Maps to app.dqCheck.dupCheckFlag in config file
   * partitionDateColName                   String Type.Defaulted to report_d.Maps to app.dqCheck.partitionDateColName in config file
   * targetTablename                        String Type.Defaulted to empty.Maps to app.dqCheck.targetTablename in config file
   * threshold                              Double Type Type.Defaulted to 0.Maps to app.dqCheck.threshold in config file
   * thresholdPercent                       Double Type.Defaulted to 0.Maps to app.dqCheck.thresholdPercent in config file
   * thresholdPercentJson                   String Type.Defaulted to empty string.Maps to app.dqCheck.thresholdPercentJson in config file
   * ignoreCountCheck                       boolean Type.Defaulted to false.Maps to app.dqCheck.ignoreCountCheck in config file
   * dqCheckResultShowToLog                 boolean Type.Defaulted to false.Maps to app.dqCheck.dqCheckResultShowToLog in config file
   *
   * @return DqCheck type
   */

  case class DqCheck(pipeLineConfig: PipeLineConfig) {
    val dqCheckSourceAndTargetDFName: Array[String] = convertStringToArray(getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_SOURCEANDTARGETDFNAME))
    val dqColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_DQCOLUMNS)
    val dupsCheckKey: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_DUPSCHECKKEY)
    val nullsCheckKey: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_NULLSCHECKKEY)
    val loggerName: String = getStringValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_LOGGERNAME)
    val reportingDateColumnFlag: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_REPORTINGDATECOLUMNFLAG)
    val srcColumnRenameList: Seq[(String, String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_SRCCOLUMNRENAMELIST)
    val tgtColumnRenameList: Seq[(String, String)] = getValueFromPipeLineConfigAndConvertToListOfStringTuple(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_TGTCOLUMNRENAMELIST)
    val scale: Int = getIntValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_SCALE,0)
    val throwExceptionUponFailure: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_THROWEXCEPTIONUPONFAILURE,true)
    val nullCheckFlag: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_NULLCHECKFLAG,true)
    val dupCheckFlag: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_DUPCHECKFLAG,true)
    val partitionDateColName: String = getStringValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_PARTITIONDATECOLNAME,"report_d")
    val targetTablename: String = getStringValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_TARGETTABLENAME)
    val threshold: Double = getDoubleValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_THRESHOLD,0)
    val thresholdPercent: Double = getDoubleValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_THRESHOLDPERCENT,0)
    val thresholdPercentJson: String = getStringValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_THRESHOLDPERCENTJSON)
    val ignoreCountCheck: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_IGNORECOUNTCHECK)
    val dqCheckResultShowToLog: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DQCHECK_DQCHECKRESULTSHOWTOLOG)
  }

  /**
   * ArchiveArchivePartition Case Class
   * Members
   * deleteSource              Boolean Type.Maps to app.archiveArchivePartition.deleteSource in config file
   * partitionColName          String Type.Defaulted to empty string.Maps to app.archiveArchivePartition.partitionColName in config file
   *
   * @return ArchiveArchivePartition type
   */
  case class ArchiveArchivePartition(pipeLineConfig: PipeLineConfig) {

    val deleteSource: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ARCHIVEARCHIVEPARTITION_DELETESOURCE)
    val partitionColName: String = getStringValueFromPipeLineConfig(pipeLineConfig, MarketingConstants.APP_atomicHistPartitionColumnName)
  }


  /**
   * ArchiveEphemeral Case Class
   * Members
   * deleteSource              Boolean Type Type.Maps to app.archiveEphemeral.deleteSource in config file
   *
   * @return ArchiveEphemeral type
   */
  case class ArchiveEphemeral(pipeLineConfig: PipeLineConfig) {
    val deleteSource: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ARCHIVEEPHEMERAL_DELETESOURCE)

  }

  /**
   * AtomicHistLoad Case Class
   * Members
   * inputDFNameFromExecutionOrder              String Type.Defaulted to empty string.Maps to app.atomicHistLoad.inputDFNameFromExecutionOrder in config file
   * insertColumns                              Seq[String]  Type.Maps to app.atomicHistLoad.insertColumns in config file
   * partitionColumns                           Seq[String]  Type.Maps to app.atomicHistLoad.partitionColumns in config file
   * repartition                                Boolean Type.Maps to app.atomicHistLoad.repartition in config file
   *
   * @return AtomicHistLoad type
   */
  case class AtomicHistLoad(pipeLineConfig: PipeLineConfig) {

    val inputDFNameFromExecutionOrder: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ATOMICHISTLOAD_INPUTDFNAMEFROMEXECUTIONORDER)
    val insertColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ATOMICHISTLOAD_INSERTCOLUMNS)
    val partitionColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig: PipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ATOMICHISTLOAD_PARTITIONCOLUMNS)
    val repartition: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_ATOMICHISTLOAD_REPARTITION)

  }

  /**
   * FndLoad Case Class
   * Members
   * inputDFNameFromExecutionOrder                           String Type.Defaulted to empty string.Maps to app.fndLoad.inputDFNameFromExecutionOrder in config file
   * insertColumns                                           Seq[String]  Type.Maps to app.fndLoad.insertColumns in config file
   * partitionColumns                                        Seq[String]  Type.Maps to app.fndLoad.partitionColumns in config file
   * repartition                                             Boolean Type.Maps to app.fndLoad.repartition in config file
   *
   * @return FndLoad type
   */
  case class FndLoad(pipeLineConfig: PipeLineConfig) {

    val inputDFNameFromExecutionOrder: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_FNDLOAD_INPUTDFNAMEFROMEXECUTIONORDER)
    val insertColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_FNDLOAD_INSERTCOLUMNS)
    val partitionColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_FNDLOAD_PARTITIONCOLUMNS)
    val repartition: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_FNDLOAD_REPARTITION)
  }

  /**
   * DeleteTriggerFile Case Class
   * Members
   * doneFile              String Type.Defaulted to empty string.Maps to app.deleteTriggerFile.doneFile in config file
   *
   * @return DeleteTriggerFile type
   */
  case class DeleteTriggerFile(pipeLineConfig: PipeLineConfig) {

    val doneFile: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_DELETETRIGGERFILE_DONEFILE)

  }

  /**
   * SendDFToEmail Case Class
   * Members
   * customSqlFlag          Boolean Type.Defaulted to false.Maps to app.sendDFToEmail.customSqlFlag in config file
   * customSql              String Type.Defaulted to empty string.Maps to app.sendDFToEmail.customSql in config file
   * fromAddress            String Type.Defaulted to empty string.Maps to app.sendDFToEmail.fromAddress in config file
   * toAddress              String Type.Defaulted to empty string.Maps to app.sendDFToEmail.toAddress in config file
   * reportFilename         String Type.Defaulted to empty string.Maps to app.sendDFToEmail.reportFilename in config file
   * emailSubject           String Type.Defaulted to empty string.Maps to app.sendDFToEmail.emailSubject in config file
   * emailBodySubject       String Type.Defaulted to empty string.Maps to app.sendDFToEmail.emailBodySubject in config file
   * attachDFResult         Boolean Type.Defaulted to false.Maps to app.sendDFToEmail.attachDFResult in config file
   * attachDFResultToEmailBody Boolean Type.Defaulted to false.Maps to app.sendDFToEmail.attachDFResultToEmailBody in config file
   * debug String Type.Defaulted to "false".Maps to app.sendDFToEmail.debug in config file
   * @return SendDFToEmail type
   */
  case class SendDFToEmail(pipeLineConfig: PipeLineConfig) {

    val customSqlFlag: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_CUSTOMSQLFLAG)
    val customSql: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_CUSTOMSQL)
    val fromAddress: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_FROMADDRESS)
    val toAddress: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_TOADDRESS)
    val reportFilename: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_REPORTFILENAME)
    val emailSubject: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_EMAILSUBJECT)
    val emailBodySubject: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_EMAILBODYSUBJECT)
    val attachDFResult: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_ATTACHDFRESULT)
    val attachDFResultToEmailBody: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_ATTACHDFRESULTTOEMAILBODY)
    val debug: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SENDDFTOEMAIL_DEBUG,"false")

  }

  /**
   * TransferFromSftpToHdfs Case Class
   * Members
   * sftpPasswordJceKeyStorePath          String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpPasswordJceKeyStorePath in config file
   * sftpPasswordJceKeyName               String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpPasswordJceKeyName in config file
   * sftpPrivateKeyPath                   String Type.Defaulted to empty string(Bigred3 private key is in the path /user/$nuid/hive/privatekey/privatekey).Maps to app.transferFromSftpToHdfs.sftpPrivateKeyPath in config file
   * sftpUser                             String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpUser in config file
   * sftpHost                             String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpHost in config file
   * sftpPort                             Int Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpPort in config file
   * sftpFileLocationPath                 String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpFileLocationPath in config file
   * sftpFileRenameFileName               String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpFileRenameFileName in config file
   * sftpSearchFilePattern                String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.sftpSearchFilePattern in config file
   * copyMechanismType                    String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.copyMechanismType in config file
   * hdfsFileLocationPath                 String Type.Defaulted to empty string.Maps to app.transferFromSftpToHdfs.hdfsFileLocationPath in config file
   * fileCompletenessCheck                Boolean.Defaulted to false.If set to true then will compares initial file size of the file in sft server with current file size.
   *                                      and modified timestamp.If file size is same and modified timestamp is same then we will return unit else will retry based on retriesForFileCompletenessCheck parameter
   *                                      with sleeping between retries based on sleepForFileCompletenessCheck parameter.
   *                                      will throw exception after retries
   * retriesForFileCompletenessCheck      Int.Defaulted to 2.Applicable if fileCompletenessCheck is set
   * sleepForFileCompletenessCheck        Long.Defaulted to 3 mins.Applicable if fileCompletenessCheck is set
   * isFileAlreadySftpCheck  :            Defaulted to false.Used while moving file from sftp to hdfs.If set will check the sftp file if its already moved to hdfs which is indicated by parameter hdfsFileLocationToCheckIsFileAlreadySftpCheck
   *                                      If yes then will skip the file
   * hdfsFileLocationToCheckIsFileAlreadySftpCheck : Defaulted to empty string.Used while moving file from sftp to hdfs.hdfs file location to keep track of files that are sftp

   * @return TransferFromSftpToHdfs type
   */
  case class TransferFromSftpToHdfs(pipeLineConfig: PipeLineConfig) {

    val sftpPasswordJceKeyStorePath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPASSWORDJCEKEYSTOREPATH)
    val sftpPasswordJceKeyName: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPASSWORDJCEKEYNAME)
    //val sftpPrivateKeyPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH)
    val env: String = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    val sftpPrivateKeyPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH)
//    val sftpPrivateKeyPath: String = env match {
//      case "stage" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH,"/user/SVMDEDMD/hive/privatekey/privatekey")
//      case "stg" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH,"/user/SVMDEDMD/hive/privatekey/privatekey")
//      case "prod" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH,"/user/SVMDEDMP/hive/privatekey/privatekey")
//      case "prd" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH,"/user/SVMDEDMP/hive/privatekey/privatekey")
//      case _ => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPRIVATEKEYPATH)
//    }
    val sftpUser: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPUSER)
    val sftpHost: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPHOST)
    val sftpPort: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPPORT)
    val sftpFileLocationPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPFILELOCATIONPATH)
    val sftpFileRenameFileName: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPFILERENAMEFILENAME)
    val sftpSearchFilePattern: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SFTPSEARCHFILEPATTERN)
    val copyMechanismType: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_COPYMECHANISMTYPE,"stream")
    val hdfsFileLocationPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_HDFSFILELOCATIONPATH)
    val fileCompletenessCheck: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_FILECOMPLETENESSCHECK)
    val retriesForFileCompletenessCheck: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_RETRIESFORFILECOMPLETENESSCHECK,2)
    val sleepForFileCompletenessCheck: Long = getLongValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_SLEEPFORFILECOMPLETENESSCHECK,3*60000)
    val isFileAlreadySftpCheck: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_ISFILEALREADYSFTPCHECK)
    val hdfsFileLocationToCheckIsFileAlreadySftpCheck: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMSFTPTOHDFS_HDFSFILELOCATIONTOCHECKISFILEALREADYSFTPCHECK)


  }


  /**
   * TransferFromHdfsToSftp Case Class
   * Members
   * sftpPasswordJceKeyStorePath          String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.sftpPasswordJceKeyStorePath in config file
   * sftpPasswordJceKeyName               String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.sftpPasswordJceKeyName in config file
   * sftpPrivateKeyPath                   String Type.Defaulted to empty string(Bigred3 private key is in the path /user/$nuid/hive/privatekey/privatekey).Maps to app.transferFromHdfsToSftp.sftpPrivateKeyPath in config file
   * sftpUser                             String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.sftpUser in config file
   * sftpHost                             String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.sftpHost in config file
   * sftpPort                             Int Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.sftpPort in config file
   * hdfsFileLocationPath                 String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.hdfsFileLocationPath in config file
   * hdfsFileRenameFlag                   Boolean Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.hdfsFileRenameFlag in config file
   * hdfsFileRenameFileName               String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.hdfsFileRenameFileName in config file
   * hdfsSearchFilePattern                String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.hdfsSearchFilePattern in config file
   * copyMechanismType                    String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.copyMechanismType in config file
   * sftpSleep                            Long Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.sftpSleep in config file
   * sftpFileLocationPath                 String Type.Defaulted to empty string.Maps to app.transferFromHdfsToSftp.sftpFileLocationPath in config file

   * @return TransferFromHdfsToSftp type
   */
  case class TransferFromHdfsToSftp(pipeLineConfig: PipeLineConfig) {

    val sftpPasswordJceKeyStorePath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPASSWORDJCEKEYSTOREPATH)
    val sftpPasswordJceKeyName: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPASSWORDJCEKEYNAME)
    //val sftpPrivateKeyPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH)
    val env: String = pipeLineConfig.config.getString(MarketingConstants.APP_ENV)
    val sftpPrivateKeyPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH)
//    val sftpPrivateKeyPath: String = env match {
//      case "stage" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH,"/user/SVMDEDMD/hive/privatekey/privatekey")
//      case "stg" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH,"/user/SVMDEDMD/hive/privatekey/privatekey")
//      case "prod" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH,"/user/SVMDEDMP/hive/privatekey/privatekey")
//      case "prd" => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH,"/user/SVMDEDMP/hive/privatekey/privatekey")
//      case _ => getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPRIVATEKEYPATH)
//    }
    val sftpUser: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPUSER)
    val sftpHost: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPHOST)
    val sftpPort: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPPORT)
    val hdfsFileLocationPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_HDFSFILELOCATIONPATH)
    val hdfsFileRenameFlag: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_HDFSFILERENAMEFLAG)
    val hdfsFileRenameFileName: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_HDFSFILERENAMEFILENAME)
    val hdfsSearchFilePattern: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_HDFSSEARCHFILEPATTERN)
    val copyMechanismType: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_COPYMECHANISMTYPE,"stream")
    val sftpSleep: Long = getLongValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPSLEEP)
    val sftpFileLocationPath: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_TRANSFERFROMHDFSTOSFTP_SFTPFILELOCATIONPATH)


  }


  /**
   * SaveDFToHdfs Case Class
   * Members
   * selectColumns                  Seq[String] Type.Defaulted to empty.Maps to app.saveDFToHdfs.selectColumns in config file
   * filterCondition                String Type.Defaulted to empty string.Maps to app.saveDFToHdfs.filterCondition in config file
   * hdfsTempFileLocation           String Type.Defaulted to empty string.Maps to app.saveDFToHdfs.hdfsTempFileLocation in config file
   * hdfsFileLocation               String Type.Defaulted to empty string.Maps to app.saveDFToHdfs.hdfsFileLocation in config file
   * hdfsSaveOptions                String Type.Defaulted to empty string.Maps to app.saveDFToHdfs.hdfsSaveOptions in config file
   * hdfsSaveMode                   Map[String, String] Type.Defaulted to OVERWRITE .Maps to app.saveDFToHdfs.hdfsSaveMode in config file
   * hdfsSaveFileFormat             String Type.Defaulted to CSV.Maps to app.saveDFToHdfs.hdfsSaveFileFormat in config file
   * hdfsSaveCoalesce               Int Type.Defaulted to 1.Maps to app.saveDFToHdfs.hdfsSaveCoalesce in config file
   * coalesceInputSplit             Int Type.Defaulted to 0.Maps to app.saveDFToHdfs.coalesceInputSplit in config file
   * hdfsRenameFilename             String Type.Defaulted to empty string.Maps to app.saveDFToHdfs.hdfsRenameFilename in config file
   * hdfsRenameFileSearchPattern    String Type.Defaulted to empty string.Maps to app.saveDFToHdfs.hdfsRenameFileSearchPattern in config file
   * deleteHdfsFileSearchPattern    String Type.Defaulted to *.Maps to app.saveDFToHdfs.deleteHdfsFileSearchPattern in config file
   * deleteHdfsFileRecursive        Boolean Type.Defaulted to false.Maps to app.saveDFToHdfs.deleteHdfsFileRecursive in config file

   * @return TransferFromHdfsToSftp type
   */
  case class SaveDFToHdfs(pipeLineConfig: PipeLineConfig) {

    val selectColumns: Seq[String] = getValueFromPipeLineConfigAndConvertToSeq(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_SELECTCOLUMNS)
    val filterCondition: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_FILTERCONDITION)
    val hdfsTempFileLocation: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSTEMPFILELOCATION)
    val hdfsFileLocation: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSFILELOCATION)
    val hdfsSaveOptions: Map[String, String] = getValueFromPipeLineConfigAndConvertToMap(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSSAVEOPTIONS)
    val hdfsSaveMode: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSSAVEMODE,"OVERWRITE")
    val hdfsSaveFileFormat: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSSAVEFILEFORMAT,"CSV")
    val hdfsSaveCoalesce: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSSAVECOALESCE,1)
    val coalesceInputSplit: Int = getIntValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_COALESCEINPUTSPLIT,0)
    val hdfsRenameFilename: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSRENAMEFILENAME)
    val hdfsRenameFileSearchPattern: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_HDFSRENAMEFILESEARCHPATTERN)
    val deleteHdfsFileSearchPattern: String = getStringValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_DELETEHDFSFILESEARCHPATTERN,"*")
    val deleteHdfsFileRecursive: Boolean = getBooleanValueFromPipeLineConfig(pipeLineConfig, CommonExecutePipelineApplicationConstants.APP_SAVEDFTOHDFS_DELETEHDFSFILERECURSIVE)


  }
}
