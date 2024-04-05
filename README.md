 # mdf_dpp_common
A library of reusable Spark Scala code common to all Data Processing Pipelines for Silver and Gold Layer

## Version Information
This library currently supports the following versions:

| Scala   | Spark        | Java | Default |
|---------|--------------|------|---------|
| 2.11.12 | 2.3.1.tgt.26 | 8    | No      |
| 2.12.18 | 3.4.1        | 11   | Yes     |

In order to accomplish this, the build.sbt is set up to support [Cross Building](https://www.scala-sbt.org/1.x/docs/Cross-Build.html). 
While building/testing in local systems, the default version a developer will be set to is `2.12.xx` which should be backwards
compatible to `2.11.xx`. It is important then, that before pushing up changes, a developer tests for compatibility in both
supported versions. 

### Cross-Building
When running any SBT task (eg. 'clean test', 'jacoco', etc), the default versions of Scala/Spark/Java are used. In order
to run an SBT task against the non-default version, a developer just needs to preceed the task with a `+` symbol:

> sbt '+ clean; + jacoco'

Alternatively, a `++` followed by the target version of Scala would permanently switch all following tasks to run against
the specified version:

> sbt '++2.11.12; clean; jacoco'

### Managing Multiple JDK Versions
When developing in local, since each version of Spark/Scala depends on a different version of Java, it is important to
have both installed, and be able to switch between versions as needed.

#### SDKMan
The easiest way to do this is by using [SDKMan](https://sdkman.io). 

#### IntelliJ SBT Shell and Test GUI
Since the default version of Spark/Scala requires JDK 11, be sure to set your IntelliJ project to point to that version of
Java. That will allow a developer to launch and use SBT Shell, as well as run tests through IntelliJ against the default 
version set. Go to the Project settings through the following menu navigation:
> File ==> Project Structure

This will open a pop-up window titled "Project Structure". On the default page (which is organized under Project Settings - Project)
is a drop-down selection titled "SDK". Open this to point to the JDK for the version you wish to open your SBT Shell and
run tests against within IntelliJ.

#### Terminal SBT
As mentioned in the [IntelliJ SBT Shell and Test GUI](#intellij-sbt-shell-and-test-gui), running SBT commands through 
the SBT Shell built into IntelliJ as well as launching testing through the IntelliJ GUI requires you to switch between 
JDK versions depending on which version of Spark/Scala you want to test. When testing through the Terminal, you would also
need to ensure the version of JDK being used to launch SBT supports that version of Scala/Spark.

Assume in the local developer's system, they have the following JDKs installed and are using SDKMan:
* Java 8 => 8.0.362-amzn
* Java 11 => 11.0.18-tem

To run against the non-default version, in one terminal:
> sdk use java 8.0.362-amzn; sbt '++ 2.11.12; clean; jacoco'

To run against the default version, in one terminal:
> sdk use java 11.0.18-tem; sbt 'clean; jacoco'

#### Docker Images for CICD Builds
When including this jar into a library, ensure the version of JDK testing in that project also matches. The same is true
in CICD. The Docker image used in the CICD step must have a compatible JDK in order to successfully run SBT tasks in that 
step.

#### SBT Plugin Limitations
The versions of the SBT plugins for `jacoco`, `scoverage` and `scapegoat` were bumped to make them as compatible as possible
with the two supported versions of Scala in this project. Unfortunately, due to issues with how these plugins began to be
built pinned to minor releases of Scala, it is no longer possible to perform `scoverage` on the `2.11.12` build. Since
Sonar metrics only need to be collected for one of the builds, and not both, the CICD pipeline will only perform a `test` on
the `2.11.12` build, but will perform all other checks, eg. `sbt clean coverage jacoco coverageReport sonarScan` on the 
`2.12.18` version.

### CommonExecutePipelineService
(Refer Readme of common_execute_pipeline for config file parameters)
CommonExecutePipelineService could also be extended in any DPP and use the common template with entries in config file for handling
common transformation like moveEphemeralFilesToProcess,readInput,validateSchema,
addFileDate,addDatePartitionColumn,dropDups,validateNotNull,ValidateDatatype,ValidateDateFormat etc which will output
list of output of dataframe after executing each transformation/Action mentioned in app.executionOrder.executionOrder
(If any transformation does not output a Dataframe then previous transformation Dataframe in stored in list of output of dataframe
like archiveArchivePartition will move files but will add the previous transformation output dataframe to list,so
if there 9 entries in executionOrder then there will 9 dataframes in the list of final output with 0-8 as index
and pick any dataframe and apply subsequent transformation specific to project like below
(Each transformation/Action/File Operation stages has a corresponding wrapper like CommonExecutePipelineUtils.archiveArchivePartition has CommonExecutePipelineWrapper.archiveArchivePartitionWrapper which could be called
which takes case class as parameter and once initialized will read from config file the corresponding parameters or could be called directly like
CommonExecutePipelineUtils.archiveArchivePartition and parameters are passed from application specific constants instead of config file)

### Transformation name:
      moveEphemeralFilesToProcess
      readInput
      readInputKafkaFiles
      validateSchema
      flatten
      addFileDate
      addDatePartitionColumn
      getLatestByKeyCol(for eg gets latest record if there are same partitions across multiple files or file dates) 
      countByFileDateAndPartitionDate(groups by partition date and file date and gets the count and collapse and coverts this to key/value pair and stores in grafana metric tgtDfPartitionFileDateCountJson and tgtDfFileDate
      dropDups
      validateNotNull
      validateDataType
      validateDateFormat
      removeDollarCharacter
      replaceNullWithDefaultForAmtColumn
      replaceNullWithDefaultForQtyColumn
      replaceNullWithDefaultForStringColumn
      removeNonUtfCharacter
      convertMicroToCurrency
      handleNullsInPKColumns
      convertDatePartOfTsToDefault
      convertDateFormat
      convertTimestampToDate
      convertTimestampFromLocalToUtc
      convertTimestampFromUtcToLocal
      joinCal(joins calendar table)
      joinTwoDataFrames
      rename
      dqCheck
      sendDFToEmail(Will send DF result to email)
      transferFromSftpToHdfs(Will transfer from sftp tp hdfs)                                                                                    
      transferFromHdfsToSftp(Will transfer from hdfs tp sftp)                                  
      saveDFToHdfs(Will save DF to HDFS path and renames the file)

### Action Name:
      atomicHistLoad
      fndLoad
      archiveArchivePartition
      archiveEphemeral

### File Operation
        deleteTriggerFile(deletes the doneFile)
        updateCheckPointPath

```
  object PipeLineService extends LoggingTrait with CommonExecutePipelineService {

      def executePipeLine(config: MarketingPipelineConfigs): Unit = {
    
      implicit val sparkSession: SparkSession = config.sparkSession

        log.info("Parse the parameters after loading the config file and initialize the helper case classes corresponding to " +
          "the transformation that will called from CommonExecutePipeLineService")
        val initializeCommonPipeLineHelper = CommonPipeLineHelper(config.pipeLineConfig)
        val initializeRename = Rename(config.pipeLineConfig)
        val initializeJoinCal = JoinCal(config.pipeLineConfig)
        val initializeDqCheck = DqCheck(config.pipeLineConfig)
        val initializeArchiveArchivePartition = ArchiveArchivePartition(config.pipeLineConfig)
        val initializeArchiveEphemeral = ArchiveEphemeral(config.pipeLineConfig)
        val initializeFndLoad = FndLoad(config.pipeLineConfig)
    
        log.info("In executePipeLine Method")
    
        log.info("Calling CommonExecutePipeLineService and executing the transformation mentioned in the executionOrder in config file")
        val listOfDF = CommonExecutePipeLineService.executePipeLine(config: MarketingPipelineConfigs)
        log.info("Extracting the dataframes needed in the dpp from the list of dataframes after executing CommonExecutePipeLineService after providing the index number from the executionOrder of corresponding dataframe or transformation name.Index number start from 0")
        If   executionOrder: "moveEphemeralFilesToProcess,readInput,validateSchema,addFileDate,replaceNullWithDefaultForAmtColumn,replaceNullWithDefaultForQtyColumn,replaceNullWithDefaultForStringColumn,convertTimestampToDate,removeDollarCharacter,trim,validateNotNull,validateDataType"
        and want to pick output of addReportDate,removeDollarCharacter,validateDataType then
        val addReportDateDF = listOfDF(3)
        val removeDollarCharacterDF = listOfDF(10)
        val validateDateTypesDF = listOfDF(11)
    
         
         apply specific transformation like
          val unionDF = aggByNotInSharedIdDF.select(SELECTUNIONCOLUMNS.map(m => col(m)): _*)
        .union(filterInSharedIdWithNewColDF.select(SELECTUNIONCOLUMNS.map(m => col(m)): _*))
        ........
        
        Then could call the wrapper with parameters defined in config file
        val renameDF = renameWrapper(unionDF, initializeRename)
        val joinCalDF = joinCalWrapper(renameDF,initializeJoinCal,initializeCommonPipeLineHelper)
        dqCheckWrapper(removeDollarCharacterDF, joinCalDF, initializeDqCheck)
        fndLoadWrapper(joinCalDF,initializeFndLoad, initializeCommonPipeLineHelper)
        archiveArchivePartitionWrapper(initializeArchiveArchivePartition,initializeCommonPipeLineHelper)
        archiveEphemeralWrapper(initializeArchiveEphemeral,initializeCommonPipeLineHelper)


    }
    
  Sample flow:
  https://git.target.com/data-engineering-affiliates/impactradius_api_performance_by_media

   Below is how to initialize the helper case classes for different transformation available to parse parameters from config file
    
    Like if we want to use rename transformation then we could call
    val initializeRename = Rename(config.pipeLineConfig) 
    val renameDF = renameWrapper(inputDF, initializeRename)
    
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
```

## Transformation/Action/File Operation job names

### Transformation name:

    moveEphemeralFilesToProcess(will call Will call dpp common CommonExecutePipelineWrapper.moveEphemeralFilesToProcess.This takes parameter from CommonPipeLineHelper case class which has all common parameters defined from MarketingConstant)

    readInput(Will call FileReader.readFileToDataFrame.Maps to app.readInput followed by parameter name in config file)
              inputEphemeralFilesFormat: Represented as string in underlying method.Need to provide.Provide single string like "csv" or "json"
              templateLocation:  Represented as string and Optional in underlying method.Provide if inputEphemeralFilesFormat="json" to infer schema
              hasHeader:  Represented as boolean and Optional and defaulted to false in underlying method.
              renameColumns: Represented as Seq[String] and Optional and defaulted to empty in underlying method.Provide as comma separated string like "abc,qwer,asd" "
              selectColumns: Represented as Seq[String] and Optional and defaulted to empty in underlying method.Provide as comma separated string like "abc,qwer,asd" "
              filter:  Represented as string and Optional in underlying method.
              delimiter:  Represented as string and Optional in underlying method.Provide single string like ","
              options:  Represented as Map[String,String] and Optional in underlying method.Provide like "header->true,quote->\",escape->\",MultiLine->false,ignoreTrailingWhiteSpace-> true"
              getFileName:  Represented as boolean and Optional and defaulted to false in underlying method
              abortOnZeroRecords: Represented as boolean Type and Optional and defaulted to false in underlying method.If set will abort if input dataframe count is zero.

    readKafkaInput(Will call FileReader.ReadKafkaFiles)
              inputEphemeralFilesFormat: Represented as string in underlying method.Defaulted to Json
              templateLocation:  Represented as string in underlying method.Provide if inputEphemeralFilesFormat="json" to infer schema
              checkpointPath:  Represented as string in underlying method.Provide checkpoint path
              dropColumns: Represented as Seq[String] and Optional and defaulted to empty in underlying method.Provide as comma separated string like "abc,qwer,asd" "
              ephemeralFilePathPrefix:  Represented as string in underlying method.Defaulted to "load_date="
              addKafkaTimestamp:  Represented as boolean and Optional and defaulted to false in underlying method.Will add kafkaDateFieldName=report_d and kafkaTimestampFieldName=hive_load_timestamp as columns
              abortOnZeroRecords: Represented as boolean Type and Optional and defaulted to false in underlying method.If set will abort if input dataframe count is zero.
              handleEmptyTopic:Represented as boolean Type and Optional and defaulted to false in underlying method.If set will get previous day's records from the table based on partition column if input dataframe count is zero
              fndTablePartitionColumn:Represented as String Type and Optional and defaulted to empty string in underlying method.Used when handleEmptyTopic is true to get previous day's records from the table based on partition column

    validateSchema(Will call CommonExecutePipelineWrapper.validateSchema)
             csvSchemaString: Provide like if inputEphemeralFilesFormat=csv 
                              "Source string, CompanyName string,AccountId string, CampaignName string, CampaignId string,CampaignBudget string, CampaignStartDate string, CampaignEndDate string, ClassId string, DepartmentId string,VendorNumber string, TCIN string, CalendarYear string, CalendarMonth string, Revenue string,filename string"
             templateLocation=Provide if inputEphemeralFilesFormat="json"

    flatten(Will call kelsa FlattenFunctions.flattenDataframe)
            flatten:  Provide like true or false
    
    addFileDate(Will call dpp common DataFramUtils.regexExtractDateToDataframe)
          sourceDateFormat:      Provide like "yyyy-MM-dd"
          sourceColumn:  Provide single string like "filename"
          targetColumn:  Provide single string like "file_date"
    
    addDatePartitionColumn(Will call CommonExecutePipelineUtils.addDatePartitionColumn)
          isPartitionColumnDatatypeDate:  Provide like true if partition column in table is date else false
          sourceColumn:               Provide single string like "file_date"
          targetColumn:               Provide single string like "report_d"

    getLatestByKeyCol(Will call dpp common DataFrameUtils.getLatestByPartitionDateArray)
          keyColName:            Represented as Seq[string] in underlying method.Provide like "report_d,anyothercolumnname"
          orderByColName:        Represented as string in underlying method.Provide single string like "file_date"
          windowingStrategy:     Represented as string in underlying method.Provide single string like "row_number" or dense_rank or rank

     countByFileDateAndPartitionDate(Will call dpp common DataFrameUtils.countByFilePartitionDateAndCollect)
            partitionDateColumn: Represented as string in underlying method.Provide single string like "report_d"
            fileDateColumn: Represented as string in underlying method.Provide single string like "file_date"

    executeExternalSql(Will call CommonExecutePipelineUtils.executeExternalSql.Will execute sql against the output of any listed transformation name and show sample result)
          dataFrameNameUsedInSql: Represented as Seq[String].Provide all the name of Transformation/Action used in sql like "replaceNullWithDefaultForAmtColumn,addFileDate"
          inputSql: Represented as Seq[(String,String)].Provide sql accessing from  dataFrameNameUsedInSql like(key can be anything like 1 and value is the sql)
          """{ \"1\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from replaceNullWithDefaultForAmtColumn group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"   ,
                                                  \"2\":\"select Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs,
                                  sum(CampaignBudget)CampaignBudget,sum(Revenue) Revenue from addFileDate group by
                                  Source,CompanyName,AccountId,CampaignName,CampaignId, CampaignStartDate, CampaignEndDate, ClassId, DepartmentId,VendorNumber,
                                  TCIN, CalendarYear, CalendarMonth,filename,CampaignJoinDate,CampaignJoinDateTs,CampaignJoinDateLocalTs,CampaignJoinDateUtclTs\"
                                    }""",

    dropDups(Will call CommonExecutePipelineUtils.dropDup)
          location:  "/user/SVMDEDMD/landing/common_execute_pipeline/data_processing_pipeline/output/dqcorruptrecords/"
          partitionColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like  
                            "Source,CompanyName,AccountId,CampaignName ,CampaignId,CampaignStartDate,
                            CampaignEndDate,ClassId,DepartmentId,VendorNumber,TCIN,CalendarYear,CalendarMonth"
          orderByColumn:  Provide single string like "CampaignStartDate"

    validateNotNull(Will call dpp common DataFrameValidatorService.validateNOTNULLColumns)
        inputColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like 
                                "Source,CompanyName,AccountId,CampaignName ,CampaignId,
                                CampaignStartDate,CampaignEndDate,ClassId,DepartmentId,VendorNumber,TCIN,CalendarYear,CalendarMonth"

    validateDataType(Will call dpp common DataFrameValidatorService.validateDataTypes)
        inputColumns: Represented as Seq[String] in underlying method.Provide as Json like string like "{\"CampaignBudget\":\"decimal(38,2)\", \"Revenue\":\"decimal(38,8)\"}"

    validateDateFormat(Will call dpp common DataFrameValidatorService.validateDateFormat)
        inputColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like "CampaignStartDate,CampaignEndDate"
        sourceDateFormat:  Provide like "yyyy-MM-dd"

    removeDollarCharacter(Will call dpp common DataFramUtils.removeDollarCharacter)
        inputColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like "CampaignBudget,Revenue"

    replaceNullWithDefaultForAmtColumn(Will call dpp common DataFramUtils.replaceNullWithDefault)
        inputColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like  "CampaignBudget,Revenue"
        default: Provide like  "0.00"

    replaceNullWithDefaultForQtyColumn(Will call dpp common DataFramUtils.replaceNullWithDefault)
        inputColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like "CampaignBudget,Revenue"
        default:  Provide like  "0"

    replaceNullWithDefaultForStringColumn(Will call dpp common DataFramUtils.replaceNullWithDefault)
    inputColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like "CampaignBudget,Revenue"
    default:  Provide like  ""
    
    removeNonUtfCharacter(Will call dpp common DataFramUtils.removeNonUtfCharacter)
        inputColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like  "CampaignBudget,Revenue"

    convertMicroToCurrency(Will call dpp common RevenueTransformer.convertMicroToCurrency)
            inputColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like  "CampaignBudget,Revenue"

     handleNullsInPKColumns(Will call CommonExecutePipelineUtils.handleNullsInPKColumns)
            inputColumns: Represented as Seq[(String,String)] in underlying method.Provide Json like """{ \"CampaignId\":\"123\"   ,
                                            \"CampaignName\":\"NA\"   ,
                                            \"CampaignStartDate\":\"9999-12-31\"
                                            }"""

     convertDatePartOfTsToDefault(Will call dpp common DataFramUtils.convertToSparkDateTypeFormat)
             inputColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like  "CampaignJoinDate,CampaignStartDate"
             inputTsFormat: Provide like "MM/dd/yy HH:mm"

    convertDateFormat(Will call dpp common DataFramUtils.convertDateFormat)
        inputColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like  "CampaignJoinDate,CampaignStartDate"
        inputDateFormat:  Provide like "dd/MM/yyyy"
        outputDateFormat:  Provide like "yyyy-MM-dd"

    convertTimestampToDate(Will call CommonExecutePipelineUtils.convertTimestampToDate)
        inputColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like "CampaignJoinDateTs,CampaignStartDateTs"
        outputFormat:  Provide like "yyyy-MM-dd"

    convertTimestampFromLocalToUtc(Will call CommonExecutePipelineUtils.convertTimestampFromLocalToUtc)
        inputColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like "CampaignJoinDateLocalTs,CampaignStartDateLocalTs"
        inputFormat:   Provide like "yyyy-MM-dd'T'HH:mm:ss.SSS"
        outputFormat:  Provide like "yyyy-MM-dd HH:mm:ss"
        timezone:      Provide like "America/Chicago"

    convertTimestampFromUtcToLocal(Will call CommonExecutePipelineUtils.convertTimestampFromUtcToLocal)
        inputColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like "CampaignJoinDateUtclTs,CampaignStartDateUtclTs"
        inputFormat:   Provide like "yyyy-MM-dd'T'HH:mm:ss.SSS"
        outputFormat:  Provide like "yyyy-MM-dd HH:mm:ss"
        timezone:      Provide like "America/Chicago"

    joinCal(Will call CommonExecutePipelineUtils.joinCal)
        calendarColumns:                      Represented as Seq[String] in underlying method.Provide as comma separated string like 
                                              "fiscal_month_id,fiscal_month_n,fiscal_quarter_id ,
                                               fiscal_quarter_n, fiscal_year_id ,fiscal_year_week_id".If calendarColumnsWithAnyTransformation is specified then this could be empty string.
                                              Specify this even when specifying calendarColumnsWithAnyTransformation when running it local same as calendarColumnsWithAnyTransformation but only columns no transformation .
        calendarColumnsWithAnyTransformation: Represented as Seq[String] in underlying method.Provide as comma separated string like 
                                              """calendar_d,day_n,fiscal_week_n,fiscal_week_end_d,fiscal_week_begin_d,
                                                 concat(fiscal_week_begin_d , '-' ,fiscal_week_end_d) 
                                                 calendar_fiscal_week,fiscal_month_id,fiscal_month_abbreviation_n  fiscal_month_n , 
                                                 fiscal_quarter_id , fiscal_quarter_n, fiscal_year_id ,fiscal_year_week_id""".This takes precedence over calendarColumns
        calendarTable:                        Provide like "prd_cal_fnd.calendar"
        tempCalendarExtLocation:              Provide like  "/user/SVMDEDMD/hive/common_execute_pipeline/calendar"
        inputJoinConditionColumn:             Provide like  "report_d"
        calendarJoinConditionColumn:          Provide like  "calendar_d"

    
    joinTwoDataFrames(Will call dpp common DataFrameUtils.joinTwoDataFrames)
        joinTwoDataframesLeftAndRightDFName: Provide as comma separated transformation name which will be used as left and right Dataframes 
                                             like  "dropDups,rename"
        joinType:                            Provide like "inner" or "left" or "right"
        joinCondition:                       Provide like  "report_d = calendar_date"".join condition to join Left with Right as a string value.
                                             If for eg joining based on dma_code,if dma_code column exists in left as well as right table, dma_code column in right table
                                             should be prefixed with "right_" like   val joinConditionDma=  "dma_code = right_dma_code"
        prefixRightDFCommonCol:              Provide like  "right".If join column in the left and right table are same then right table
                                             column will be prefixed with prefixRightDFCommonCol + underscore like
                                             if dma_code column exists in left as well as right table, dma_code column in right table
                                             will be prefixed with default "right_" like right_dma_code"
    
    rename(Will call kelsa TransFunction.renameColumns)
        renameColumns:  Represented as Seq[(String,String)] in underlying method.Provide Json like  """{ \"Source\":\"source_name\"   ,
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
                                       }"""

    dqCheck(Will call CommonExecutePipelineWrapper.dataQualityCheckGeneric)
        dqCheckSourceAndTargetDFName: Provide as comma separated transformation name which will be used as source and target Dataframes 
                                      like  "dropDups,rename"
        dqColumns:                    Represented as Seq[String] in underlying method.Provide as comma separated string like "campaign_budget_a , revenue_a ,report_d"
        dupsCheckKey:                 Represented as Seq[String] in underlying method.Provide as comma separated string like
                                      "source_name,company_name,account_id,campaign_name ,campaign_id,campaign_budget_a,
                                      campaign_start_date,campaign_end_date,class_id,dept_id,vendor_number,tcin,calendar_year,calendar_month",
        nullsCheckKey:                Represented as Seq[String] in underlying method.Provide as comma separated string like 
                                      "source_name,company_name,account_id,campaign_name ,campaign_id,
                                      campaign_start_date,campaign_end_date,class_id,dept_id,vendor_number,tcin,calendar_year,calendar_month"
        loggerName:  Provide like "CitrusInvoiceRevenueMonthly",
        reportingDateColumnFlag: Defaulted to false.Provide like  "true",
        srcColumnRenameList: Represented as Seq[(String,String)] in underlying method.Provide Json like  """{ \"Source\":\"source_name\"   ,
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
                                    }""",
        tgtColumnRenameList: Represented as Seq[(String,String)] in underlying method.Provide Json like string.Defaulted to empty
        scale:  0,(Defaulted to 0)
        throwExceptionUponFailure:  "false",
        nullCheckFlag:  "true",(Defaulted to true)
        dupCheckFlag:  "true",(Defaulted to true)
        partitionDateColName:  "report_d",(Defaulted to report_d)
        targetTablename:  "stg_mdf_fnd.common_execute_pipeline",(Defaulted to empty)
        #threshold: (Defaulted to 0)
        #thresholdPercent:(Defaulted to 0)
        #thresholdPercentJson:(Defaulted to empty)
        ignoreCountCheck:  true,(Defaulted to false)
        dqCheckResultShowToLog:  false (Defaulted to false)
                
    archiveArchivePartition(Will call CommonExecutePipelineWrapper.archiveArchivePartition,partition column comes from app.atomicHistPartitionColumnName
                            and other parameters comes from CommonPipeLineHelper type which has most of parameters defined in the config file )
      deleteSource:  "false" (Defaulted to false)

    archiveEphemeral(Will call CommonExecutePipelineWrapper.archiveEphemeral)
      deleteSource:  "true" (Defaulted to false)

    sendDFToEmail(This calls dpp common CommonExecutePipelineWrapper.sendDFToEmail.This method is to send an email to the given address by attaching the input dataframe)
                 If attachDFResult and attachDFResultToEmailBody is set to false then DF will not be attached but still
                 needs to provide a dummy dataframe as inputDF in the wrapper CommonExecutePipelineWrapper.sendDFToEmail)
        customSqlFlag:              Boolean.Defaulted to false.If set then will execute customSql against inputDF and result is send to email
        customSql:                  If customSqlFlag is set then will execute customSql against inputDF and result is send to email
        fromAddress:                From Address
        toAddress:                  To Address
        reportFilename:             Report Filename with extension(timestamp will be added to the filename) attached to the email.If empty string then will not attach the file.Date will added to filename and csv file extension will also be added
        emailSubject:               Email Subject line.Date will added to Subject line
        emailBodySubject:           Subject line in the message body.Date will added to Subject line
        attachDFResult:             Boolean.Defaulted to false.If set then will attach DF as a file to the email
        attachDFResultToEmailBody:  Boolean.Defaulted to false.If set then will attach DF to the email body in html format
        debug:                      String.Values are "true" or "false".If set to "true" then debug mode is turned on for email service

    transferFromSftpToHdfs
            Creates sftp connection and calls transferFromSftpToHdfs to transfer file from SFTP to HDFS
            Recursively transfer file from sftp server to hdfs.If sftp location contains sub directory then it copies files from sub directory as well
        sftpPasswordJceKeyStorePath:       Defaulted to empty string.If its password based then default sftpPrivateKeyPath to empty string.
        sftpPasswordJceKeyName:            Defaulted to empty string.If its password based then default sftpPrivateKeyPath to empty string
        sftpPrivateKeyPath:                Defaulted to /user/$nuid/hive/privatekey/privatekey which contains the private key.Nuid is SVMDEDMD/SVMDEDMP.If its key based then no need to provide sftpPasswordJceKeyStorePath,sftpPasswordJceKeyName
        sftpUser:                          sftpUser
        sftpHost:                          sftpHost
        sftpPort:                          sftpPort
        sftpFileLocationPath:              Sftp file location
        sftpFileRenameFileName:            Defaulted to empty string.sftp rename file name with extension.If set ,sftp File name will be renamed to sftpFileRenameFileName in the hdfs
                                            If there are multiple files in sftp which needs to be renamed while moving to hdfs format of rename will be filename_$i.fileextension else will use the filename in remote
        sftpSearchFilePattern:             Defaulted to empty string.Filename search pattern to look for file in remote/sftp server
        copyMechanismType                  Values are download or if not specified will default to stream.If download then will download to java temp dir and then moved to hdfs.If not set to download then will stream the file from hdfs to sftp.
        hdfsFileLocationPath:              HDFS file location
        fileCompletenessCheck              Defaulted to false.If set to true then will compares initial file size of the file in sft server with current file size.
                                            and modified timestamp.If file size is same and modified timestamp is same then we will return unit else will retry based on retriesForFileCompletenessCheck parameter
                                            with sleeping between retries based on sleepForFileCompletenessCheck parameter.
                                            will throw exception after retries
        retriesForFileCompletenessCheck    Defaulted to 2.Applicable if fileCompletenessCheck is set
        sleepForFileCompletenessCheck      Defaulted to 3 min.Applicable if fileCompletenessCheck is set
        isFileAlreadySftpCheck  :          Defaulted to false.Used while moving file from sftp to hdfs.If set will check the sftp file if its already moved to hdfs which is indicated by parameter hdfsFileLocationToCheckIsFileAlreadySftpCheck
                                            If yes then will skip the file
        hdfsFileLocationToCheckIsFileAlreadySftpCheck : Defaulted to empty string.Used while moving file from sftp to hdfs.hdfs file location to keep track of files that are sftp


    transferFromHdfsToSftp
            Creates sftp connection and calls transferFromHdfsToSftp to transfer file from HDFS to SFTP
            Recursively transfer file from HDFS location to SFTP location.If hdfs location contains sub directory then it copies files from sub directory as well
        sftpPasswordJceKeyStorePath:       Defaulted to empty string.If its password based then default sftpPrivateKeyPath to empty string.
        sftpPasswordJceKeyName:            Defaulted to empty string.If its password based then default sftpPrivateKeyPath to empty string
        sftpPrivateKeyPath:                Defaulted to /user/$nuid/hive/privatekey/privatekey which contains the private key.Nuid is SVMDEDMD/SVMDEDMP.If its key based then no need to provide sftpPasswordJceKeyStorePath,sftpPasswordJceKeyName
        sftpUser:                          sftpUser
        sftpHost:                          sftpHost
        sftpPort:                          sftpPort
        hdfsFileLocationPath:              HDFS file location
        hdfsFileRenameFlag:                Defaulted to true.If set then will rename the file(hdfsFileRenameFileName) in the sftpFileLocation
        hdfsFileRenameFileName:            Defaulted to empty string.Hdfs rename file name.Hdfs File name will be renamed to hdfsFileRenameFileName in the sftp server
                                            If there are multiple files in hdfs which needs to be renamed while moving to sftpformat of rename will be filename_$i.fileextension.
        hdfsSearchFilePattern:             Defaulted to empty string.Used to search for hdfs file in the hdfsFileLocation based on this pattern
        copyMechanismType:                  Values are download or if not specified will default to stream. If download then will download to java temp dir and then moved to hdfs.If not set to download then will stream the file from hdfs to sftp.
        sftpSleep:                          Defaulted to 0 millisecond.Will sleep before ftping the next file.
        sftpFileLocationPath:              Sftp file location

    saveDFToHdfs
            Will save DF to HDFS path and renames the file
        selectColumns:                select Columns from inputDF
        filterCondition:             filter Condition on inputDF
        hdfsTempFileLocation:        Temp Hdfs location where the DF will stored.
        hdfsFileLocation:            File in hdfsTempFileLocation will be moved to this hdfs location after renaming the file in sftpHdfsTempFileLocation to sftpFilename
        hdfsSaveOptions:             Options to be used while saving DF to HDFS.like Map("header"->"true","sep"->",","mapreduce.fileoutputcommitter.marksuccessfuljobs"->"false")
        hdfsSaveMode:                Defaulted to overwrite to be used while saving DF to HDFS
        hdfsSaveFileFormat:          Defaulted to csv to be used while saving DF to HDFS
        hdfsSaveCoalesce:            Defaulted to  1 to be used while saving DF to HDFS
        coalesceInputSplit:          Defaulted to  1.If greater than 1 then inputDF will be split by the coalesceInputSplit and saves those many files in hdfs
        hdfsRenameFilename:          Defaulted to empty string.Filename with extension.Timestamp will be added to the filename.If set,this filename will be used to rename file in hdfs.If set ,file in hdfsTempFileLocation will be renamed to hdfsRenameFilename in the hdfs.If there are multiple files in hdfsTempFileLocation which needs to be renamed while moving to hdfs
                                     format of rename will be filename_$i_timestamp.fileextension else will use the filename in  hdfsTempFileLocation
        hdfsRenameFileSearchPattern: Defaulted to empty string.Based on the pattern, file in searched in the path hdfsTempFileLocation and this will be renamed before moving to hdfsFileLocation
        deleteHdfsFileSearchPattern: Defaulted to * .Based on the pattern file will be deleted from hdfsFileLocation before renaming file in hdfsTempFileLocation to hdfsFileLocation
        deleteHdfsFileRecursive     Defaulted to false.If set,file will be deleted recursively from hdfsFileLocation before renaming file in hdfsTempFileLocation to hdfsFileLocation


### Action Name:
    atomicHistLoad(Will call CommonExecutePipelineWrapper.atomicHistLoad)
      inputDFNameFromExecutionOrder: Provide from which transformation to pick data from like "addFileDate",
      insertColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like "Source,CompanyName , AccountId ,CampaignName , CampaignId , CampaignBudget ,CampaignStartDate , CampaignEndDate , DepartmentId , VendorNumber , TCIN ,CalendarYear , CalendarMonth , Revenue",
      partitionColumns:  Represented as Seq[String] in underlying method.Provide as comma separated string like  "file_date",
      repartition: Represented as Boolean in underlying method

    fndLoad(Will call CommonExecutePipelineWrapper.fndLoad)
      inputDFNameFromExecutionOrder:  Provide from which transformation to pick data like "rename"
      insertColumns:  Represented as Seq[String] in underlying method.Provide like """source_name,company_name ,
                                    account_id ,
                                    campaign_name,
                                    campaign_id ,
                                    campaign_budget_a ,
                                    campaign_start_date ,
                                    campaign_end_date ,
                                    class_id ,
                                    dept_id ,
                                    vendor_number ,
                                    tcin ,
                                    revenue_a ,
                                    file_date,
                                    calendar_year ,
                                    calendar_month,
                                    """,
      partitionColumns: Represented as Seq[String] in underlying method.Provide as comma separated string like "calendar_year,calendar_month",
      repartition: Represented as Boolean in underlying method

### File Operation
    deleteTriggerFile(Will call CommonExecutePipelineUtils.deleteTriggerFile)
        doneFile: ""

    updateCheckPointPath(Will call kelsa CompactorService.updateCheckPointPath.Will use checkpointPath from readKafkaInput and maxProcessedPartition is derived )  


## Sample Config File:
```
#Needed app config entries which are inputs to underlying methods
 
   app {
  #application name
  name: "CommonExecutePipeline"
  #blossom id of th project. Get blossom id from blossom application
  blossomID: "CI02995445",
  #team name
  teamName: "DataSciences-DE-MDF-TM"
  #Contact details
  contactDetails: "DataSciences-DE-MDF-TM@Target.com",
  env: "stg",
  sla: "01:00 PM"
  queueName: "SVMDEDMD_AUTO"
  executionEngine: "tez"
  dynamicPartition: "true"
  dynamicPartitionMode: "nonstrict"
  jdbcUrl: "jdbc:hive2://brcn1003.target.com:2181,brcn1004.target.com:2181,brcn1008.target.com:2181,brcn1009.target.com:2181,brcn1012.target.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
  hiveTezInputFormat: "org.apache.hadoop.hive.ql.io.HiveInputFormat"
  mapReduceJobReduces: "50"
  jdbcHiveDriver: "org.apache.hive.jdbc.HiveDriver"

  templateLocation:  "/user/SVMDEDMD/hive/common_execute_pipeline/sample_schema_datafile/",
  fileAPILandingLocation: "/user/SVMDEDMD/hive/common_execute_pipeline/land/",
  inputLocation: "/user/SVMDEDMD/hive/common_execute_pipeline/processing/",
  existingRecordsLocation: "/user/SVMDEDMD/hive/common_execute_pipeline/archive/",
  archivePartitonRecordsLocation: "/user/SVMDEDMD/hive/common_execute_pipeline/archive_partition/",
  corruptRecordLocation:  "/user/SVMDEDMD/landing/common_execute_pipeline/data_processing_pipeline/output/corruptrecords/"
  dataQualityLocation:  "/user/SVMDEDMD/landing/common_execute_pipeline/data_processing_pipeline/output/dqcorruptrecords/"

  # Failure location
  pipeLineFailures: "/user/SVMDEDMD/landing/common_execute_pipeline/data_processing_pipeline/output/pipeLineFailures"
  #location where lock will be created, so that user cannot rerun job if the lock is present
  lockLocation: "/user/SVMDEDMD/landing/common_execute_pipeline/configurations/dpp_lock"

  #Used for write to external table
  fndExtLocation:  "/user/SVMDEDMD/hive/stg_sgn_fnd/common_execute_pipeline/"

  #spark session creation properties
  spark_sources_partitionOverwriteMode: "static"
  hive_partition_mode: "nonstrict"
  spark_orc_impl: "native"
  spark_enableVectorizedReader: "true"
  spark_wareHouseLocation: "/apps/hive/warehouse"
  hive_max_dynamic_partitions: "2000"
  dqThreshold: "1"

  # slack
  dq_channelID: "dStqc8EeBLuKxRu5hdV4I0gWk0V4TPBnQ108UL2rwlEy7Xm4enCar6xMJ9Jp4IPn"
  alert_channelID: "syDd4MKrWEbL7GtNlptYM2040rSGhg1ASZvdtqJmjccsueXq7dnWC9NYUfVCy5lz"

  #DB schema names
  fndSchema:  "stg_mdf_fnd"
  stgSchema:  "stg_mdf_stg"
  lndSchema:  "stg_mdf_lzn"

  #DB table names
  lndCoreHistTable: "common_execute_pipeline_e"
  coreHistTable: "common_execute_pipeline"

  atomicHistPartitionColumnName: "file_date"
  coreHistPartitionColumnName: "file_date"
  #coalesce to be done for each stage. For demo we are taking this as global value.
  # Value should be calculated based on expected load
  coalesceFactor: 1
  #numberOfPartitions
  #the max partitions allowed to be written into the path provided
  numberOfPartitions: 10
    influxdb {
  #measurement name got from measurement team
  measurementName:  "common-execute-pipeline-stg"
  #end point of influx to push data
  grafanaEndPoint:  "https://metricsfrontdoor-shared.prod.target.com/write?db=metrics"
  }

    metrics {
  metricsKafkaPush:  false
  metricsInfluxPush: true
  }

    timeliness {
  jceksFilePath: "jceks://hdfs/common/SVMDEDMP/ssl_certs/mdf_secrets.jceks"
  truststoreFilePath: "client.truststore.jks"
  keystoreFilePath: "mdf-prd.target.com.jks"

  }
  #Common pipeline config entries
    executionOrder{
  executionOrder: "moveEphemeralFilesToProcess,readInput,validateSchema,removeDollarCharacter,replaceNullWithDefaultForAmtColumn,executeExternalSql,addFileDate,addDatePartitionColumn,dropDups,trim,validateDataType,validateDateFormat,rename,dqCheck,fndLoad,archiveArchivePartition,archiveEphemeral"

  }

    showResultsToLog{
  showResultsToLog:  "true",
  numOfRows: 10

  }

    showCustomResultsToLog{
  showCustomResultsToLog:  "true"
  dataFrameNameAndExecuteSql: """{ \"readInput\":\"select * from readInput\"   ,
                                              \"rename\":\"select * from rename\"
                                        }""",
  numOfRows: 10
  }

      abortOnZeroRecords{
  abortOnZeroRecords: "false"
  
  }
    readInput {
  inputEphemeralFilesFormat:  "csv"
  #templateLocation:  provide if inputEphemeralFilesFormat="json" to infer schema.Defaulted to empty string"
  #hasHeader:  "true"
  #renameColumns:  "defaulted to Seq.empty[String].Should be provided as string here like "abc,qwer,asd" "
  #selectColumns:  "defaulted to Seq.empty[String].Should be provided as string here like "abc,qwer,asd" "
  #filter:  "defaulted to empty string"
  #delimiter:  "defaulted to empty string"
  options:  "header->true,quote->\",escape->\",MultiLine->false,ignoreTrailingWhiteSpace-> true".defaulted to empty Map
  getFileName:  "true".Defaulted to false
  #abortOnZeroRecords:"false"

  }
  
      readInputKafka {
  inputEphemeralFilesFormat:  "json"
  templateLocation:  provide if inputEphemeralFilesFormat="json" to infer schema
  checkpointPath  provide the path
  dropColumns:  "defaulted to Seq.empty[String].Should be provided as string here like "abc,qwer,asd" "
  ephemeralFilePathPrefix:  "load_date="
  addKafkaTimestamp:  "true"
  #abortOnZeroRecords:"false"
  #handleEmptyTopic:"true"
  #fndTablePartitionColumn:"report_d"
  }

  #add filename as the last column in csvSchemaString if getFileName="true" in readInput
    validateSchema {
  csvSchemaString:  "Source string, CompanyName string,AccountId string, CampaignName string, CampaignId string,CampaignBudget string, CampaignStartDate string, CampaignEndDate string, ClassId string, DepartmentId string,VendorNumber string, TCIN string, CalendarYear string, CalendarMonth string, Revenue string,filename string"
  #templateLocation=provide if inputEphemeralFilesFormat="json"
  #csvSchemaString:  "Source string, CompanyName string,AccountId string, CampaignName string, CampaignId string,CampaignBudget string, CampaignStartDate string, CampaignEndDate string, ClassId string, DepartmentId string,VendorNumber string, TCIN string, CalendarYear string, CalendarMonth string, Revenue string,filename string,CampaignJoinDate string,CampaignJoinDateTs string,CampaignJoinDateLocalTs string,CampaignJoinDateUtclTs string"

  }

    flatten{
  flatten:  "false"

  }

      addFileDate{
  sourceDateFormat: "yyyy-MM-dd"
  sourceColumn: "filename"
  targetColumn: "file_date"

  }

    addDatePartitionColumn{
  isPartitionColumnDatatypeDate:  true
  sourceColumn:  "file_date"
  targetColumn:  "report_d"

  }

    getLatestByKeyCol{
  keyColName:  "report_d"
  orderByColName:  "file_date"
  windowingStrategy="row_number"

  }

    dropDups{
  location:  "/user/SVMDEDMD/landing/common_execute_pipeline/data_processing_pipeline/output/dqcorruptrecords/"
  partitionColumns:  "Source,CompanyName,AccountId,CampaignName ,CampaignId,CampaignStartDate,CampaignEndDate,ClassId,DepartmentId,VendorNumber,TCIN,CalendarYear,CalendarMonth"
  orderByColumn:  "CampaignStartDate"

  }

    validateNotNull{
  inputColumns:  "Source,CompanyName,AccountId,CampaignName ,CampaignId,CampaignStartDate,CampaignEndDate,ClassId,DepartmentId,VendorNumber,TCIN,CalendarYear,CalendarMonth"

  }

    validateDataType{
  inputColumns:  "{\"CampaignBudget\":\"decimal(38,2)\", \"Revenue\":\"decimal(38,8)\"}"

  }

    validateDateFormat{
  inputColumns:  "CampaignStartDate,CampaignEndDate"
  sourceDateFormat:  "yyyy-MM-dd"

  }

    removeDollarCharacter{
  inputColumns:  "CampaignBudget,Revenue"

  }

    replaceNullWithDefaultForAmtColumn{
  replaceNullWithDefaultForAmtColumns:  "CampaignBudget,Revenue"
  default:  "0.00"

  }

    replaceNullWithDefaultForQtyColumn{
  inputColumns:  ""
  default:  "0"

  }

    removeNonUtfCharacter{
  inputColumns:  ""

  }

    convertMicroToCurrency{

  inputColumns: "CampaignBudget,Revenue"

  }


    countByFileDateAndPartitionDate{
  partitionDateColumn: "report_d"
  fileDateColumn: "file_date"

  }


    handleNullsInPKColumns{
  inputColumns: """{ \"CampaignId\":\"123\"   ,
                                             \"CampaignName\":\"NA\"   ,
                                             \"CampaignStartDate\":\"9999-12-31\"
                                             }"""
  }


    convertDatePartOfTsToDefault{
  inputColumns: ""
  inputTsFormat: "MM/dd/yy HH:mm"

  }

    convertDateFormat{
  inputColumns:  "CampaignJoinDate"
  inputDateFormat:  "dd/MM/yyyy"
  outputDateFormat:  "yyyy-MM-dd"

  }


    convertTimestampToDate{
  inputColumns:  "CampaignJoinDateTs"
  outputFormat:  "yyyy-MM-dd"

  }

    convertTimestampFromLocalToUtc{
  inputColumns:  "CampaignJoinDateLocalTs"
  inputFormat:   "yyyy-MM-dd'T'HH:mm:ss.SSS"
  outputFormat:  "yyyy-MM-dd HH:mm:ss"
  timezone:  "America/Chicago"

  }

    convertTimestampFromUtcToLocal{
  inputColumns:  "CampaignJoinDateUtclTs"
  inputFormat:   "yyyy-MM-dd'T'HH:mm:ss.SSS"
  outputFormat:  "yyyy-MM-dd HH:mm:ss"
  timezone:  "America/Chicago"

  }

    joinCal{
  calendarColumns:  "fiscal_month_id,fiscal_month_n,fiscal_quarter_id ,fiscal_quarter_n, fiscal_year_id ,fiscal_year_week_id"
  calendarColumnsWithAnyTransformation:  "calendar_d,day_n,fiscal_week_n,fiscal_week_end_d,fiscal_week_begin_d,concat(fiscal_week_begin_d , '-' ,fiscal_week_end_d) calendar_fiscal_week,fiscal_month_id,fiscal_month_abbreviation_n  fiscal_month_n , fiscal_quarter_id , fiscal_quarter_n, fiscal_year_id ,fiscal_year_week_id"
  calendarTable:  "prd_cal_fnd.calendar"
  tempCalendarExtLocation:  "/user/SVMDEDMD/hive/common_execute_pipeline/calendar"
  inputJoinConditionColumn:  "report_d"
  calendarJoinConditionColumn: "calendar_d"

  }
  
    joinTwoDataFrames{
  joinTwoDataframesLeftAndRightDFName:  "dropDups,rename"
  joinType:  "left"
  joinCondition:  "report_d=calendar_date"
  prefixRightDFCommonCol:  "right"

  }

    rename {
  renameColumns:   """{ \"Source\":\"source_name\"   ,
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
                                        }"""

  }

    dqCheck{
  dqCheckSourceAndTargetDFName:  "dropDups,rename"
  dqColumns:   "campaign_budget_a , revenue_a ,report_d"
  dupsCheckKey:  "source_name,company_name,account_id,campaign_name ,campaign_id,campaign_budget_a,campaign_start_date,campaign_end_date,class_id,dept_id,vendor_number,tcin,calendar_year,calendar_month",
  nullsCheckKey:  "source_name,company_name,account_id,campaign_name ,campaign_id,campaign_start_date,campaign_end_date,class_id,dept_id,vendor_number,tcin,calendar_year,calendar_month"
  loggerName:  "CitrusInvoiceRevenueMonthly",
  reportingDateColumnFlag:  "true",
  srcColumnRenameList:  """{ \"Source\":\"source_name\"   ,
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
                                            }""",
  #tgtColumnRenameList:
  scale:  0,
  throwExceptionUponFailure:  "false",
  nullCheckFlag:  "true",
  dupCheckFlag:  "true",
  partitionDateColName:  "report_d",
  targetTablename:  "stg_mdf_fnd.common_execute_pipeline",
  #threshold
  #thresholdPercent
  #thresholdPercentJson
  ignoreCountCheck:  true,
  dqCheckResultShowToLog:  false

  }
  
      saveDFToHdfs{
    selectColumns: "oid,actiontrackerid,Amount,SKU,Quantity,Reason"
    filterCondition:  "reason_code <> 'NR'"
    hdfsTempFileLocation: "/user/SVMDEDMD/hive/impactradius_monthly_return/irmonthlyreturn/tmp"
    hdfsFileLocation: /user/SVMDEDMD/hive/impactradius_monthly_return/irmonthlyreturn/fileexpressinput
    hdfsSaveOptions: "header->true,sep->\\,,mapreduce.fileoutputcommitter.marksuccessfuljobs->false"
    hdfsSaveMode:  "overwrite"
    hdfsSaveFileFormat:  "csv"
    hdfsSaveCoalesce: 1
    coalesceInputSplit:500000
    hdfsRenameFilename: "IR_RECON_FINAL_TESTING.csv"
    hdfsRenameFileSearchPattern: ""
    deleteHdfsFileSearchPattern: "*"
    deleteHdfsFileRecursive: true  
    }

    transferFromHdfsToSftp{
    sftpPasswordJceKeyStorePath: ""
    sftpPasswordJceKeyName: ""
    sftpPrivateKeyPath:/user/SVMDEDMD/hive/privatekey/privatekey
    sftpUser: "mdf_impact_radius_to_fex_dst"
    sftpHost: "fileexpress-test.target.com"
    sftpPort: 22
    hdfsFileLocationPath: "/user/SVMDEDMD/hive/impactradius_monthly_return/irmonthlyreturn/fileexpressinput"
    hdfsFileRenameFlag: false
    hdfsFileRenameFileName: ""
    hdfsSearchFilePattern: ""
    copyMechanismType: "stream"
    sftpSleep: 60000
    sftpFileLocationPath:"/"      
    }

    transferFromSftpToHdfs {
    sftpPasswordJceKeyStorePath: ""
    sftpPasswordJceKeyName: ""
    sftpPrivateKeyPath:/user/SVMDEDMD/hive/privatekey/privatekey
    #sftpPrivateKeyPath:/user/SVMDEDMP/hive/privatekey/privatekey
    sftpUser: "mdf_impact_radius_to_fex_dst"
    sftpHost: "fileexpress-test.target.com"
    sftpPort: 22
    sftpFileLocationPath:"/"
    sftpFileRenameFileName: ""
    sftpSearchFilePattern: ""
    copyMechanismType: "stream"
    hdfsFileLocationPath:  "/user/SVMDEDMD/hive/impactradius_monthly_return/irmonthlyreturn/fileexpressinput"
    sftpInputSplit = 500000
    #if set to true will only send email no sftp.If false will sftp and and send email
    sftpSendEmailNoSftp = true
    isFileAlreadySftpCheck=true
    hdfsFileLocationToCheckIsFileAlreadySftpCheck="/Users/Z0030ZF/hive/criteo/filecheck.txt"
    fileCompletenessCheck=true
    }

    sendDFToEmail {
    customSqlFlag: true
    customSql: "select reason_code,count(*) count_of_records_sent_to_impact,sum(Amount)recon_amount_sent_to_impact,sum(return_sale_tran_a) return_sale_tran_a,sum(sale_tran_a) sale_tran_a,sum(SKU) recon_sku_sent_to_impact,  sum(return_sale_q) return_sale_q,sum(sale_q) sale_q from sftpInsertDF group by reason_code" 
    fromAddress: "SVMDEDMP"
    toAddress: "sudhindra.ramakrishna@target.com"
    reportFilename:IR_RECON_FINAL_TESTING.csv"
    emailSubject:Monthly recon counts sent to impact radius
    emailBodySubject:Monthly recon counts sent to impact radius
    attachDFResult:false
    attachDFResultToEmailBody:true
    debug: "false"
    }

    archiveArchivePartition{
  deleteSource:  "false"
  }

    archiveEphemeral{
  deleteSource:  "true"

  }

    atomicHistLoad{
  inputDFNameFromExecutionOrder:  "addFileDate",
  insertColumnNames: "Source,CompanyName , AccountId ,CampaignName , CampaignId , CampaignBudget ,CampaignStartDate , CampaignEndDate , DepartmentId , VendorNumber , TCIN ,CalendarYear , CalendarMonth , Revenue",
  partitionColumns:  "file_date",
  repartition: true
  }

    fndLoad{
  inputDFNameFromExecutionOrder:  "rename"
  insertColumnNames:  """source_name,company_name ,
                                    account_id ,
                                    campaign_name,
                                    campaign_id ,
                                    campaign_budget_a ,
                                    campaign_start_date ,
                                    campaign_end_date ,
                                    class_id ,
                                    dept_id ,
                                    vendor_number ,
                                    tcin ,
                                    revenue_a ,
                                    file_date,
                                    calendar_year ,
                                    calendar_month,
                                    """,
  partitionColumns:  "calendar_year,calendar_month",
  repartition: true

  }

      deleteTriggerFile{
  doneFile: ""
  }
```