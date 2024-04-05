package com.tgt.dse.mdf.common.pipeline.constants

import java.time.format.DateTimeFormatter

object MarketingConstants {

  //Application variables
  val APP_ENV: String = "app.env"
  val APP_NAME: String = "app.name" //application name
  val APP_BLOSSOMID: String = "app.blossomID" //blossom id of the application to identify and measure metrics
  val APP_TEAMNAME: String = "app.teamName" //Owner of the pipeline for reaching out
  val APP_CONTACTDETAILS: String = "app.contactDetails" //Primary contact person of the pipeline
  val APP_CHANNEL: String = "app.channel"
  val APP_ERRCHANNEL:String = "app.errChannel"
  val APP_DQTHRESHOLD:String = "app.dataportal.dqThreshold"
  val APP_TEMPLATELOCATION: String = "app.templateLocation" //path of template to validate structure and find any missing or extra fields
  val APP_INPUTLOCATION: String = "app.inputLocation" //input location of messages to be read
  val APP_FILEAPILANDINGLOCATION: String = "app.fileAPILandingLocation" //input location of messages to be read
  val APP_CURRUPTRECORDLOCATION: String = "app.corruptRecordLocation"
  val APP_PIPELINEFAILURES: String = "app.pipeLineFailures" //location for dataTypeFailures and validationFailures
  val APP_EXISTINGRECORDSLOCATION: String = "app.existingRecordsLocation" //location having existing dataset which needs to be updated
  val APP_ARCHIVEPARTITIONRECORDSLOCATION : String = "app.archivePartitonRecordsLocation"
  val APP_LOCKLOCATION: String = "app.lockLocation" //Location where lock will be created for the execution of job
  val COALESCEFACTOR: String = "app.coalesceFactor" //After join data would have got spilled. Coalesce them to reduce partitions
  val APP_numberOfPartitions: String = "app.numberOfPartitions" //the max partitions allowed to be written into the path provided (DIP check point)
  val APP_TEMPFNDEXTLOCATION: String = "app.tempFNDExtLocation"
  val APP_FNDEXTLOCATION: String = "app.fndExtLocation"
  val APP_DATEPARMLOCATION: String = "app.dateparmLocation"
  val APP_CALENDARLOCATION: String = "app.calednarLocation"
  val FILEFORMAT: String = "app.fileFormat"
  val APP_SLACKWEBHOOK = "app.slackWebHook"
  val APP_DQCHANNELID = "app.dq_channelID"
  val APP_ALERTCHANNELID = "app.alert_channelID"
  val APP_DATAQUALITYLOCATION: String = "app.dataQualityLocation"
  val APP_MAXPARTITIONDATELAGINDAYS = "app.maxPartitionDateLagInDays"
  val APP_PUSHCUSTOMINFLUXMETRIC = "app.pushCustomInfluxMetric"
  val APP_SETPIPELINEMETADATACUSTOMINFLUXMETRIC = "app.setPipeLineMetadataInfluxMetric"
  val APP_INFLUXCUSTOMMEASUREMENTNAME= "app.influxCustomMeasurementName"
  val APP_TABLEISEXTERNAL = "app.tableIsExternal"
  val APP_READALPACA = "app.readAlpaca"
  val APP_CHECKPOINTPATH: String = "app.checkPointPath" //last processed load date partition
  val APP_TEMPFNDREADLOCATION: String = "app.tempFNDReadLocation"

  //Configuration validations
  val CONFIG_SEQ: Seq[String] = Seq(
    APP_NAME,
    APP_BLOSSOMID,
    APP_TEAMNAME,
    APP_CONTACTDETAILS,
    APP_INPUTLOCATION,
    // APP_TEMPLATELOCATION,
    APP_PIPELINEFAILURES,
    APP_EXISTINGRECORDSLOCATION,
    APP_LOCKLOCATION
  )

  //Session specific constants
  val SPARK_SQL_SOURCES_PARTITIONOVERWRITEMODE: String = "spark.sql.sources.partitionOverwriteMode"
  /*("When INSERT OVERWRITE a partitioned data source table, we currently support 2 modes: " +
    "static and dynamic. In static mode, Spark deletes all the partitions that match the " +
    "partition specification(e.g. PARTITION(a=1,b)) in the INSERT statement, before " +
    "overwriting. In dynamic mode, Spark doesn't delete partitions ahead, and only overwrite " +
    "those partitions that have data written into it at runtime. By default we use static " +
    "mode to keep the same behavior of Spark prior to 2.3. Note that this config doesn't " +
    "affect Hive serde tables, as they are always overwritten with dynamic mode.")*/
  val HIVE_EXEC_DYNAMIC_PARTITION: String = "hive.exec.dynamic.partition"
  val HIVE_EXEC_DYNAMIC_PARTITION_MODE: String = "hive.exec.dynamic.partition.mode"
  val SPARK_SQL_ORC_IMPL: String = "spark.sql.orc.impl"
  val SPARK_SQL_ORC_ENABLEVECTORIZEDREADER: String = "spark.sql.orc.enableVectorizedReader"
  val SPARK_SQL_WAREHOUSE_DIR: String = "spark.sql.warehouse.dir"
  val HIVE_EXEC_MAX_DYNAMIC_PARTITIONS: String = "hive.exec.max.dynamic.partitions"
  val SPARK_SOURCES_PARTITIONOVERWRITEMODE: String = "app.spark_sources_partitionOverwriteMode"
  val HIVE_PARTITION: String = "true"
  val HIVE_PARTITION_MODE: String = "nonstrict"
  val SPARK_ORC_IMPL: String = "app.spark_orc_impl"
  val SPARK_ENABLEVECTORIZEDREADER: String = "app.spark_enableVectorizedReader"
  val SPARK_WAREHOUSELOCATION: String = "app.spark_wareHouseLocation"
  val HIVE_MAX_DYNAMIC_PARTITIONS: String = "app.hive_max_dynamic_partitions"
  val SPARK_MAX_TO_STRING_FIELDS: String = "spark.sql.debug.maxToStringFields"
  val SPARK_SHUFFLE_PARTITIONS: String = "spark.sql.shuffle.partitions"
  val ONE_HUNDRED: String = "100"

  //Spark related parameters
  val SPARK_EXECUTOR_INSTANCES = "spark.executor.instances"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_DRIVER_MEMORY = "spark.driver.memory"
  val SPARK_YARN_QUEUE = "spark.yarn.queue"
  val SPARK_APP_ID = "spark.app.id"

  val APPLICATION_ID = "applicationId" // used as metric key,applicationId of the spark Application for tracking
  val TOTAL_EXECUTORS = "totalExecutors" // used as metric key,number of executors configured
  val EXECUTOR_CORES = "executorCores" // used as metric key,number of cores per executor
  val EXECUTOR_MEMORY = "executorMemory" // used as metric key,executor memory configured
  val DRIVER_MEMORY = "driverMemory" // used as metric key,driver memory configured
  val QUEUE_NAME = "queueName" //used as metric key,queuename
  val TOPIC_NAME = "topicName"
  val EPHEMERAL_FILE_COUNT = "ephemeral_fileCount"
  val MAX_KAFKA_MESSAGES = "maxKafkaMessages"
  val COMPACT_ATOMIC_COALESCE = "compactAtomicCoalesce"
  val WORKFLOW_ID = "workflowID" //workflow id of the application if the job is scheduled from oozie
  val VERSION = "Version" //Code version
  val CORE_VERSION = "CoreVersion" //Code version for core artifact

  // Hive DB names
  val APP_calDateDimSchema : String = "app.calDateDimSchema"
  val APP_locationSchema :String = "app.locationSchema"
  val APP_strTasksEventsSchema:String ="app.strTasksEventsSchema"
  val APP_strTasksEventsSTGSchema : String = "app.strTasksEventsSTGSchema"
  val APP_FNDSCHEMA: String = "app.fndSchema"
  val APP_STGSCHEMA: String = "app.stgSchema"
  val APP_LNDSCHEMA: String = "app.lndSchema"
  val APP_WRKSCHEMA: String = "app.wrkSchema"
  val APP_QUEUENAME: String = "app.queueName"
  val APP_EXECUTIONENGINE: String = "app.executionEngine"
  val APP_DYNAMICPARTITION: String = "app.dynamicPartition"
  val APP_DYNAMICPARTITIONMODE: String = "app.dynamicPartitionMode"
  val APP_JDBCURL: String = "app.jdbcUrl"
  val APP_CHLOCPATH: String = "app.coreHistLocationPath"
  val APP_HIVETEZINPUTFORMAT: String ="app.hiveTezInputFormat"
  val APP_MAPREDUCEJOBREDUCES: String ="app.mapReduceJobReduces"
  val APP_JDBCHIVEDRIVER: String = "org.apache.hive.jdbc.HiveDriver"
  val DEFAULT_BR3_JDBC_URL: String = "jdbc:hive2://brcn1003.target.com:2181,brcn1004.target.com:2181,brcn1008.target.com:2181," +
    "brcn1009.target.com:2181,brcn1012.target.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

  //Table names
  val APP_calDateDimTable :String = "app.calDateDimTable"
  val APP_locationTable :String = "app.locationTable"
  val APP_lndCoreHistTable: String = "app.lndCoreHistTable"
  val APP_stgCoreHistTable: String = "app.stgCoreHistTable"
  val APP_coreHistTable: String = "app.coreHistTable"
  val APP_fndExtTable: String = "app.fndExtTable"
  val APP_lndExtTable: String = "app.lndExtTable"
  val APP_coreHistPartitionColumnName : String = "app.coreHistPartitionColumnName"
  val APP_atomicHistPartitionColumnName : String = "app.atomicHistPartitionColumnName"

  //stages of Pipeline
  val LOADEPHEMERAL: String = "Load Ephemeral" //used as metric stage value
  val VALIDATEDATATYPES: String = "ValidateDataTypes" //used as metric stage value
  val APPLYVALIDATIONS: String = "ApplyValidations" //used as metric stage value
  val REMOVEDUPLICATES: String = "RemoveDuplicates" //used as metric stage value
  val TRANSFORMATION: String = "TransFormation" //used as metric stage value
  val DECORATION: String = "Decoration" //used as metric stage value
  val EXPLODE: String = "Explode" //used as metric stage value
  val VALIDATE_CONTRACT: String = "ValidateContract" //used as metric stage value
  val UPSERT: String = "Upsert" //used as metric stage value
  val SUCCESS: String = "SUCCESS" //used as metric final status value
  val FAILURE: String = "FAILURE" //used as metric final status value
  val Success_Camel: String = "Success"
  val APPEND: String = "append" //File write mode
  val FILE_FORMAT: String = "orc" // File format
  val JSON_FORMAT: String = "json" // File format
  val IS_ROW_VALID: String = "is_row_valid_datatypecastvalidator" //For filtering valid records messages
  val ACTION_VALIDATION_FAILED: String = "action validation failed"
  val SUB_ACTION_VALIDATION_FAILED: String = "sub action validation failed"
  val VALIDATENOTNULLCOLUMNS: String = "ValidateNOTNULLColumns" //used as metric stage value
  val INVALIDNOTNULLROWS: String = "invalidNotNullRows" //used as metric key
  val FINALINSERTOVERWRITE: String = "FinalInsertOverwrite" //used as metric stage value


  //Metric keys
  val DETAILS: String = "details" //used as metric key
  val ERRORMESSAGE: String = "errorMessage" //used as metric key
  val STAGE: String = "stage" //used as metric key
  val MISSINGCOLLSOURCE: String = "missingCollSource" //used as metric key
  val EXTRACOLLSOURCE: String = "extraCollSource" //used as metric key
  val EXPLODEDINPUTROWS: String = "explodedInputRows" //used as metric key
  val INVALIDDATATYPEROWS: String = "invalidDataTypeRows" //used as metric key
  val VALIDATIONFAILEDROWS: String = "validationFailedRows" //used as metric key
  val DUPLICATEDROWS: String = "duplicatedRows" //used as metric key
  val FINALOUTPUTROWS: String = "finalOutputRows" //used as metric key
  val FINALMERGEROWS: String = "finalMergeRows" //used as metric key
  val INVALIDINPUTMESSAGECOUNT: String = "invalidInputMessageCount" //used as metric key
  val CORRUPTROWS: String = "corruptRows" //used as metric key
  val _CORRUPT_RECORD: String = "_corrupt_record"
  //val INPUTROWCOUNT: String = "inputRowCount" //used as metric key
  val LOADCOMPLETIONTIME: String = "loadCompletionTime"
  val APP_SLA: String = "app.sla"

  //join types
  val LEFT: String = "left" //used for table join
  val COLNAME_ROW_NO = "row_number"

  //Sub folders to be created under the failures of pipeline
  val DATATYPEFAILURES: String = "dataTypeFailures" //folder name for dataTypeFailures
  val VALIDATIONFAILURES: String = "validationFailures" //folder name for validation failures
  val PIPELINEREJECTEDRECORDS: String = "pipelineRejectedRecords" //folder name for pipeline rejected records
  val CORRUPTRECORDS: String = "corruptRecords" //Invalid json messages
  val NULLOREMPTYRECORDS: String = "nullOrEmptyRecords"

  //Grafana metrics
  // influx metric push parameters
  val APP_METRICS_KAFKA_PUSH: String = "app.metrics.metricsKafkaPush"
  val APP_METRICS_INFLUX_PUSH: String = "app.metrics.metricsInfluxPush" // enable influx push to metrics
  val APP_INFLUXDB_MEASUREMENTNAME: String = "app.influxdb.measurementName" // dashboard name
  val APP_INFLUXDB_GRAFANAENDPOINT: String = "app.influxdb.grafanaEndPoint" // influx endpoint to be used for metric push
  val DATAFRESHNESS = "dataFreshness"
  val JOB_SLA = "APP_SLA"
  val _BLOSSOM_ID="_blossom_id"
  val MARKETING_CONSTANTS: String = "MARKETING_CONSTANTS :"
  val INFLUX_METRICS_EXT_TABLENAME="custom_influx_metrics"
  val INFLUX_METRICS_EXT_TABLE_INSERT_COLUMN_NAMES = Seq("lastrun","maxpartition","countofmaxpartition","sla","maxpartitiondatelagindays","lastjobstartdatetime","lastjobenddatetime","lastjobprocessingtimeinseconds","tablename")
  val INFLUX_METRICS_EXT_TABLE_PARTITION_COLUMN_NAMES = Seq("tablename")

  //ApplicationUtil
  val DATE_FORMAT_WITH_ZONE_FOR_DATAPORTAL: String = "yyyy-MM-dd HH:mm:ss z"
  val TIME_ZONE: String = "UTC"

  //timeliness event
  val EVENT_CLASS: String = "dataMovementTimeliness"
  val MESSAGE_TABLE: String = "PROCESSING DATA"
  val MESSAGE_WF: String = "COMPLETED"
  val EVENT_STATUS: String = "COMPLETE"
  val EVENT_TYPE: String = "TIMELINESS"
  val DATEFORMATTERFORTIMELINESS: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val DATEFORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")
  val DATE_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val US_CENTRAL_ZONE: String="US/Central"
  val JOBSTARTTIME: String = "00:00:00 UTC"
  val SLA: String="09:00 AM"

  //potpourri
  val NULL: String = ""
  val status : String = "status"
  val MICRO_CURRENCY_CONVERSION_DENOMINATOR: Int = 1000000

  //Contract validation metrics
  val DATATYPE_MISMATCH_SOURCE: String = "DataTypeMismatchSource"
  val NULLABLE_MISMATCH_SOURCE: String = "NullableMismatchSource"
  val NULLABLE_MISMATCH_ROWS_COUNT: String = "NullableMismatchRowsCount"

  //Decimal Precision and Scale Global Defaults
  val DECIMAL_PRECISION = 38
  val DECIMAL_SCALE = 18

  //SlackPublish constants
  val WEBHOOK_URL = "https://api.target.com/slack_events/v1/webhooks/"
  val TEXT_WITHQUOTES: String= "\"text\""
  val OPEN_BRACES        = "{"
  val COLON              = ":"
  val QUOTES             = "\""
  val CLOSE_BRACES       = "}"
  val POST = "POST"
  val CONTENT_TYPE = "Content-Type"
  val APPLICATION_JSON = "application/json"

  //DataQualityPublisher Constants
  val DIFF_IMPRESSIONS = "srcTgtDiffImpressions"
  val DIFF_SPEND = "srcTgtDiffSpend"
  val DIFF_CLICKS = "srcTgtDiffClicks"
  val TOTAL_IMPRESSIONS = "TotalImpressions"
  val TOTAL_SPEND = "TotalSpend"
  val TOTAL_CLICKS = "TotalClicks"
  val NULL_COUNT = "nullRowsCount"
  val DUPLICATE_COUNT = "duplicateRowsCount"

  //Producing Timeliness
  val oozieURL = "http://bigred3oozie.target.com:11000/oozie"
  val SSL_KEYSTORE_PASSWORD = "ssl.keystore.password"
  val SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password"
  val SECURITY_PROTOCOL = "security.protocol"
  val SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location"
  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "ssl.endpoint.identification.algorithm"
  val VALUE_SERIALIZER = "value.serializer"
  val SSL_KEYSTORE_LOCATION = "ssl.keystore.location"
  val KEY_SERIALIZER = "key.serializer"
  val CLIENT_ID = "client.id"
  val SSL_PROTOCOL = "ssl.protocol"

  val TIMELINESS_DP_KAFKA_KEY_STORE = "mdf_keystore_password"
  val TIMELINESS_DP_KAFKA_TRUST_STORE = "mdf_truststore_password"
  val JCEKS_FILE_PATH = "jceks://file/home_dir/svmdedmp/ssl_certs/mdf_secrets.jceks"
  val APP_CONF_JCEKS_FILE_PATH = "app.timeliness.jceksFilePath"
  val TIMELINESS_BOOTSTRAP_SERVERS = "kafka-ttc-app.prod.target.com:9093"
  val TIMELINESS_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
  val TIMELINESS_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
  val TIMELINESS_SSL_TRUSTSTORE_LOCATION = "/home_dir/svmdedmp/ssl_certs/client.truststore.jks"
  val APP_CONF_TRUSTSTORE_FILE_PATH = "app.timeliness.truststoreFilePath"
  val TIMELINESS_SSL_KEYSTORE_LOCATION = "/home_dir/svmdedmp/ssl_certs/mdf-prd.target.com.jks"
  val APP_CONF_KEYSTORE_FILE_PATH = "app.timeliness.keystoreFilePath"
  val TIMELINESS_SECURITY_PROTOCOL = "SSL"
  val TIMELINESS_SSL_PROTOCOL = "TLSv1.2"
  val TIMELINESS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = ""
  val TIMELINESS_TOPIC_NAME = "kelsa-dataportal"

  //External table defaults
  val REPORT_D: String = "report_d"
  val LOCAL_ENV: String = "local"
  val DEFAULT_PARTITION_COLUMNS: Seq[String] = Seq(REPORT_D)
  val WRITE_RETRY_COUNT: Int = 3
  val WRITE_RETRY_SLEEP: Long = 3 * 1000

  //sftp
  val APP_SFTPPASSWORDJCEKEYNAME = "app.sftpPasswordJceKeyName"
  val APP_SFTPPASSWORDJCEKEYSTOREPATH = "app.sftpPasswordJceKeyStorePath"
  val APP_SFTPPRIVATEKEYPATH =  "app.sftpPrivateKeyPath"
  val APP_SFTPUSER =  "app.sftpUser"
  val APP_SFTPHOST =  "app.sftpHost"
  val APP_SFTPPORT =  "app.sftpPort"
  val APP_SFTPFILELOCATIONPATH = "app.sftpFileLocationPath"
  val APP_HDFSFILELOCATIONPATH = "app.hdfsFileLocationPath"
  val APP_HDFSSEARCHFILEPATTERN = "app.hdfsSearchFilePattern"
  val APP_SFTPSEARCHFILEPATTERN = "app.sftpSearchFilePattern"
  val APP_SFTPSLEEP = "app.sftpSleep"
  val APP_HDFSFILERENAMEFILENAME = "app.hdfsFileRenameFileName"
  val APP_COPYMECHANISMTYPE = "app.copyMechanismType"
  val APP_HDFSFILERENAMEFLAG = "app.hdfsFileRenameFlag"
  val APP_SFTPFILERENAMEFILENAME="app.sftpFileRenameFileName"
  val APP_FILECOMPLETENESSCHECK="app.fileCompletenessCheck"
  val APP_RETRIESFORFILECOMPLETENESSCHECK="app.retriesForFileCompletenessCheck"
  val APP_SLEEPFORFILECOMPLETENESSCHECK="app.sleepForFileCompletenessCheck"
  val APP_ISFILEALREADYSFTPCHECK="app.isFileAlreadySftpCheck"
  val APP_HDFSFILELOCATIONTOCHECKISFILEALREADYSFTPCHECK="app.hdfsFileLocationToCheckIsFileAlreadySftpCheck"

}
