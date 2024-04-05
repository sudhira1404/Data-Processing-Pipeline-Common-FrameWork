package com.tgt.dse.mdf.common.pipeline.service

import com.tgt.dsc.kelsa.datapipeline.core.transformations.{FlattenFunctions, TransFunctions}
import com.tgt.dse.mdf.common.pipeline.MarketingTestHelpers
import com.tgt.dse.mdf.common.pipeline.exceptions.DateSequenceException
import com.tgt.dse.mdf.common.pipeline.util.{DataFrameUtils, DateTimeUtils}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.{Failure, Success, Try}

class BuildAllPartitionsFromEarliestPartitionReadTest extends AnyFlatSpec with MarketingTestHelpers {

  // Columns
  val CAMPAIGN = "campaign"
  val BLUEPRINT_ID = "blueprint_id"
  val CAMPAIGN_ID = "campaign_id"
  val CAMPAIGN_NAME = "campaign_name"
  val CAMPAIGN_TYPE = "campaign_type"
  val CREATED_BY = "created_by"
  val CREATED_DATE = "created_date"
  val CREATIVE_APPROVAL_LEVEL = "creative_approval_level"
  val IS_MIGRATED = "is_migrated"
  val MILESTONE = "milestone"
  val MILESTONE_MILESTONES_ASSIGNED_TO = "milestone_milestones_assigned_to"
  val MILESTONE_MILESTONES_AUTO_CHECK = "milestone_milestones_auto_check"
  val MILESTONE_MILESTONES_COMPLETED_BY = "milestone_milestones_completed_by"
  val MILESTONE_MILESTONES_COMPLETED_DATE = "milestone_milestones_completed_date"
  val MILESTONE_MILESTONES_COMPLETION_LEVEL = "milestone_milestones_completion_level"
  val MILESTONE_MILESTONES_CREATED_BY = "milestone_milestones_created_by"
  val MILESTONE_MILESTONES_CREATED_DATE = "milestone_milestones_created_date"
  val MILESTONE_MILESTONES_CRITICAL = "milestone_milestones_critical"
  val MILESTONE_MILESTONES_CUSTOM_MILESTONE = "milestone_milestones_custom_milestone"
  val MILESTONE_MILESTONES_DAYS_OUT = "milestone_milestones_days_out"
  val MILESTONE_MILESTONES_DEPENDS_ON = "milestone_milestones_depends_on"
  val MILESTONE_MILESTONES_DISABLED = "milestone_milestones_disabled"
  val MILESTONE_MILESTONES_DUE_DATE = "milestone_milestones_due_date"
  val MILESTONE_MILESTONES_INFORMATION_ONLY = "milestone_milestones_information_only"
  val MILESTONE_MILESTONES_MILESTONE_ID = "milestone_milestones_milestone_id"
  val MILESTONE_MILESTONES_NAME = "milestone_milestones_name"
  val MILESTONE_MILESTONES_NOTES = "milestone_milestones_notes"
  val MILESTONE_MILESTONES_PROJECT_PHASE = "milestone_milestones_project_phase"
  val MILESTONE_MILESTONES_RISK = "milestone_milestones_risk"
  val MILESTONE_MILESTONES_RISK_DAYS_OUT = "milestone_milestones_risk_days_out"
  val MILESTONE_MILESTONES_ROLE = "milestone_milestones_role"
  val MILESTONE_MILESTONES_STATUS = "milestone_milestones_status"
  val MILESTONE_MILESTONES_STATUS_DESCRIPTION = "milestone_milestones_status_description"
  val MILESTONE_MILESTONES_UPDATED_BY = "milestone_milestones_updated_by"
  val MILESTONE_MILESTONES_UPDATED_DATE = "milestone_milestones_updated_date"
  val MILESTONE_SCHEDULE_TYPE = "milestone_schedule_type"
  val MILESTONE_TEMPLATE_NAME = "milestone_template_name"
  val MILESTONE_UPDATED_BY = "milestone_updated_by"
  val NAME = "name"
  val NOTES_AUTHOR = "notes_author"
  val NOTES_CONTENT = "notes_content"
  val NOTES_ID = "notes_id"
  val NOTES_TIMESTAMP = "notes_timestamp"
  val PLANNED_BUDGET = "planned_budget"
  val ROSTER_TITLE = "roster_title"
  val ROSTER_TYPE = "roster_type"
  val ROSTER_USERS_EMAIL = "roster_users_email"
  val ROSTER_USERS_ID = "roster_users_id"
  val ROSTER_USERS_NAME = "roster_users_name"
  val ROSTER_USERS_ROLES = "roster_users_roles"
  val SET_DATE = "set_date"
  val SIGN_VOLUME_ESTIMATE = "sign_volume_estimate"
  val STATUS = "status"
  val TIER_ONE = "tier_one"
  val TYPE = "type"
  val UPDATED_BY = "updated_by"
  val UPDATED_DATE = "updated_date"
  val VERSION_COMPLEXITY = "version_complexity"
  val PYRAMIDS = "pyramids"
  //Kafka metadata fields
  val TIMESTAMP = "timestamp"
  val KEY = "key"
  val PARTITION = "partition"
  val TIMESTAMP_TYPE = "timestampType"
  val TOPIC = "topic"
  val VALUE = "value"
  //Fields derived while ingesting Kafka metadata fields
  val HIVE_LOAD_DATE = "report_d"
  val VALUE_JSON = "value_json"
  val HIVE_LOAD_TIMESTAMP = "hive_load_timestamp"
  // Renamed columns
  val CREATED_UTC_TS = "created_utc_ts"
  val MILESTONE_ASSIGNED_TO = "milestone_assigned_to_arr"
  val MILESTONE_COMPLETED_BY = "milestone_completed_by"
  val MILESTONE_COMPLETED_UTC_TS = "milestone_completed_utc_ts"
  val MILESTONE_COMPLETION_LEVEL = "milestone_completion_level"
  val MILESTONE_CREATED_BY = "milestone_created_by"
  val MILESTONE_CREATED_UTC_TS = "milestone_created_utc_ts"
  val MILESTONE_CRITICAL_F = "milestone_critical_f"
  val MILESTONE_DAYS_OUT = "milestone_days_out"
  val MILESTONE_DISABLED_F = "milestone_disabled_f"
  val MILESTONE_DUE_DATE_UTC_TS = "milestone_due_date_utc_ts"
  val MILESTONE_ID = "milestone_id"
  val MILESTONE_NAME = "milestone_name"
  val MILESTONE_RISK_F = "milestone_risk_f"
  val MILESTONE_RISK_DAYS_OUT = "milestone_risk_days_out"
  val MILESTONE_ROLE = "milestone_role"
  val MILESTONE_STATUS = "milestone_status"
  val MILESTONE_STATUS_DESCRIPTION = "milestone_status_description"
  val MILESTONE_UPDATED_UTC_TS = "milestone_updated_utc_ts"
  val BLUEPRINT_NAME = "blueprint_name"
  val SET_DATE_UTC_TS = "set_date_utc_ts"
  val BLUEPRINT_STATUS = "blueprint_status"
  val TIER_ONE_F = "tier_one_f"
  val BLUEPRINT_TYPE = "blueprint_type"
  val UPDATED_UTC_TS = "updated_utc_ts"

  val PARTITION_COLUMNS: Seq[String] = Seq(HIVE_LOAD_DATE)

  val INPUT_SELECT_COLUMNS: Seq[String] = Seq(
    UPDATED_BY,
    CAMPAIGN,
    TIER_ONE,
    CREATED_BY,
    CREATIVE_APPROVAL_LEVEL,
    CREATED_DATE,
    SIGN_VOLUME_ESTIMATE,
    IS_MIGRATED,
    NAME,
    PLANNED_BUDGET,
    MILESTONE,
    SET_DATE,
    UPDATED_DATE,
    STATUS,
    TYPE,
    BLUEPRINT_ID,
    VERSION_COMPLEXITY,
    HIVE_LOAD_DATE,
    HIVE_LOAD_TIMESTAMP)

  val FOUNDATION_SCHEMA: StructType = StructType(Seq(
    StructField(BLUEPRINT_ID, StringType, nullable = false),
    StructField(CAMPAIGN_ID, StringType, nullable = true),
    StructField(CAMPAIGN_NAME, StringType, nullable = true),
    StructField(CAMPAIGN_TYPE, StringType, nullable = true),
    StructField(CREATED_BY, StringType, nullable = true),
    StructField(CREATED_UTC_TS, TimestampType, nullable = true),
    StructField(MILESTONE_ASSIGNED_TO, ArrayType(StringType), nullable = true),
    StructField(MILESTONE_COMPLETED_BY, StringType, nullable = true),
    StructField(MILESTONE_COMPLETED_UTC_TS, TimestampType, nullable = true),
    StructField(MILESTONE_COMPLETION_LEVEL, StringType, nullable = true),
    StructField(MILESTONE_CREATED_BY, StringType, nullable = true),
    StructField(MILESTONE_CREATED_UTC_TS, TimestampType, nullable = true),
    StructField(MILESTONE_CRITICAL_F, BooleanType, nullable = true),
    StructField(MILESTONE_DAYS_OUT, LongType, nullable = true),
    StructField(MILESTONE_DISABLED_F, BooleanType, nullable = true),
    StructField(MILESTONE_DUE_DATE_UTC_TS, TimestampType, nullable = true),
    StructField(MILESTONE_ID, StringType, nullable = false),
    StructField(MILESTONE_NAME, StringType, nullable = true),
    StructField(MILESTONE_RISK_F, BooleanType, nullable = true),
    StructField(MILESTONE_RISK_DAYS_OUT, LongType, nullable = true),
    StructField(MILESTONE_ROLE, StringType, nullable = true),
    StructField(MILESTONE_STATUS, StringType, nullable = true),
    StructField(MILESTONE_STATUS_DESCRIPTION, StringType, nullable = true),
    StructField(MILESTONE_UPDATED_BY, StringType, nullable = true),
    StructField(MILESTONE_UPDATED_UTC_TS, TimestampType, nullable = true),
    StructField(BLUEPRINT_NAME, StringType, nullable = true),
    StructField(PLANNED_BUDGET, DoubleType, nullable = true),
    StructField(SET_DATE_UTC_TS, TimestampType, nullable = true),
    StructField(SIGN_VOLUME_ESTIMATE, StringType, nullable = true),
    StructField(BLUEPRINT_STATUS, StringType, nullable = true),
    StructField(TIER_ONE_F, BooleanType, nullable = true),
    StructField(BLUEPRINT_TYPE, StringType, nullable = true),
    StructField(UPDATED_BY, StringType, nullable = true),
    StructField(UPDATED_UTC_TS, TimestampType, nullable = true),
    StructField(HIVE_LOAD_DATE, DateType, nullable = false),
    StructField(HIVE_LOAD_TIMESTAMP, TimestampType, nullable = false)
  ))

  val FINAL_COLUMN_NAMES: Seq[String] = Seq(
    BLUEPRINT_ID,
    CAMPAIGN_ID,
    CAMPAIGN_NAME,
    CAMPAIGN_TYPE,
    CREATED_BY,
    CREATED_UTC_TS,
    MILESTONE_ASSIGNED_TO,
    MILESTONE_COMPLETED_BY,
    MILESTONE_COMPLETED_UTC_TS,
    MILESTONE_COMPLETION_LEVEL,
    MILESTONE_CREATED_BY,
    MILESTONE_CREATED_UTC_TS,
    MILESTONE_CRITICAL_F,
    MILESTONE_DAYS_OUT,
    MILESTONE_DISABLED_F,
    MILESTONE_DUE_DATE_UTC_TS,
    MILESTONE_ID,
    MILESTONE_NAME,
    MILESTONE_RISK_F,
    MILESTONE_RISK_DAYS_OUT,
    MILESTONE_ROLE,
    MILESTONE_STATUS,
    MILESTONE_STATUS_DESCRIPTION,
    MILESTONE_UPDATED_BY,
    MILESTONE_UPDATED_UTC_TS,
    BLUEPRINT_NAME,
    PLANNED_BUDGET,
    SET_DATE_UTC_TS,
    SIGN_VOLUME_ESTIMATE,
    BLUEPRINT_STATUS,
    TIER_ONE_F,
    BLUEPRINT_TYPE,
    UPDATED_BY,
    UPDATED_UTC_TS,
    HIVE_LOAD_DATE)

  val COLUMNS_TO_RENAME: Seq[(String, String)] = Seq(
    (CREATED_DATE, CREATED_UTC_TS),
    (MILESTONE_MILESTONES_ASSIGNED_TO, MILESTONE_ASSIGNED_TO),
    (MILESTONE_MILESTONES_COMPLETED_BY, MILESTONE_COMPLETED_BY),
    (MILESTONE_MILESTONES_COMPLETED_DATE, MILESTONE_COMPLETED_UTC_TS),
    (MILESTONE_MILESTONES_COMPLETION_LEVEL, MILESTONE_COMPLETION_LEVEL),
    (MILESTONE_MILESTONES_CREATED_BY, MILESTONE_CREATED_BY),
    (MILESTONE_MILESTONES_CREATED_DATE, MILESTONE_CREATED_UTC_TS),
    (MILESTONE_MILESTONES_CRITICAL, MILESTONE_CRITICAL_F),
    (MILESTONE_MILESTONES_DAYS_OUT, MILESTONE_DAYS_OUT),
    (MILESTONE_MILESTONES_DISABLED, MILESTONE_DISABLED_F),
    (MILESTONE_MILESTONES_DUE_DATE, MILESTONE_DUE_DATE_UTC_TS),
    (MILESTONE_MILESTONES_MILESTONE_ID, MILESTONE_ID),
    (MILESTONE_MILESTONES_NAME, MILESTONE_NAME),
    (MILESTONE_MILESTONES_RISK, MILESTONE_RISK_F),
    (MILESTONE_MILESTONES_RISK_DAYS_OUT, MILESTONE_RISK_DAYS_OUT),
    (MILESTONE_MILESTONES_ROLE, MILESTONE_ROLE),
    (MILESTONE_MILESTONES_STATUS, MILESTONE_STATUS),
    (MILESTONE_MILESTONES_STATUS_DESCRIPTION, MILESTONE_STATUS_DESCRIPTION),
    (MILESTONE_MILESTONES_UPDATED_BY, MILESTONE_UPDATED_BY),
    (MILESTONE_MILESTONES_UPDATED_DATE, MILESTONE_UPDATED_UTC_TS),
    (NAME, BLUEPRINT_NAME),
    (STATUS, BLUEPRINT_STATUS),
    (TYPE, BLUEPRINT_TYPE),
    (SET_DATE, SET_DATE_UTC_TS),
    (TIER_ONE, TIER_ONE_F),
    (UPDATED_DATE, UPDATED_UTC_TS)
  )

  val TIMESTAMP_CONVERSION_COLUMNS: Seq[String] = Seq(
    CREATED_DATE,
    UPDATED_DATE,
    MILESTONE_MILESTONES_CREATED_DATE,
    MILESTONE_MILESTONES_COMPLETED_DATE,
    MILESTONE_MILESTONES_UPDATED_DATE)

  val UNIX_TIMESTAMP_CONVERSION_COLUMNS: Seq[String] = Seq(
    MILESTONE_MILESTONES_DUE_DATE,
    SET_DATE)

  val PRIMARY_KEY: Seq[String] = Seq(
    BLUEPRINT_ID,
    MILESTONE_NAME,
    HIVE_LOAD_DATE
  )

  val tempFndTablePath: String = "src/test/Data/data-processing-pipeline/rubix_blueprints_temp/"

  it should "generate a sql statement" in {
    val expectedOutput: String = "select blueprint_id,campaign_id,campaign_name,campaign_type,created_by,created_utc_ts," +
      "milestone_assigned_to_arr,milestone_completed_by,milestone_completed_utc_ts,milestone_completion_level,milestone_created_by," +
      "milestone_created_utc_ts,milestone_critical_f,milestone_days_out,milestone_disabled_f,milestone_due_date_utc_ts,milestone_id," +
      "milestone_name,milestone_risk_f,milestone_risk_days_out,milestone_role,milestone_status,milestone_status_description," +
      "milestone_updated_by,milestone_updated_utc_ts,blueprint_name,planned_budget,set_date_utc_ts,sign_volume_estimate," +
      "blueprint_status,tier_one_f,blueprint_type,updated_by,updated_utc_ts, date_add(report_d, 1) as report_d, updated_utc_ts " +
      "as hive_load_timestamp from stg_sgn_fnd.rubix_blueprints where report_d = \"2022-08-20\""

    val generatedOutput: String = BuildAllPartitionsFromEarliestPartitionRead.generateEarliestPartitionQuery(
      minFoundationDate = "2022-08-20",
      columnNames = FINAL_COLUMN_NAMES,
      partitionColumn = HIVE_LOAD_DATE,
      fndDb = "stg_sgn_fnd",
      fndTable = "rubix_blueprints",
      fndTimestamp = UPDATED_UTC_TS,
      kafkaTimestamp = HIVE_LOAD_TIMESTAMP
    )
    assertResult(expectedOutput)(generatedOutput)
  }

  it should "initialize the starting DataFrame" in {
    Try(FileWriterService.emptyDir("src/test/data/partition_build_test/")(spark))
    match {
      case Success(_) => log.info("Successfully deleted prior test data")
      case Failure(e) => log.error(e.getMessage)
    }
    val ephemeralPath = "src/test/data/partition_build_test/ephemeral/"
    val testPartFile: String = "src/test/resources/load_date=202208160704"
    val testPartFile2: String = "src/test/resources/load_date=202208140701"
    FileWriterService.makeDir(ephemeralPath)
    FileWriterService.copyFile(testPartFile, ephemeralPath)
    FileWriterService.copyFile(testPartFile2, ephemeralPath)

    val templateFile = "src/test/resources/rubix_blueprints_template.json"
    val templateLocation = "src/test/data/partition_build_test/template/"
    FileWriterService.makeDir(templateLocation)
    FileWriterService.copyFile(templateFile, templateLocation)

    val checkPointDir = "src/test/data/partition_build_test/dpp_checkpoint/"
    FileWriterService.makeDir(checkPointDir)

    val corruptDir = "src/test/data/partition_build_test/corrupt_data/"
    FileWriterService.makeDir(corruptDir)

    val sparkCheckpointDir = "src/test/data/partition_build_test/spark_checkpoint/"
    spark.sparkContext.setCheckpointDir(sparkCheckpointDir)

    val expectedDateRange: String = "2022-08-13,2022-08-14"

    val initialize = FileReader.readAndDedupeKafkaFiles(ReadKafkaFiles(
      inputEphemeralPath = ephemeralPath,
      templateLocation = templateLocation,
      checkpointPath = checkPointDir,
      corruptRecordLocation = corruptDir,
      inputEphemeralFileFormat = "json",
      numberOfPartitions = 3,
      env = "local",
      dropColumns = Seq.empty,
      ephemeralFilePathPrefix = "load_date=",
      templateFileFormat = "json",
      addKafkaTimestamp = true,
      kafkaDateFieldName = HIVE_LOAD_DATE,
      kafkaTimestampFieldName = HIVE_LOAD_TIMESTAMP,
      kafkaValueFieldRename = "value_json"
    ))
    val kafkaDF: DataFrame = initialize._1
    val ephemeralCount: Long = initialize._3

    log.info(s"count from initial kafka file read: $ephemeralCount")

    val subSelectKafkaDF: DataFrame = kafkaDF.select(INPUT_SELECT_COLUMNS.map(c => col(c)): _*)

    val flattenKafkaDF = FlattenFunctions.flattenDataframe(subSelectKafkaDF)
    val trimKafkaDF = TransFunctions.trimColumns(flattenKafkaDF)
    val convertTimestampKafkaDF = DateTimeUtils.convertToTimestamp(trimKafkaDF, TIMESTAMP_CONVERSION_COLUMNS)
    val convertUnixTimestampKafkaDF = DateTimeUtils.convertUnixTimestampToTimestamp(convertTimestampKafkaDF, UNIX_TIMESTAMP_CONVERSION_COLUMNS)
    val renameKafkaDF = TransFunctions.renameColumns(convertUnixTimestampKafkaDF.drop(MILESTONE_UPDATED_BY), COLUMNS_TO_RENAME)
    val windowDedupeKafkaDF = DataFrameUtils.getLatestByPartitionDateArray(renameKafkaDF, PRIMARY_KEY, HIVE_LOAD_TIMESTAMP, "row_number")

    windowDedupeKafkaDF.createOrReplaceTempView("kafkaDF")
    val reorderedKafkaDF = spark.sql(s"select ${FINAL_COLUMN_NAMES.mkString(",")}, $HIVE_LOAD_TIMESTAMP from kafkaDF")
    reorderedKafkaDF.persist()
    log.info("after reordered Kafka DF:")
    reorderedKafkaDF.createOrReplaceTempView("reorderedKafkaDF")
    spark.sql(s"select $HIVE_LOAD_DATE, count(*) from reorderedKafkaDF group by $HIVE_LOAD_DATE order by $HIVE_LOAD_DATE").show()
    reorderedKafkaDF.show(5)

    val datesAndEarliestDate = DateTimeUtils.getDatesFromDataFrame(reorderedKafkaDF, ephemeralCount, HIVE_LOAD_DATE)
    val dates = datesAndEarliestDate._1
    val earliestDate = datesAndEarliestDate._2

    val reader: BuildAllPartitionsFromEarliestPartitionRead = BuildAllPartitionsFromEarliestPartitionRead(
      columnNames = FINAL_COLUMN_NAMES,
      partitionColumn = HIVE_LOAD_DATE,
      fndDb = "stg_sgn_fnd",
      fndTable = "rubix_blueprints",
      fndTimestamp = UPDATED_UTC_TS,
      kafkaTimestamp = HIVE_LOAD_TIMESTAMP,
      env = "local",
      queueName = "SVSGNHDS_AUTO",
      fndSchema = FOUNDATION_SCHEMA,
      tempFndTablePath = tempFndTablePath,
      jdbcUrl = "",
      hiveEngine = "tez",
      retryCount = 3,
      testFoundationMockupFile = "src/test/resources/rubix_blueprints_20220813.orc")

    val foundationDF = BuildAllPartitionsFromEarliestPartitionRead.initializeStartingDataFrame(
      dates, reorderedKafkaDF, reader)
    foundationDF.createOrReplaceTempView("foundationDF")
    val reorderedFoundationDF = spark.sql(s"select ${FINAL_COLUMN_NAMES.mkString(",")}, $UPDATED_UTC_TS as $HIVE_LOAD_TIMESTAMP from foundationDF")

    reorderedFoundationDF.persist()
    log.info("after reordering foundation DF:")
    reorderedFoundationDF.createOrReplaceTempView("reorderedFoundationDF")
    spark.sql(s"select $HIVE_LOAD_DATE, count(*) from reorderedFoundationDF group by $HIVE_LOAD_DATE order by $HIVE_LOAD_DATE").show()
    reorderedFoundationDF.show(5)

    val blendedDF = DataFrameUtils.getLatestByPartitionDateArray(reorderedKafkaDF.union(reorderedFoundationDF),
      PRIMARY_KEY, HIVE_LOAD_TIMESTAMP, "row_number")

    blendedDF.persist()
    log.info("count after blending DFs:")
    blendedDF.createOrReplaceTempView("blendedDF")
    spark.sql(s"select $HIVE_LOAD_DATE, count(*) from blendedDF group by $HIVE_LOAD_DATE order by $HIVE_LOAD_DATE").show()
    blendedDF.show(5)

    val finalDF = BuildAllPartitionsFromEarliestPartitionRead.buildAllPartitionsFromEarliestPartition(dates, earliestDate, blendedDF,
      FINAL_COLUMN_NAMES, HIVE_LOAD_DATE, HIVE_LOAD_TIMESTAMP, "local",PRIMARY_KEY,"row_number")(spark)

    finalDF.printSchema()
    finalDF.cache()
    finalDF.createOrReplaceTempView("finalDF")
    log.info("final count:")
    val sqlStatement = s"select $HIVE_LOAD_DATE, to_date($HIVE_LOAD_TIMESTAMP), count(*) " +
      s"from finalDF group by $HIVE_LOAD_DATE, to_date($HIVE_LOAD_TIMESTAMP) " +
      s"order by $HIVE_LOAD_DATE, to_date($HIVE_LOAD_TIMESTAMP)"
    val aggregateDF: DataFrame = spark.sql(sqlStatement)
    val aggregateCount = aggregateDF.count().toInt
    aggregateDF.show(aggregateCount)

    val dateFormatString = "yyyy-MM-dd"
    val minMaxDates = finalDF.select(date_format(functions.min(col(HIVE_LOAD_DATE)), dateFormatString).as("min_load_date"),
      date_format(functions.max(col(HIVE_LOAD_DATE)), dateFormatString).as("max_load_date")
    ).collect().map(row => row.mkString(",")).toSeq.headOption.getOrElse(throw DateSequenceException("Unable to retrieve min and max dates from DataFrame"))

    assert(minMaxDates == expectedDateRange)
  }

}
